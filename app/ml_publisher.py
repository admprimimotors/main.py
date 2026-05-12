"""
app/ml_publisher.py
===================
Publicación de productos NUEVOS a Mercado Libre.

A diferencia de `catalogo.push_to_ml()` (que hace PUTs a publicaciones
existentes), este módulo hace POST /items para crear publicaciones desde cero
a partir de productos del catálogo local que todavía no están en ML.

Defaults de negocio (ajustables vía env vars):
  - listing_type_id = "gold_special" (Clásica, comisión más baja).
    Si querés "Premium" con cuotas sin interés, seteá ML_LISTING_TYPE=gold_pro.
  - condition = "new"
  - garantía: 30 días, "Garantía del fabricante"
  - envío: ME2 (Mercado Envíos), FLEX activo (logistic_type=self_service),
    free_shipping=true cuando precio_final >= ML_FREE_SHIPPING_MIN (default 55000).
  - status inicial = "paused" (borrador, no sale al aire hasta que el
    usuario la active).

Categoría: usa `ml_client.predict_category()` la primera vez y cachea la
respuesta en `categoria_ml_mapping` (nuestra_categoria → ml_category_id).
Productos sin `categoria` interna se publican uno a uno con preflight manual.

Flujo público:
  - validate_ready(producto)            → list de "qué falta" antes de publicar
  - get_or_predict_ml_category(...)     → resolver ML category_id
  - build_create_payload(...)           → arma el JSON de POST /items
  - create_publication(db, sku)         → orquestador, devuelve (ok, msg, ml_item_id)
  - bulk_create(db, skus, dry_run=False) → batch publish con rate-limit
"""

from __future__ import annotations

import os
import time
from decimal import Decimal
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from . import ml_client
from .models import CategoriaMLMapping, Producto


# =============================================================
# Defaults de negocio — overridables por env vars
# =============================================================

DEFAULT_LISTING_TYPE = os.environ.get("ML_LISTING_TYPE", "gold_special")
DEFAULT_CONDITION = "new"
DEFAULT_BUYING_MODE = "buy_it_now"
DEFAULT_CURRENCY = "ARS"
DEFAULT_INITIAL_STATUS = os.environ.get("ML_INITIAL_STATUS", "paused")

# "Garantía del fabricante" en ML es value_id 2230280, pero también podés
# mandarlo por value_name y ML lo resuelve. Usamos value_name por simplicidad.
DEFAULT_WARRANTY_TYPE = "Garantía del fabricante"
DEFAULT_WARRANTY_TIME_VALUE = 30
DEFAULT_WARRANTY_TIME_UNIT = "días"

# Precio mínimo para que el envío sea gratis (lo paga el vendedor)
FREE_SHIPPING_MIN = Decimal(os.environ.get("ML_FREE_SHIPPING_MIN", "55000"))

# FLEX (Mercado Envíos Flex — same-day delivery). El seller debe estar habilitado
# en ML; si no lo está, ML ignora el tag o devuelve error. Lo mandamos porque
# Primi Motors confirmó tener FLEX activo en su cuenta.
FLEX_ENABLED = os.environ.get("ML_FLEX_ENABLED", "true").lower() in ("1", "true", "yes")

# Catalog opt-out: para que las publicaciones tengan TÍTULO EDITABLE después de
# publicar, hay que decirle a ML "no me linkees al catálogo". ML por default
# linkea a categorías que tienen catálogo (ej Camisas de Motor) y bloquea
# la edición de título. Si seteamos catalog_listing=false, publicamos como
# ítem "del vendedor" — título editable, pero perdemos exposure de catálogo.
# Default: true (priorizamos edición de título sobre exposure de catálogo).
ML_CATALOG_OPTOUT = os.environ.get("ML_CATALOG_OPTOUT", "true").lower() in ("1", "true", "yes")

# Site ID (Argentina)
SITE_ID = os.environ.get("ML_SITE_ID", "MLA")


# =============================================================
# Resolución de categoría ML
# =============================================================

def get_or_predict_ml_category(
    db: Session,
    *,
    nuestra_categoria: Optional[str],
    titulo: str,
) -> tuple[Optional[str], Optional[str], list[dict]]:
    """
    Resuelve el ml_category_id a usar para un producto.

    Estrategia:
      1. Si tenemos `nuestra_categoria` y existe un row en `categoria_ml_mapping`
         para ese valor, usamos ese mapping (ya confirmado o auto-cacheado).
      2. Si no, llamamos al predictor de ML con el título y devolvemos el top
         candidato + alternativas para que el caller pueda mostrar opciones.

    Devuelve (ml_category_id, ml_category_name, candidatos).
    Si no hay mapping ni predicción posible, devuelve (None, None, []).

    NOTA: este método NO graba el mapeo automáticamente — eso lo hace
    `confirm_categoria_mapping()` cuando el usuario confirma desde la UI.
    """
    # 1) Mapping ya guardado
    if nuestra_categoria:
        mapping = db.execute(
            select(CategoriaMLMapping).where(
                CategoriaMLMapping.nuestra_categoria == nuestra_categoria
            )
        ).scalar_one_or_none()
        if mapping is not None:
            return mapping.ml_category_id, mapping.ml_category_name, []

    # 2) Predictor
    candidatos = ml_client.predict_category(db, titulo, site_id=SITE_ID, limit=5)
    if candidatos:
        top = candidatos[0]
        return top.get("category_id"), top.get("category_name"), candidatos
    return None, None, []


def confirm_categoria_mapping(
    db: Session,
    *,
    nuestra_categoria: str,
    ml_category_id: str,
    ml_category_name: Optional[str] = None,
    confirmado: bool = True,
) -> CategoriaMLMapping:
    """
    Inserta o actualiza el mapeo nuestra_categoria → ml_category_id.
    """
    existing = db.execute(
        select(CategoriaMLMapping).where(
            CategoriaMLMapping.nuestra_categoria == nuestra_categoria
        )
    ).scalar_one_or_none()

    if existing is None:
        existing = CategoriaMLMapping(
            nuestra_categoria=nuestra_categoria,
            ml_category_id=ml_category_id,
            ml_category_name=ml_category_name,
            confirmado=confirmado,
        )
        db.add(existing)
    else:
        existing.ml_category_id = ml_category_id
        if ml_category_name:
            existing.ml_category_name = ml_category_name
        existing.confirmado = confirmado

    db.commit()
    db.refresh(existing)
    return existing


# =============================================================
# Validación pre-publicación
# =============================================================

def validate_ready(
    producto: Producto,
    *,
    ml_category_id: Optional[str] = None,
    required_attrs: Optional[list[dict]] = None,
) -> list[str]:
    """
    Devuelve lista de problemas que impiden publicar este producto en ML.
    Lista vacía = está listo.

    Chequea:
      - título no vacío y <= 60 chars (límite ML)
      - precio_final > 0 y currency válida
      - stock_actual > 0 (ML puede aceptar 0 pero no tiene sentido publicar)
      - al menos 1 foto
      - categoría ML resuelta
      - todos los `required_attrs` (de la categoría ML) presentes en
        ficha_tecnica con valor no vacío
      - producto NO publicado ya (ml_item_id null)
    """
    problems: list[str] = []

    if producto.ml_item_id:
        problems.append(
            f"Ya está publicado en ML (item_id={producto.ml_item_id})."
        )

    titulo = (producto.titulo or "").strip()
    if not titulo:
        problems.append("Falta título.")
    elif len(titulo) > 60:
        problems.append(f"Título tiene {len(titulo)} chars, ML acepta máximo 60.")

    if producto.precio_final is None or producto.precio_final <= 0:
        problems.append("Falta precio final (debe ser > 0).")

    if (producto.stock_actual or 0) <= 0:
        problems.append("Stock en 0 — cargá stock antes de publicar.")

    if not producto.fotos:
        problems.append("No tiene fotos cargadas (ML requiere al menos 1).")

    if not ml_category_id:
        problems.append(
            "No se pudo resolver la categoría ML (ni mapeo guardado ni "
            "predicción del título)."
        )

    # Atributos obligatorios de la categoría
    if required_attrs:
        for attr in required_attrs:
            attr_id = attr.get("id") or ""
            attr_name = attr.get("name") or ""
            # Resolvemos vía helper que mira primero el campo dedicado del
            # producto (BRAND → producto.marca) y después la ficha_tecnica.
            value = get_producto_attr_value(producto, attr_id, attr_name)
            if not value:
                problems.append(
                    f"Falta atributo obligatorio '{attr_name or attr_id}' "
                    f"(ni en ficha técnica ni en campos del producto)."
                )

    return problems


def required_attributes(attrs: list[dict]) -> list[dict]:
    """
    Filtra de la respuesta de /categories/{id}/attributes los atributos que
    ML marca como obligatorios (`tags.required`).
    """
    out = []
    for a in attrs or []:
        tags = a.get("tags") or {}
        # ML usa varias formas: tags.required (bool) o un set string. Aceptamos
        # ambas para ser defensivos.
        is_required = (
            tags.get("required") is True
            or "required" in (tags if isinstance(tags, list) else [])
        )
        if is_required:
            out.append(a)
    return out


# =============================================================
# Construcción del payload
# =============================================================

def get_producto_attr_value(producto: Producto, attr_id: str, attr_name: str) -> Optional[str]:
    """
    Resuelve el valor de un atributo ML para un producto, buscando en:

      1. **Campo dedicado** del Producto (vía _ML_ATTR_TO_FIELD).
         Ejemplo: ML pide BRAND → leemos producto.marca (no ficha_tecnica.marca).
      2. **ficha_tecnica** matcheando por id_lowercase, id_normalizado o
         nombre_normalizado.

    Devuelve el valor como string trim, o None si no encontramos nada.
    """
    from .catalogo import _ML_ATTR_TO_FIELD, _norm_attr_key

    attr_id_up = (attr_id or "").upper()
    attr_id_lo = (attr_id or "").lower()

    # 1) Campo dedicado del producto
    field = _ML_ATTR_TO_FIELD.get(attr_id_up)
    if field:
        val = getattr(producto, field, None)
        if val is not None and str(val).strip():
            return str(val).strip()

    # 2) ficha_tecnica
    ficha = producto.ficha_tecnica or {}
    if not ficha:
        return None
    ficha_norm = {_norm_attr_key(k): v for k, v in ficha.items() if v is not None}
    for cand in (attr_id_lo, _norm_attr_key(attr_id), _norm_attr_key(attr_name)):
        if not cand:
            continue
        if cand in ficha_norm:
            v = str(ficha_norm[cand]).strip()
            if v:
                return v
    return None


def _format_attr_for_ml(attr_def: dict, raw_value: str) -> Optional[dict]:
    """
    Formatea el valor de un atributo según su value_type ML.

    ML acepta distintos shapes según el tipo:
      - number: value_name con solo dígitos/decimales
      - number_unit: value_struct {number, unit} (parseamos "1 u", "10cm", etc.)
      - list (closed list): value_id si matcheamos contra `values`, si no value_name
      - string / boolean / otros: value_name plano

    Si el valor no se puede parsear de manera válida para el tipo (ej:
    "Embalaje individual" en un atributo number), devuelve None — se skipea
    el atributo para no romper todo el POST.
    """
    import re
    val_str = str(raw_value or "").strip()
    if not val_str:
        return None
    attr_id = attr_def.get("id")
    if not attr_id:
        return None
    value_type = (attr_def.get("value_type") or "string").lower()

    if value_type == "number":
        m = re.search(r"-?\d+(?:[.,]\d+)?", val_str)
        if not m:
            return None
        return {"id": attr_id, "value_name": m.group(0).replace(",", ".")}

    if value_type == "number_unit":
        # Parsear "1", "1u", "1 u", "10cm", "1.5 kg", "75 mm", etc.
        m = re.match(r"^\s*(-?\d+(?:[.,]\d+)?)\s*([a-zA-Z]*)\s*$", val_str)
        if not m:
            # No parseamos como número — fallback: mandamos como value_name plano
            return {"id": attr_id, "value_name": val_str}
        num_str = m.group(1).replace(",", ".")
        try:
            num = float(num_str)
        except ValueError:
            return {"id": attr_id, "value_name": val_str}
        unit = (m.group(2) or "").strip()
        if not unit:
            # Si no especificó unidad en el valor, intentamos:
            #   1. Buscar la unidad en ficha (ej "unidad_de_largo" o "unidad")
            #   2. Usar la primera permitida por ML
            allowed = attr_def.get("allowed_units") or []
            if allowed:
                unit = allowed[0].get("id") or ""
        if not unit:
            # Sin unidad: fallback a value_name con solo el número (ML decide)
            n_show = int(num) if num.is_integer() else num
            return {"id": attr_id, "value_name": str(n_show)}
        # ML acepta números enteros como int o float; usamos int si es entero
        n_value = int(num) if num.is_integer() else num
        return {
            "id": attr_id,
            "value_struct": {"number": n_value, "unit": unit},
        }

    if value_type == "list":
        # Lista cerrada — matchear case-insensitive contra values permitidos
        allowed = attr_def.get("values") or []
        val_lower = val_str.lower()
        for v in allowed:
            name = (v.get("name") or "").strip()
            if name.lower() == val_lower:
                # Match exacto → mandamos value_id (más confiable)
                return {"id": attr_id, "value_id": v.get("id")}
        # Sin match: si la lista es estricta ML rechaza; mandamos value_name
        # como fallback y dejamos que ML responda
        return {"id": attr_id, "value_name": val_str}

    # string, boolean, etc.
    return {"id": attr_id, "value_name": val_str}


def _ficha_to_ml_attributes(
    producto: Producto,
    category_attrs: list[dict],
) -> list[dict]:
    """
    Convierte los atributos del producto (campos dedicados + ficha_tecnica)
    en el array `attributes` que espera POST /items.

    Para cada atributo definido por la categoría ML:
      1. Busca el valor con `get_producto_attr_value` (campos dedicados → ficha)
      2. Lo formatea según value_type vía `_format_attr_for_ml`
      3. Si formatear falla (ej texto en atributo numérico), se skipea

    Esto evita que un atributo opcional con valor mal formateado tire abajo
    todo el POST.
    """
    if not category_attrs:
        return []
    out: list[dict] = []
    for attr in category_attrs:
        attr_id = attr.get("id") or ""
        if not attr_id:
            continue
        value = get_producto_attr_value(producto, attr_id, attr.get("name") or "")
        if not value:
            continue
        formatted = _format_attr_for_ml(attr, value)
        if formatted:
            out.append(formatted)
    return out


def _derive_family_name(producto: Producto) -> str:
    """
    family_name: identificador de "familia de producto" en el catálogo de ML.

    Cuando publicás en una categoría que tiene catálogo (la mayoría de
    repuestos de auto, ej "Camisas de Motor"), ML pide este campo para
    agrupar publicaciones equivalentes de distintos vendedores.

    Estrategia:
      - Usamos el título como base (suele ser descriptivo del producto +
        compatibilidad vehicular, que es lo que define una familia).
      - Cap a 60 chars — ML rechaza family_names muy largos.
      - Sin caracteres raros que ML no acepta.
    """
    import re
    base = (producto.titulo or producto.sku or "").strip()
    # ML acepta letras, números, espacios, /, -, ., comas, paréntesis y &.
    # Removemos comillas y otros raros que a veces dan problema.
    cleaned = re.sub(r'["\'`*<>]', "", base)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned[:60]


def _normalize_picture_url(url: str) -> str:
    """
    Normaliza URLs comunes que ML no logra descargar directamente:
      - Google Drive viewer (`/file/d/ID/view`) → URL directa (`uc?export=view&id=ID`)
      - Google Drive open (`?id=ID`) → URL directa
      - Dropbox preview (`?dl=0`) → forzar descarga (`?dl=1`)

    Si no matchea ninguno de los patterns, devuelve la URL tal cual.
    """
    import re
    if not url:
        return url
    u = url.strip()

    # Google Drive — file/d/ID/view
    m = re.search(r"drive\.google\.com/file/d/([a-zA-Z0-9_-]+)", u)
    if m:
        return f"https://drive.google.com/uc?export=view&id={m.group(1)}"

    # Google Drive — open?id=ID
    m = re.search(r"drive\.google\.com/open\?id=([a-zA-Z0-9_-]+)", u)
    if m:
        return f"https://drive.google.com/uc?export=view&id={m.group(1)}"

    # Dropbox — forzar descarga
    if "dropbox.com" in u and "dl=0" in u:
        return u.replace("dl=0", "dl=1")

    return u


def _photo_urls(producto: Producto) -> list[dict]:
    """Lista de URLs de fotos en el formato que espera ML: [{"source": url}, ...].

    Normaliza URLs comunes problemáticas (Drive viewer → directo, etc.)
    para mejorar el match-rate de descarga por parte de ML.
    """
    return [
        {"source": _normalize_picture_url(f.url)}
        for f in (producto.fotos or [])
        if f.url
    ]


def _shipping_block(price: Decimal) -> dict:
    """
    Construye el bloque `shipping` del payload.
    - mode = me2 (Mercado Envíos)
    - free_shipping si precio >= FREE_SHIPPING_MIN
    - logistic_type = self_service si FLEX está activo
    """
    free = price is not None and Decimal(price) >= FREE_SHIPPING_MIN
    block: dict = {
        "mode": "me2",
        "free_shipping": free,
        "local_pick_up": False,
    }
    if FLEX_ENABLED:
        # ML acepta el tag o el logistic_type — ambos significan FLEX.
        block["tags"] = ["self_service_in"]
    return block


def _sale_terms_block() -> list[dict]:
    """
    Garantía default según política de Primi: 30 días de fábrica.

    WARRANTY_TYPE es de lista cerrada — ML pide value_id, no value_name.
    Los IDs son estables entre categorías:
      - 2230279 = Garantía del fabricante
      - 2230280 = Garantía del vendedor
      - 2230281 = Sin garantía
    WARRANTY_TIME es free-text con value_struct {number, unit}.
    """
    return [
        {
            "id": "WARRANTY_TYPE",
            "value_id": "2230279",
            "value_name": DEFAULT_WARRANTY_TYPE,
        },
        {
            "id": "WARRANTY_TIME",
            "value_name": f"{DEFAULT_WARRANTY_TIME_VALUE} {DEFAULT_WARRANTY_TIME_UNIT}",
            "value_struct": {
                "number": DEFAULT_WARRANTY_TIME_VALUE,
                "unit": DEFAULT_WARRANTY_TIME_UNIT,
            },
        },
    ]


def is_catalog_category(db: Session, category_id: str) -> bool:
    """
    Determina si una categoría ML usa el sistema de catálogo de productos.
    En categorías-catálogo, ML genera el título automáticamente desde la
    familia + atributos, y rechaza si vos mandás el campo `title`.

    Detección: la categoría tiene `settings.catalog_domain` no-vacío.
    """
    cat = ml_client.get_category(db, category_id) or {}
    settings = cat.get("settings") or {}
    return bool(settings.get("catalog_domain"))


def build_create_payload(
    producto: Producto,
    *,
    ml_category_id: str,
    category_attrs: list[dict],
    listing_type_id: Optional[str] = None,
    initial_status: Optional[str] = None,
    is_catalog: bool = False,
) -> dict:
    """
    Arma el dict de payload para POST /items.

    Si `is_catalog=True`, omitimos `title` (ML lo deriva del catálogo) y
    obligatoriamente mandamos `family_name`. Si False, mandamos `title` y
    `family_name` queda como hint (ML lo ignora si no aplica).
    """
    listing_type_id = listing_type_id or DEFAULT_LISTING_TYPE
    initial_status = initial_status or DEFAULT_INITIAL_STATUS

    payload: dict = {
        "category_id": ml_category_id,
        "price": float(producto.precio_final or 0),
        "currency_id": DEFAULT_CURRENCY,
        "available_quantity": int(producto.stock_actual or 0),
        "buying_mode": DEFAULT_BUYING_MODE,
        "listing_type_id": listing_type_id,
        "condition": DEFAULT_CONDITION,
        "pictures": _photo_urls(producto),
        "shipping": _shipping_block(producto.precio_final or Decimal("0")),
        "sale_terms": _sale_terms_block(),
        "attributes": _ficha_to_ml_attributes(producto, category_attrs),
        "status": initial_status,
        # family_name: requerido por ML para categorías con catálogo. Si la
        # categoría no es de catálogo, ML lo ignora silenciosamente.
        "family_name": _derive_family_name(producto),
    }

    # Title vs catalog_listing:
    #   - Si ML_CATALOG_OPTOUT está activo, publicamos opt-out con catalog_listing=false
    #     → título editable. Mandamos title siempre.
    #   - Si está desactivado (default catalog), mandamos title solo cuando la
    #     categoría no es catálogo (en catálogo, ML genera el título).
    if ML_CATALOG_OPTOUT:
        payload["catalog_listing"] = False
        payload["title"] = (producto.titulo or "").strip()[:60]
    elif not is_catalog:
        payload["title"] = (producto.titulo or "").strip()[:60]

    # NOTA: la descripción ya no se manda en el POST. ML la maneja como un
    # endpoint separado (PUT /items/{id}/description). Lo hacemos en
    # `create_publication` después del POST. Si la mandás inline, ML a veces
    # la ignora silenciosamente para categorías con catálogo.

    return payload


def build_description_text(producto: Producto) -> str:
    """
    Arma una descripción plain-text para mandar a ML cuando hace falta:
      - Si el producto tiene `descripcion` cargada → la usamos tal cual.
      - Si no → autogeneramos a partir del título + ficha técnica +
        compatibilidades vehiculares + footer estándar.

    El texto resultante es plain-text con saltos de línea (ML acepta
    plain_text vía PUT /items/{id}/description).
    """
    # Caso simple: descripción cargada manualmente
    custom = (producto.descripcion or "").strip()
    if custom:
        return custom

    # Fallback: armar desde ficha técnica + compats
    lines: list[str] = []

    if producto.titulo:
        lines.append(producto.titulo.strip())
        lines.append("")

    ficha = producto.ficha_tecnica or {}
    if ficha:
        lines.append("FICHA TÉCNICA")
        lines.append("─" * 30)
        for k, v in ficha.items():
            if v is None or str(v).strip() == "":
                continue
            label = str(k).replace("_", " ").upper()
            lines.append(f"• {label}: {v}")
        lines.append("")

    # Compatibilidades vehiculares (si existen)
    try:
        compats = list(producto.compatibilidades or [])
    except Exception:
        compats = []
    if compats:
        lines.append("COMPATIBILIDADES VEHICULARES")
        lines.append("─" * 30)
        for c in compats:
            try:
                v = c.vehiculo
            except Exception:
                continue
            partes = [v.marca, v.modelo]
            anio_range = ""
            if v.anio_desde and v.anio_hasta and v.anio_desde != v.anio_hasta:
                anio_range = f" ({v.anio_desde}-{v.anio_hasta})"
            elif v.anio_desde:
                anio_range = f" ({v.anio_desde}-)"
            elif v.anio_hasta:
                anio_range = f" (-{v.anio_hasta})"
            motor = ""
            if v.cilindrada_cc:
                motor = f" {v.cilindrada_cc}cc"
            if v.combustible:
                motor += f" {v.combustible}"
            line = "• " + " ".join(p for p in partes if p) + anio_range + motor
            if c.notas:
                line += f" — {c.notas}"
            lines.append(line)
        lines.append("")

    # Footer estándar
    lines.append("─" * 30)
    lines.append("PRIMI MOTORS")
    lines.append("Garantía 30 días de fábrica · Envío gratis con FLEX (CABA y GBA)")

    return "\n".join(lines).strip()


# =============================================================
# Orquestador: crear publicación individual
# =============================================================

def create_publication(
    db: Session,
    sku: str,
    *,
    ml_category_id_override: Optional[str] = None,
    listing_type_id: Optional[str] = None,
    initial_status: Optional[str] = None,
) -> tuple[bool, str, Optional[str]]:
    """
    Crea una publicación nueva en ML para el producto identificado por `sku`.

    Pasos:
      1. Carga el producto y resuelve categoría ML (mapping → predictor → override).
      2. Trae los atributos de la categoría desde ML.
      3. Valida readiness (campos básicos + atributos requeridos).
      4. Construye payload y POSTea a /items.
      5. Guarda ml_item_id, ml_permalink, ml_status en el Producto local.

    Devuelve (ok, mensaje, ml_item_id_o_None).
    """
    if not ml_client.is_write_enabled():
        return False, (
            "Write sync ML deshabilitado. "
            "Para activar, seteá ML_SYNC_WRITE_ENABLED=true en Render."
        ), None

    prod = db.execute(
        select(Producto).where(Producto.sku == sku)
    ).scalar_one_or_none()
    if prod is None:
        return False, f"SKU '{sku}' no existe", None
    if prod.ml_item_id:
        return False, (
            f"El producto ya está publicado (item_id={prod.ml_item_id}). "
            "Si querés actualizarlo usá Push a ML."
        ), None

    # Resolver categoría:
    #   1. Override explícito del caller (ej UI confirm)
    #   2. ml_category_id seteado en el producto (desde el Excel)
    #   3. Mapping cacheado por nuestra_categoria
    #   4. Predictor ML
    if ml_category_id_override:
        ml_cat_id = ml_category_id_override
        ml_cat_name = None
    elif (prod.ml_category_id or "").strip():
        ml_cat_id = prod.ml_category_id.strip()
        ml_cat_name = None
    else:
        ml_cat_id, ml_cat_name, _candidatos = get_or_predict_ml_category(
            db,
            nuestra_categoria=prod.categoria,
            titulo=prod.titulo or "",
        )

    if not ml_cat_id:
        return False, (
            "No pude resolver una categoría ML para este producto. "
            "Andá a la página de publicación y elegí una manualmente."
        ), None

    # Atributos de la categoría
    category_attrs = ml_client.get_category_attributes(db, ml_cat_id)
    req_attrs = required_attributes(category_attrs)

    # ¿Es categoría de catálogo? Determina si mandamos title o no.
    is_cat = is_catalog_category(db, ml_cat_id)

    # Pre-flight
    problems = validate_ready(
        prod, ml_category_id=ml_cat_id, required_attrs=req_attrs
    )
    if problems:
        return False, "No se puede publicar: " + " · ".join(problems), None

    # POST /items
    payload = build_create_payload(
        prod,
        ml_category_id=ml_cat_id,
        category_attrs=category_attrs,
        listing_type_id=listing_type_id,
        initial_status=initial_status,
        is_catalog=is_cat,
    )

    try:
        resp = ml_client.create_item(db, payload)
    except ml_client.MLClientError as e:
        err_str = str(e).lower()
        # Si fallamos por catalog_listing=false en una categoría catálogo-mandatory,
        # reintentamos sin opt-out (la publicación pierde título editable pero
        # se publica). ML a veces dice "catalog_listing" explícito, otras dice
        # "fields [title] are invalid" — ambas significan lo mismo: hay que
        # sacar title y catalog_listing del payload.
        catalog_fail_signals = (
            "catalog_listing", "catalog listing", "mandatory catalog",
            "must be catalog", "catalog_product",
            # ML reporta así cuando la categoría obliga catálogo y le mandaste title:
            "[title] are invalid",
            "title] are invalid",
            "fields [title]",
            "body.invalid_fields",
        )
        if (
            payload.get("catalog_listing") is False
            and any(sig in err_str for sig in catalog_fail_signals)
        ):
            retry_payload = dict(payload)
            retry_payload.pop("catalog_listing", None)
            # En catálogo, ML rechaza title — sacarlo también
            retry_payload.pop("title", None)
            try:
                resp = ml_client.create_item(db, retry_payload)
            except ml_client.MLClientError as e2:
                return False, (
                    f"ML rechazó la publicación con catalog_listing=false ({e}), "
                    f"reintenté sin él y también falló: {e2}"
                ), None
        else:
            return False, f"ML rechazó la publicación: {e}", None
    except Exception as e:
        return False, f"Error inesperado creando publicación: {type(e).__name__}: {e}", None

    new_id = resp.get("id")
    if not new_id:
        return False, f"ML respondió sin id de item: {resp}", None

    # ---- DIAGNÓSTICO: comparar lo que mandamos vs lo que ML aceptó ----
    sent_attr_ids = {a.get("id") for a in (payload.get("attributes") or []) if a.get("id")}
    received_attr_ids = {
        a.get("id") for a in (resp.get("attributes") or [])
        if a.get("id") and (a.get("value_name") or a.get("value_id") or a.get("value_struct"))
    }
    attrs_droppeados = sent_attr_ids - received_attr_ids

    sent_pics = len(payload.get("pictures") or [])
    received_pics = len(resp.get("pictures") or [])
    pics_droppeadas = sent_pics - received_pics

    diag_parts = []
    if attrs_droppeados:
        diag_parts.append(
            f"⚠ Atributos descartados por ML: {', '.join(sorted(attrs_droppeados))}"
        )
    if pics_droppeadas > 0:
        diag_parts.append(
            f"⚠ ML descartó {pics_droppeadas} de {sent_pics} foto(s) (URL no descargable)"
        )

    # Persistimos los identificadores en el producto local
    prod.ml_item_id = new_id
    prod.ml_permalink = resp.get("permalink")
    prod.ml_status = resp.get("status")
    prod.ml_stock = resp.get("available_quantity")
    if resp.get("price") is not None:
        try:
            prod.ml_precio = Decimal(str(resp.get("price")))
        except Exception:
            pass
    # Snapshot de los atributos como ML los devolvió (ya con value_id resueltos)
    prod.ml_raw_attributes = resp.get("attributes") or []

    # Descripción: PUT separado a /items/{id}/description.
    # Si falla no es fatal — la publicación ya existe, podemos reintentar después.
    desc_warning = ""
    descripcion_text = build_description_text(prod)
    if descripcion_text:
        try:
            ml_client.update_item_description(db, new_id, descripcion_text)
        except ml_client.MLClientError as e:
            desc_warning = f" (descripción no se pudo setear: {e})"
        except Exception as e:
            desc_warning = f" (descripción falló: {type(e).__name__})"

    # Si recién mapeamos esta categoría por predictor (sin row en mapping),
    # la guardamos como cache no-confirmada para futuros productos de la misma
    # categoría interna.
    if prod.categoria and not ml_category_id_override:
        existing = db.execute(
            select(CategoriaMLMapping).where(
                CategoriaMLMapping.nuestra_categoria == prod.categoria
            )
        ).scalar_one_or_none()
        if existing is None:
            db.add(CategoriaMLMapping(
                nuestra_categoria=prod.categoria,
                ml_category_id=ml_cat_id,
                ml_category_name=ml_cat_name,
                confirmado=False,
            ))

    db.commit()

    msg = f"✓ Publicado en ML como {new_id} (status={resp.get('status')})"
    if resp.get("permalink"):
        msg += f" · {resp['permalink']}"
    if diag_parts:
        msg += " · " + " · ".join(diag_parts)
    if desc_warning:
        msg += desc_warning
    return True, msg, new_id


# =============================================================
# Publicación matriz (con variantes)
# =============================================================

def build_matrix_payload(
    variants: list[Producto],
    *,
    ml_category_id: str,
    category_attrs: list[dict],
    variation_attr: dict,
    listing_type_id: Optional[str] = None,
    initial_status: Optional[str] = None,
    is_catalog: bool = False,
) -> dict:
    """
    Arma el payload para POST /items con `variations[]`.

    Cada variante:
      - attribute_combinations: [{id: <variation_attr_id>, value_name: ...}]
      - price, available_quantity propios
      - seller_custom_field: el SKU local
      - picture_ids: las URLs de las fotos del producto-variante (en POST
        van como picture_ids, pero ML acepta `pictures: [{source: url}]`
        para variations también — usamos ese formato)
    """
    listing_type_id = listing_type_id or DEFAULT_LISTING_TYPE
    initial_status = initial_status or DEFAULT_INITIAL_STATUS

    # Tomamos al primero como "matriz" (su info se hereda — título, fotos
    # nivel item si las usamos, etc.).
    master = variants[0]

    var_attr_id = variation_attr.get("id")
    allowed_values = variation_attr.get("values") or []

    # Atributos compartidos (a nivel item) — sacamos del primero. No mandamos
    # acá el atributo que varía (ese va a attribute_combinations).
    item_attrs = [
        a for a in _ficha_to_ml_attributes(master, category_attrs)
        if a.get("id") != var_attr_id
    ]

    # Acumulamos todas las fotos de TODAS las variantes a nivel item, para
    # que ML las tenga disponibles para asociar a cada variation.
    seen_urls = set()
    item_pictures: list[dict] = []
    for v in variants:
        for f in (v.fotos or []):
            if f.url and f.url not in seen_urls:
                seen_urls.add(f.url)
                item_pictures.append({"source": _normalize_picture_url(f.url)})

    variations_block: list[dict] = []
    for v in variants:
        medida_raw = _get_medida_from_ficha(v)
        value_id, value_name = match_variation_value(medida_raw, allowed_values)

        combo: dict = {"id": var_attr_id}
        if value_id:
            combo["value_id"] = value_id
        if value_name:
            combo["value_name"] = value_name

        var_pictures = [{"source": _normalize_picture_url(f.url)} for f in (v.fotos or []) if f.url]

        variations_block.append({
            "attribute_combinations": [combo],
            "price": float(v.precio_final or 0),
            "available_quantity": int(v.stock_actual or 0),
            "seller_custom_field": v.sku,
            # ML acepta `pictures` o `picture_ids`. Con `pictures` nos podemos
            # ahorrar un upload previo (ML toma las URLs y las descarga).
            "pictures": var_pictures,
        })

    payload: dict = {
        "category_id": ml_category_id,
        "currency_id": DEFAULT_CURRENCY,
        "buying_mode": DEFAULT_BUYING_MODE,
        "listing_type_id": listing_type_id,
        "condition": DEFAULT_CONDITION,
        # Las fotos a nivel item son la unión de todas las variantes
        "pictures": item_pictures,
        "shipping": _shipping_block(master.precio_final or Decimal("0")),
        "sale_terms": _sale_terms_block(),
        "attributes": item_attrs,
        "status": initial_status,
        "family_name": _derive_family_name(master),
        "variations": variations_block,
    }

    if ML_CATALOG_OPTOUT:
        payload["catalog_listing"] = False
        payload["title"] = (master.titulo or "").strip()[:60]
    elif not is_catalog:
        payload["title"] = (master.titulo or "").strip()[:60]

    return payload


def create_matrix_publication(
    db: Session,
    sku: str,
    *,
    ml_category_id_override: Optional[str] = None,
    listing_type_id: Optional[str] = None,
    initial_status: Optional[str] = None,
) -> tuple[bool, str, Optional[str], int]:
    """
    Publica un grupo de productos con mismo título como UNA publicación
    matriz con variations[]. Devuelve (ok, msg, ml_item_id, n_variantes).

    Detecta variantes por título idéntico al producto identificado por `sku`.
    Si solo hay 1 producto con ese título, error — el caller debería usar
    create_publication() simple.
    """
    if not ml_client.is_write_enabled():
        return False, (
            "Write sync ML deshabilitado. "
            "Para activar, seteá ML_SYNC_WRITE_ENABLED=true en Render."
        ), None, 0

    master = db.execute(
        select(Producto).where(Producto.sku == sku)
    ).scalar_one_or_none()
    if master is None:
        return False, f"SKU '{sku}' no existe", None, 0

    variants = find_variants(db, master.titulo or "", exclude_published=False)
    if not variants:
        return False, "No se encontraron variantes (raro — al menos el master debería matchear).", None, 0
    if len(variants) < 2:
        return False, (
            f"Solo se encontró 1 producto con título '{master.titulo}'. "
            "Para publicar como matriz necesito al menos 2 variantes con mismo título."
        ), None, 0

    # Validar que ninguna esté ya publicada (no podemos crear matriz si una
    # variante ya tiene su propio ml_item_id).
    ya_publicadas = [v for v in variants if v.ml_item_id]
    if ya_publicadas:
        skus_pub = ", ".join(v.sku for v in ya_publicadas)
        return False, (
            f"Estas variantes ya están publicadas individualmente y deben "
            f"despublicarse antes: {skus_pub}"
        ), None, 0

    # Resolver categoría desde el master (con el mismo orden que create_publication)
    if ml_category_id_override:
        ml_cat_id = ml_category_id_override
    elif (master.ml_category_id or "").strip():
        ml_cat_id = master.ml_category_id.strip()
    else:
        ml_cat_id, _name, _ = get_or_predict_ml_category(
            db, nuestra_categoria=master.categoria, titulo=master.titulo or "",
        )
    if not ml_cat_id:
        return False, "No se pudo resolver la categoría ML.", None, 0

    # Atributos de la categoría
    category_attrs = ml_client.get_category_attributes(db, ml_cat_id)
    req_attrs = required_attributes(category_attrs)
    is_cat = is_catalog_category(db, ml_cat_id)

    # Encontrar el atributo ML que admite variations
    var_attr = find_variation_attribute(category_attrs)
    if not var_attr:
        return False, (
            "Esta categoría ML no admite variantes según su definición. "
            "Hay que publicar cada producto por separado."
        ), None, 0

    # Pre-flight para cada variante: chequeos básicos + atributos requeridos
    # (ojo: el atributo de variación NO tiene que estar en cada ficha entera,
    # solo el VALOR de medida)
    for v in variants:
        # Chequeos básicos sin categoría
        problems_basic = validate_ready(v, ml_category_id="__DUMMY__", required_attrs=[])
        if problems_basic:
            return False, (
                f"Variante {v.sku}: " + " · ".join(problems_basic)
            ), None, 0
        if not _get_medida_from_ficha(v):
            return False, (
                f"Variante {v.sku}: no tiene 'medida' cargada en ficha técnica "
                "(es lo que diferencia las variantes)."
            ), None, 0

    # Atributos shared a nivel item: chequeamos contra el master
    if req_attrs:
        # El atributo de variación no es obligatorio a nivel item (va en cada variation)
        req_item_attrs = [a for a in req_attrs if a.get("id") != var_attr.get("id")]
        problems_attrs = validate_ready(
            master, ml_category_id=ml_cat_id, required_attrs=req_item_attrs
        )
        if problems_attrs:
            return False, (
                "Faltan atributos obligatorios a nivel matriz: "
                + " · ".join(problems_attrs)
            ), None, 0

    # Armar payload y publicar
    payload = build_matrix_payload(
        variants,
        ml_category_id=ml_cat_id,
        category_attrs=category_attrs,
        variation_attr=var_attr,
        listing_type_id=listing_type_id,
        initial_status=initial_status,
        is_catalog=is_cat,
    )

    try:
        resp = ml_client.create_item(db, payload)
    except ml_client.MLClientError as e:
        err_str = str(e).lower()
        catalog_fail_signals = (
            "catalog_listing", "catalog listing", "mandatory catalog",
            "must be catalog", "catalog_product",
            "[title] are invalid",
            "title] are invalid",
            "fields [title]",
            "body.invalid_fields",
        )
        if (
            payload.get("catalog_listing") is False
            and any(sig in err_str for sig in catalog_fail_signals)
        ):
            retry_payload = dict(payload)
            retry_payload.pop("catalog_listing", None)
            retry_payload.pop("title", None)
            try:
                resp = ml_client.create_item(db, retry_payload)
            except ml_client.MLClientError as e2:
                return False, (
                    f"ML rechazó la matriz con catalog_listing=false ({e}), "
                    f"reintenté sin él y también falló: {e2}"
                ), None, 0
        else:
            return False, f"ML rechazó la publicación matriz: {e}", None, 0
    except Exception as e:
        return False, f"Error inesperado: {type(e).__name__}: {e}", None, 0

    new_item_id = resp.get("id")
    if not new_item_id:
        return False, f"ML respondió sin id de item: {resp}", None, 0

    # Mapear variation_id ML → SKU local. ML devuelve `variations[]` con
    # cada variation_id y sus attribute_combinations — matcheamos por el
    # value de medida.
    ml_variations = resp.get("variations") or []
    medida_to_var_id: dict[str, str] = {}
    for mv in ml_variations:
        for combo in mv.get("attribute_combinations") or []:
            val_name = (combo.get("value_name") or "").strip().lower()
            if val_name and combo.get("id") == var_attr.get("id"):
                medida_to_var_id[val_name] = mv.get("id")
                break

    # Persistir en cada variante
    for v in variants:
        v.ml_item_id = new_item_id
        v.ml_permalink = resp.get("permalink")
        v.ml_status = resp.get("status")
        medida = _get_medida_from_ficha(v).strip().lower()
        v.ml_variation_id = medida_to_var_id.get(medida)
        v.ml_stock = v.stock_actual
        if v.precio_final is not None:
            v.ml_precio = v.precio_final

    # Cache de mapping de categoría si hay nuestra_categoria + venía de predict
    if master.categoria and not ml_category_id_override:
        existing = db.execute(
            select(CategoriaMLMapping).where(
                CategoriaMLMapping.nuestra_categoria == master.categoria
            )
        ).scalar_one_or_none()
        if existing is None:
            db.add(CategoriaMLMapping(
                nuestra_categoria=master.categoria,
                ml_category_id=ml_cat_id,
                confirmado=False,
            ))

    db.commit()

    # Descripción matriz: usamos la del master (igual para todas las variantes)
    desc_warning = ""
    descripcion_text = build_description_text(master)
    if descripcion_text:
        try:
            ml_client.update_item_description(db, new_item_id, descripcion_text)
        except Exception as e:
            desc_warning = f" (descripción no se pudo setear: {e})"

    msg = (
        f"✓ Publicación matriz creada en ML como {new_item_id} "
        f"con {len(variants)} variantes (status={resp.get('status')})"
    )
    if resp.get("permalink"):
        msg += f" · {resp['permalink']}"
    if desc_warning:
        msg += desc_warning
    return True, msg, new_item_id, len(variants)


# =============================================================
# Bulk
# =============================================================

# ML rate-limit: ~5-10 req/seg sostenido. Siendo conservadores: 0.4s entre
# llamadas (≈ 2.5 req/seg) para tener margen sobre el GET de attrs + POST item.
_BULK_DELAY_SECONDS = float(os.environ.get("ML_BULK_DELAY_SECONDS", "0.4"))


def bulk_create(
    db: Session,
    skus: list[str],
    *,
    dry_run: bool = False,
    on_progress=None,
) -> dict:
    """
    Publica masivamente. Para cada SKU:
      - Si dry_run=True, ejecuta solo validate_ready y reporta el estado.
      - Si dry_run=False, llama a create_publication.

    Devuelve un dict con summary:
      {
        "total": N,
        "ok": [{"sku": ..., "ml_item_id": ..., "msg": ...}, ...],
        "fail": [{"sku": ..., "msg": ...}, ...],
        "skipped": [{"sku": ..., "msg": ...}, ...],
      }
    """
    summary = {"total": len(skus), "ok": [], "fail": [], "skipped": []}

    for i, sku in enumerate(skus, start=1):
        if on_progress:
            try:
                on_progress(i, len(skus), sku)
            except Exception:
                pass

        prod = db.execute(
            select(Producto).where(Producto.sku == sku)
        ).scalar_one_or_none()
        if prod is None:
            summary["fail"].append({"sku": sku, "msg": "SKU no existe"})
            continue
        if prod.ml_item_id:
            summary["skipped"].append({
                "sku": sku,
                "msg": f"Ya publicado ({prod.ml_item_id})",
            })
            continue

        if dry_run:
            ml_cat_id, _, _ = get_or_predict_ml_category(
                db, nuestra_categoria=prod.categoria, titulo=prod.titulo or "",
            )
            cat_attrs = ml_client.get_category_attributes(db, ml_cat_id) if ml_cat_id else []
            req = required_attributes(cat_attrs)
            problems = validate_ready(prod, ml_category_id=ml_cat_id, required_attrs=req)
            if problems:
                summary["fail"].append({"sku": sku, "msg": " · ".join(problems)})
            else:
                summary["ok"].append({"sku": sku, "ml_item_id": None, "msg": "Listo para publicar"})
            time.sleep(_BULK_DELAY_SECONDS)
            continue

        ok, msg, item_id = create_publication(db, sku)
        if ok:
            summary["ok"].append({"sku": sku, "ml_item_id": item_id, "msg": msg})
        else:
            summary["fail"].append({"sku": sku, "msg": msg})
        time.sleep(_BULK_DELAY_SECONDS)

    return summary


# =============================================================
# Helpers para la UI
# =============================================================

# =============================================================
# Variantes (publicaciones matriz)
# =============================================================

def _norm_titulo(s: str) -> str:
    """Normaliza un título para matchear variantes: trim + colapso de espacios."""
    if not s:
        return ""
    import re
    return re.sub(r"\s+", " ", str(s).strip())


def find_variants(db: Session, titulo: str, *, exclude_published: bool = False) -> list[Producto]:
    """
    Busca todos los productos activos cuyo título coincide con `titulo`
    (después de normalización: trim + colapso de espacios).

    Si N >= 2: se considera un grupo de variantes y se publican como matriz ML.
    Si N == 1: producto único, publicación simple.

    Si exclude_published=True, descarta los que ya tienen ml_item_id (útil
    para el preflight masivo).
    """
    norm = _norm_titulo(titulo)
    if not norm:
        return []
    q = (
        select(Producto)
        .options(selectinload(Producto.fotos))
        .where(Producto.activo.is_(True))
    )
    if exclude_published:
        q = q.where(Producto.ml_item_id.is_(None))
    rows = list(db.execute(q).scalars().all())
    # Filtramos en memoria por el título normalizado (más confiable que SQL
    # case-insensitive con espacios variables).
    return [p for p in rows if _norm_titulo(p.titulo or "") == norm]


def find_variation_attribute(category_attrs: list[dict]) -> Optional[dict]:
    """
    Encuentra el atributo de la categoría ML que se usa para diferenciar
    variantes (ej MEASUREMENT, SIZE_GROUP).

    Estrategia:
      1. Filtrar atributos con tags.allow_variations=True
      2. Preferir uno cuyo nombre matchee "medida"/"size"/"medidas"/"diametro"
      3. Fallback: el primero que admita variations
    """
    candidates = []
    for a in category_attrs or []:
        tags = a.get("tags") or {}
        if tags.get("allow_variations") is True:
            candidates.append(a)
    if not candidates:
        return None

    from .catalogo import _norm_attr_key
    preferidos = {"medida", "medidas", "size", "tamano", "talle", "diametro", "measurement"}
    for c in candidates:
        name_norm = _norm_attr_key(c.get("name") or "")
        id_norm = (c.get("id") or "").lower()
        if name_norm in preferidos or id_norm in preferidos:
            return c
    return candidates[0]


def match_variation_value(
    raw_value: str,
    allowed_values: list[dict],
) -> tuple[Optional[str], str]:
    """
    Matchea un valor de variante (ej "STD", "+0.30", "0.60") contra la lista
    de valores aceptados de un atributo ML closed-list.

    Devuelve (value_id_o_None, value_name_a_enviar).
    Si no encuentra match en la lista cerrada, devuelve (None, raw_value)
    — ML decide si lo acepta como free-text o lo rechaza.
    """
    raw = (raw_value or "").strip()
    if not raw:
        return None, ""

    if not allowed_values:
        return None, raw  # Atributo es free-text

    raw_norm = raw.lower().replace(" ", "").replace("mm", "").replace(",", ".")

    # 1) Match exacto por nombre (case-insensitive)
    for v in allowed_values:
        if (v.get("name") or "").strip().lower() == raw.lower():
            return v.get("id"), v.get("name", raw)

    # 2) Match normalizado: "+0.30 mm" == "+0.30"
    for v in allowed_values:
        v_norm = (v.get("name") or "").lower().replace(" ", "").replace("mm", "").replace(",", ".")
        if v_norm == raw_norm:
            return v.get("id"), v.get("name", raw)

    # 3) Sin match → mandar como free-text (ML rechaza si la lista es cerrada
    # estricta, pero la mayoría de los atributos numéricos aceptan)
    return None, raw


def _get_medida_from_ficha(producto: Producto) -> str:
    """
    Extrae el valor de 'medida' (o keys equivalentes) de la ficha técnica de
    un producto. Útil para identificar qué variante es cada uno dentro del
    grupo matriz.
    """
    ficha = producto.ficha_tecnica or {}
    if not ficha:
        return ""
    candidatas = ("medida", "medidas", "size", "tamano", "talle", "diametro", "diametro_mm")
    for k in candidatas:
        if k in ficha and str(ficha[k] or "").strip():
            return str(ficha[k]).strip()
    return ""


def get_publishable_products(db: Session, limit: int = 500) -> list[Producto]:
    """
    Productos activos que NO están publicados en ML (sin ml_item_id).
    Eager-loadea fotos para evitar N+1 al renderizar la tabla masiva.
    """
    q = (
        select(Producto)
        .options(selectinload(Producto.fotos))
        .where(Producto.activo.is_(True))
        .where(Producto.ml_item_id.is_(None))
        .order_by(Producto.created_at.desc())
        .limit(limit)
    )
    return list(db.execute(q).scalars().all())
