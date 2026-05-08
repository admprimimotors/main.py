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
        ficha = producto.ficha_tecnica or {}
        # Las keys en ficha están normalizadas (snake_case ASCII). Para
        # matchear con los IDs ML (típicamente UPPER_SNAKE como BRAND, MODEL),
        # comparamos por: id ML directo, id ML lowercase, name normalizado.
        ficha_keys_ci = {k.lower(): k for k in ficha.keys()}
        for attr in required_attrs:
            attr_id = attr.get("id") or ""
            attr_name = attr.get("name") or ""
            # Buscamos el valor en la ficha por: id_lowercase, o nombre_normalizado
            from .catalogo import _norm_attr_key
            candidates = [
                attr_id.lower(),
                _norm_attr_key(attr_name),
                _norm_attr_key(attr_id),
            ]
            found = False
            for cand in candidates:
                if not cand:
                    continue
                if cand in ficha_keys_ci and (ficha[ficha_keys_ci[cand]] or "").strip():
                    found = True
                    break
            if not found:
                problems.append(
                    f"Falta atributo obligatorio '{attr_name or attr_id}' "
                    f"en ficha técnica."
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

def _ficha_to_ml_attributes(
    ficha: dict,
    category_attrs: list[dict],
) -> list[dict]:
    """
    Convierte ficha_tecnica local en el array `attributes` que espera
    POST /items. Para cada atributo definido por la categoría ML, busca el
    valor correspondiente en la ficha (matcheando por id o nombre normalizado)
    y lo agrega como {id, value_name}.

    Si la ficha tiene atributos extras que NO están en la categoría, los
    ignora (ML los rechaza en POST).
    """
    from .catalogo import _norm_attr_key

    if not ficha or not category_attrs:
        return []

    ficha_norm = {_norm_attr_key(k): v for k, v in ficha.items() if v is not None}
    out: list[dict] = []

    for attr in category_attrs:
        attr_id = attr.get("id") or ""
        attr_name = attr.get("name") or ""
        if not attr_id:
            continue

        # Probamos matchear por: id_lowercase, id_normalizado, nombre_normalizado
        for cand in (attr_id.lower(), _norm_attr_key(attr_id), _norm_attr_key(attr_name)):
            if not cand:
                continue
            if cand in ficha_norm:
                value = str(ficha_norm[cand]).strip()
                if value:
                    out.append({"id": attr_id, "value_name": value})
                break

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


def _photo_urls(producto: Producto) -> list[dict]:
    """Lista de URLs de fotos en el formato que espera ML: [{"source": url}, ...]."""
    return [
        {"source": f.url}
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
    """Garantía default según política de Primi: 30 días de fábrica."""
    return [
        {"id": "WARRANTY_TYPE", "value_name": DEFAULT_WARRANTY_TYPE},
        {
            "id": "WARRANTY_TIME",
            "value_name": f"{DEFAULT_WARRANTY_TIME_VALUE} {DEFAULT_WARRANTY_TIME_UNIT}",
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
        "attributes": _ficha_to_ml_attributes(
            producto.ficha_tecnica or {}, category_attrs
        ),
        "status": initial_status,
        # family_name: requerido por ML para categorías con catálogo. Si la
        # categoría no es de catálogo, ML lo ignora silenciosamente.
        "family_name": _derive_family_name(producto),
    }

    # Title: solo si NO es catálogo. En catálogo ML lo genera a partir de la
    # familia + atributos y rechaza si vos lo mandás.
    if not is_catalog:
        payload["title"] = (producto.titulo or "").strip()[:60]

    # Descripción se manda en endpoint separado después del POST. La incluimos
    # acá también porque algunas categorías ML aceptan `description` inline
    # en el POST inicial — si no, igual llamamos PUT /items/{id}/description.
    if (producto.descripcion or "").strip():
        # ML quiere {plain_text: "..."} acá
        payload["description"] = {"plain_text": producto.descripcion.strip()}

    return payload


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

    # Resolver categoría
    if ml_category_id_override:
        ml_cat_id = ml_category_id_override
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
        return False, f"ML rechazó la publicación: {e}", None
    except Exception as e:
        return False, f"Error inesperado creando publicación: {type(e).__name__}: {e}", None

    new_id = resp.get("id")
    if not new_id:
        return False, f"ML respondió sin id de item: {resp}", None

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
    return True, msg, new_id


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
