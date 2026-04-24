"""
ml/publicaciones.py
===================
Builder de drafts de publicaciones Mercado Libre a partir de productos del catálogo.

Flujo general:
  1. Recibís un producto_id (o SKU master/ML).
  2. Se arma un título SEO-optimizado ≤60 chars combinando marca + rubro + descripción.
  3. Se predice la categoría ML con /sites/MLA/category_predictor/predict.
  4. Se consultan los atributos requeridos de esa categoría y se completan
     los que el catálogo local conoce (marca, SKU, part number, compatibilidades).
  5. Se guarda un DraftPublicacion en la tabla publicaciones_drafts.
  6. El draft se puede previsualizar con `.to_ml_payload()` para ver el JSON
     que se mandaría a POST /items, pero este módulo NO publica nada.

Reglas acordadas con Federico (Primi Motors):
  - Listing type: gold_pro (más visibilidad).
  - Condition: new.
  - Shipping mode: me2.
  - Garantía: 90 días (warranty_type=seller, warranty_time='90 días').
  - Currency: ARS.
  - Pictures: vacío hasta Chunk B (extracción desde PDF ZEN + carpeta local SKU).
  - Stock: productos.stock_actual (fallback a 1 si es 0).
  - Precio: productos.precio_venta (si 0, el draft queda con estado='borrador'
    y mensaje_error).
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Optional

import db
from logger import get_logger
from ml import client as ml_client
from ml import atributos as mlattrs

log = get_logger(__name__)


# ==========================================================
# Defaults de negocio (alineados con publicación referencia MLA1746973751)
# ==========================================================
DEFAULT_LISTING_TYPE = "gold_special"           # más barato que gold_pro y con misma exposición
DEFAULT_CONDITION = "new"
DEFAULT_SHIPPING_MODE = "me2"
DEFAULT_FREE_SHIPPING = True                    # envío gratis siempre (lo paga el vendedor)
DEFAULT_LOCAL_PICK_UP = True                    # permitir retiro en persona en Berazategui
DEFAULT_CURRENCY = "ARS"
DEFAULT_WARRANTY_TYPE = "Garantía de fábrica"   # coincide con publicación de referencia
DEFAULT_WARRANTY_TIME = "30 días"
MAX_TITULO_CHARS = 60

# CUOTAS PROMOCIONADAS (3-12 con interés bajo, ~5% fee):
#   Se activan PER-ÍTEM vía el tag 'pcj-co-funded' en el campo `tags` del
#   POST /items. Requiere que el vendedor haya habilitado la campaña para la
#   categoría (se hace una sola vez con
#   POST /special_installments/pcj-co-funded/categories/{cat}/enabled).
#   Si el tag no se acepta (vendedor no inscripto en la campaña), ML lo
#   ignora silenciosamente — no rompe el POST.
DEFAULT_CAMPAIGN_TAGS: tuple[str, ...] = ("pcj-co-funded",)

# Configuración de marca para construir PART_NUMBER: "ZE" + sku_master con padding
SKU_PREFIX = "ZE"
SKU_PAD_LENGTH = 4
BUSINESS_NAME = "Primi Motors"
PICKUP_LOCATION = "Berazategui"

# MARCA DEL ARTÍCULO publicado en ML (BRAND attribute + sufijo del título).
# IMPORTANTE: Primi Motors publica TODO bajo la marca "ZEN" — que es la marca
# que Primi vende bajo su catálogo, no la del OEM compatible (Delco Remy,
# Bosch, etc). Hardcodeamos esta constante y la usamos en todos lados
# (título, BRAND attr, descripción) para que no dependa del dato de la
# columna marca_proveedor de productos — que puede venir sucio con el OEM
# en vez de "ZEN" según cómo se haya importado el catálogo.
BUSINESS_BRAND = "ZEN"

# Palabras clave que disparan VEHICLE_TYPE = "Línea Pesada" en vez del default
# "Auto/Camioneta". Se chequean contra descripcion + marca_auto + compatibilidades.
_KEYWORDS_LINEA_PESADA = (
    "CATERPILLAR", "SCANIA", "VOLVO", "IVECO", "MWM", "CUMMINS",
    "MERCEDES BENZ", "MBZ", "FORD CARGO", "VW CAMION", "VW CARGO",
    "CAMION", "CAMIÓN", "TRACTOR", "MAQUINARIA", "DIESEL PESADO",
    "AGRICOLA", "AGRÍCOLA", "COSECHADORA",
)


def deducir_vehicle_type(producto: dict, compatibilidades: Optional[list[dict]] = None) -> str:
    """
    Devuelve 'Línea Pesada' si el producto pertenece a maquinaria pesada
    (por descripción o compatibilidad), si no 'Auto/Camioneta'.
    """
    bag: list[str] = []
    for campo in ("descripcion", "descripcion_larga", "marca_auto", "rubro_origen"):
        v = producto.get(campo) if producto else None
        if v:
            bag.append(str(v).upper())
    for c in compatibilidades or []:
        for campo in ("marca", "modelo", "motor", "notas"):
            v = c.get(campo) if isinstance(c, dict) else None
            if v:
                bag.append(str(v).upper())
    texto = " ".join(bag)
    for kw in _KEYWORDS_LINEA_PESADA:
        if kw in texto:
            return "Línea Pesada"
    return "Auto/Camioneta"


class PublicacionError(Exception):
    """Error al construir o persistir un draft."""


# ==========================================================
# Estructuras
# ==========================================================
@dataclass
class DraftPublicacion:
    """Draft listo para guardarse/previsualizarse. NO se publica automáticamente."""

    producto_id: int
    titulo: str
    descripcion: str
    category_id_ml: Optional[str]
    listing_type_id: str
    condition_ml: str
    precio: float
    currency: str
    stock: int
    atributos: list[dict] = field(default_factory=list)
    pictures: list[dict] = field(default_factory=list)
    shipping_mode: str = DEFAULT_SHIPPING_MODE
    free_shipping: bool = DEFAULT_FREE_SHIPPING
    local_pick_up: bool = DEFAULT_LOCAL_PICK_UP
    warranty_type: str = DEFAULT_WARRANTY_TYPE
    warranty_time: str = DEFAULT_WARRANTY_TIME
    tags: list[str] = field(default_factory=lambda: list(DEFAULT_CAMPAIGN_TAGS))
    mensaje_error: Optional[str] = None
    id: Optional[int] = None  # id en publicaciones_drafts
    estado: str = "borrador"

    def to_ml_payload(self) -> dict:
        """
        Devuelve el JSON que se mandaría a POST /items.

        Importante (flujo 2024+ de ML para repuestos automotores):
          - ML exige `family_name` en el ROOT del body para muchas categorías
            (ej. MLA61009 "Burros de Arranque"). references:["body"] en el error
            400 "body.required_fields [family_name]" lo confirma.
          - Cuando se manda `family_name`, ML NO acepta `title` en el body
            (error 400 "body.invalid_fields [title] are invalid for requested
            call"). ML construye el título mostrado a partir del family_name +
            atributos (BRAND, MODEL, PART_NUMBER, etc.).
          - Por eso: mandamos family_name = título completo (ya limpio y con
            todo el contexto del producto), y OMITIMOS `title`.
        """
        family_name = (self.titulo or "").strip()[:60] or "Repuesto"

        payload: dict[str, Any] = {
            "family_name": family_name,
            "category_id": self.category_id_ml,
            "price": self.precio,
            "currency_id": self.currency,
            "available_quantity": self.stock,
            "buying_mode": "buy_it_now",
            "listing_type_id": self.listing_type_id,
            "condition": self.condition_ml,
            "shipping": {
                "mode": self.shipping_mode,
                "free_shipping": self.free_shipping,
                "local_pick_up": self.local_pick_up,
            },
            "description": {"plain_text": self.descripcion},
        }
        if self.warranty_type:
            payload["sale_terms"] = [
                {"id": "WARRANTY_TYPE", "value_name": self.warranty_type},
                {"id": "WARRANTY_TIME", "value_name": self.warranty_time},
            ]
        if self.atributos:
            payload["attributes"] = self.atributos
        if self.pictures:
            payload["pictures"] = self.pictures
        if self.tags:
            payload["tags"] = list(self.tags)
        return payload


# ==========================================================
# Helpers de texto
# ==========================================================
_MARCAS_MULTI_RE = re.compile(r"\s*,\s*")


def _marca_principal(marca_auto: Optional[str]) -> Optional[str]:
    """'FORD, MWM' -> 'FORD' (la primera)."""
    if not marca_auto:
        return None
    partes = [p.strip() for p in _MARCAS_MULTI_RE.split(marca_auto) if p.strip()]
    return partes[0] if partes else None


def _normalizar_rubro(rubro: Optional[str]) -> Optional[str]:
    if not rubro:
        return None
    r = rubro.strip().upper()
    # Rubros largos → versión corta amigable para título
    mapeo = {
        "IMPULSORES DE ARRANQUE": "Impulsor de Arranque",
        "CUBRE IMPULSORES DE ARRANQUE": "Cubre Impulsor",
        "POLEAS DE ALTERNADOR Y OTROS": "Polea de Alternador",
        "DESPIECE DE ALTERNADOR": "Repuesto Alternador",
        "DESPIECE DE ARRANQUE": "Repuesto Arranque",
        "TAPA/PORT.ESCO/BARRILIT": "Porta Escobillas",
        "RODAMIENTO TENSOR DE CORREA INA/SKF/": "Rodamiento Tensor",
        "AMORTIGUADORES Y TREN DELANTERO": "Amortiguador",
    }
    return mapeo.get(r, rubro.strip().title())


def _limpiar_descripcion_corta(desc: str) -> str:
    """
    Limpia abreviaturas típicas del Excel ZEN para que el título lea natural.

    Fallback cuando el SKU no tiene compatibilidades cargadas: se usa este
    texto como descripción limpia para el título. El flujo preferido es
    armar el título desde compatibilidades_vehiculares (ver construir_titulo).
    """
    if not desc:
        return ""
    d = desc.strip()
    reemplazos = [
        (r"\bIMP\.APL\.\s*", ""),          # IMP.APL.CHEV. → CHEV.
        (r"\bIMP\.\s*", "Impulsor "),
        (r"\bAPL\.\s*", ""),
        (r"\bCHEV\.\s*", "Chevrolet "),    # espacio siempre, aunque venía sin él
        (r"\bMERC\.\s*", "Mercedes "),
        (r"\bMBZ\b", "Mercedes Benz"),
        (r"\bVW\b", "Volkswagen"),
        (r"\bRNLT\b", "Renault"),
        (r"\bT/F\b", "Tipo F"),
        # Quitar referencias a D.REMY / DELCO REMY del título: el OEM va en
        # la ficha técnica y las compatibilidades, NO en el título (porque
        # ML los lee como marca del artículo y rompe BRAND=ZEN).
        (r"\bDELCO\s+REMY\b", ""),
        (r"\bD\s*\.?\s*REMY\b", ""),
        (r"\(X?\s*PED[^)]*\)", ""),        # paréntesis tipo "(X PED)" → fuera
        (r",(?=\S)", ", "),                # coma pegada → coma + espacio
        (r"\s{2,}", " "),
    ]
    for pat, rep in reemplazos:
        d = re.sub(pat, rep, d, flags=re.IGNORECASE)
    return d.strip(" .,-")


def _title_case_smart(s: str) -> str:
    """
    Title Case capitalizando cada palabra, pero preservando siglas cortas
    (≤3 chars con todas mayúsculas en el original, como 'GM', 'MWM', 'VW').
    Respeta palabras que ya tienen mayúsculas internas (camelCase).
    """
    if not s:
        return ""
    palabras = re.split(r"(\s+|[,/])", s)  # preserva separadores
    out: list[str] = []
    for p in palabras:
        if not p.strip():
            out.append(p)
            continue
        # Siglas cortas (2-3 letras, todo mayúscula originalmente) → se dejan
        if len(p) <= 3 and p.isupper() and p.isalpha():
            out.append(p)
            continue
        # Números solos → se dejan
        if p.isdigit():
            out.append(p)
            continue
        out.append(p.capitalize())
    return "".join(out)


def _truncar_por_palabras(s: str, max_chars: int) -> str:
    """Trunca la cadena a max_chars sin cortar palabras."""
    s = (s or "").strip()
    if len(s) <= max_chars:
        return s
    palabras = s.split()
    out: list[str] = []
    acum = 0
    for p in palabras:
        extra = len(p) + (1 if out else 0)
        if acum + extra > max_chars:
            break
        out.append(p)
        acum += extra
    return " ".join(out).strip()


def construir_titulo(
    producto: dict,
    compatibilidades: Optional[list[dict]] = None,
    *,
    max_chars: int = MAX_TITULO_CHARS,
) -> str:
    """
    Construye un título SEO en Title Case con el formato:
        "<Rubro> <MarcaVehículo> <Modelo(s)Vehículo> <MARCA_ARTÍCULO>"

    Ejemplo (acordado con Federico — Primi Motors):
        'Impulsor De Arranque Chevrolet Apache C20 Pick Up ZEN'

    Lógica:
      1. Si hay compatibilidades: la PRIMERA fila de compatibilidades manda.
         Se usa su `marca` y `modelo` como vehículo principal. Si hay más
         filas con la misma `marca`, se suman sus modelos al título hasta
         llenar el espacio (máx 60 chars, respetando palabras).
      2. Si NO hay compatibilidades cargadas: fallback a la descripción
         limpia del Excel ZEN (sin "D.REMY" / "DELCO REMY").
      3. La marca del artículo (ZEN) va SIEMPRE al final, en mayúsculas.

    Por qué la marca del artículo al final:
      MLA61009 tiene BRAND como string libre. Para que la NLP de ML
      respete BRAND=Zen hay que nombrarla explícitamente en el título —
      lo hacemos al final, como cierre, tal como quieren los compradores
      argentinos ("repuesto <vehículo> <MARCA>").
    """
    rubro = _normalizar_rubro(producto.get("rubro_origen"))
    # OJO: para el sufijo NO usamos marca_proveedor (puede venir sucio con el
    # OEM tipo "DELCO REMY"). Siempre usamos BUSINESS_BRAND ("ZEN") — es la
    # marca bajo la cual Primi Motors publica.
    desc_original = producto.get("descripcion") or ""

    # Rubro default si falta
    if not rubro:
        desc_upper = desc_original.upper().strip()
        if desc_upper.startswith(("IMP.APL.", "IMP.")) or "IMPULSOR" in desc_upper:
            rubro = "Impulsor de Arranque"
        elif desc_upper.startswith(("POLEA", "POL.")):
            rubro = "Polea de Alternador"
        else:
            rubro = "Repuesto"

    partes: list[str] = [rubro]

    if compatibilidades:
        # ---- Path A: armar desde compatibilidades ----
        primaria = compatibilidades[0]
        marca_veh = (primaria.get("marca") or "").strip()
        modelo_principal = (primaria.get("modelo") or "").strip()

        if marca_veh:
            partes.append(marca_veh.title())
        if modelo_principal:
            partes.append(modelo_principal)

        # Sumar modelos extra de la misma marca (hasta quedar sin espacio)
        if marca_veh:
            extras = [
                (c.get("modelo") or "").strip()
                for c in compatibilidades[1:]
                if (c.get("marca") or "").strip().upper() == marca_veh.upper()
                and (c.get("modelo") or "").strip()
            ]
            for mod in extras:
                tentativo = " ".join(partes + [mod, BUSINESS_BRAND]).strip()
                if len(tentativo) <= max_chars:
                    partes.append(mod)
                else:
                    break
    else:
        # ---- Path B: fallback a descripción del Excel ----
        desc_limpia = _limpiar_descripcion_corta(desc_original)
        marca_auto = _marca_principal(producto.get("marca_auto"))
        if marca_auto and marca_auto.title() not in desc_limpia.title():
            partes.append(marca_auto.title())
        if desc_limpia:
            partes.append(desc_limpia)

    # ---- Armar cuerpo, truncar respetando espacio para la marca final ----
    cuerpo = " ".join(p for p in partes if p).strip()
    cuerpo = re.sub(r"\s*,\s*", " ", cuerpo)
    cuerpo = re.sub(r"\s+", " ", cuerpo).strip()
    cuerpo = _title_case_smart(cuerpo)

    # La marca del ARTÍCULO va SIEMPRE al final. Hardcodeamos a BUSINESS_BRAND
    # ("ZEN") — no dependemos de marca_proveedor, que puede venir contaminada
    # con el OEM en la DB (ej. "DELCO REMY").
    sufijo_marca = f" {BUSINESS_BRAND}"
    # Reservamos el largo del sufijo para que ZEN NUNCA se corte
    espacio_cuerpo = max_chars - len(sufijo_marca)
    cuerpo_trunc = _truncar_por_palabras(cuerpo, espacio_cuerpo)

    titulo = (cuerpo_trunc + sufijo_marca).strip()
    return titulo


def construir_part_number(producto: dict) -> str:
    """
    Genera el PART_NUMBER con formato ZE + sku_master con padding 4 dígitos,
    igual a la publicación de referencia ('ZE0097' para sku_master='0097').
    Si el sku_master no es numérico, devuelve el sku_proveedor o sku_master tal cual.
    """
    sku_master = (producto.get("sku_master") or "").strip()
    if sku_master.isdigit():
        return f"{SKU_PREFIX}{sku_master.zfill(SKU_PAD_LENGTH)}"
    sku_prov = (producto.get("sku_proveedor") or "").strip()
    return sku_prov or sku_master or ""


def construir_descripcion(
    producto: dict,
    compatibilidades: list[dict],
    fichas: Optional[list[dict]] = None,
) -> str:
    """
    Descripción larga (plain text) con el template Primi Motors, inspirado en
    la publicación de referencia MLA1746973751. Estructura:

        <descripción en prosa corta — enganche>

        ✅ FICHA TÉCNICA
        • Marca: ...
        • Código: ...
        • (cada fila de fichas_tecnicas)

        ✅ COMPATIBILIDADES
        • Marca Modelo (años) motor

        ✅ VENDIDO POR PRIMI MOTORS®
        <bajada de confianza>

        ✅ GARANTÍA
        30 días de garantía de fábrica contra vicios de fabricación.

        ✅ HORARIOS DE ATENCIÓN
        Lunes a Viernes de 9 a 18 hs · Sábados de 9 a 13 hs

        ✅ ENVÍOS
        • GBA / CABA: despacho en el día hábil siguiente.
        • Interior del país: despacho en 24-48 hs por Mercado Envíos.

        ✅ PUNTO DE RETIRO
        Berazategui, Buenos Aires (coordinado por mensaje previo).

        ¿Dudas? Consultanos por mensaje — respondemos dentro del horario comercial.
    """
    bloques: list[str] = []

    # --- 0. Enganche: descripción corta en prosa ---
    desc_larga = (producto.get("descripcion_larga") or "").strip()
    desc_base = desc_larga or _limpiar_descripcion_corta(producto.get("descripcion") or "")
    if desc_base:
        bloques.append(desc_base)

    # --- 1. FICHA TÉCNICA ---
    ficha_lineas: list[str] = []
    # Marca del artículo = BUSINESS_BRAND (ZEN). NUNCA usamos marca_proveedor
    # acá, porque en el catálogo esa columna a veces contiene el OEM
    # ("DELCO REMY") en vez de la marca comercial real.
    ficha_lineas.append(f"• Marca: {BUSINESS_BRAND.title()}")
    # Código con el formato ZE + padding (ej. ZE0097)
    codigo = construir_part_number(producto)
    if codigo:
        ficha_lineas.append(f"• Código: {codigo}")
    rubro = producto.get("rubro_origen")
    if rubro:
        ficha_lineas.append(f"• Rubro: {_normalizar_rubro(rubro) or rubro.title()}")
    # El OEM (Delco Remy, Bosch, etc.) que venga sucio en marca_proveedor lo
    # mostramos en la ficha como "Aplicación OEM" — es información útil para
    # el comprador pero NO es la marca del artículo.
    marca_oem = (producto.get("marca_proveedor") or "").strip()
    if marca_oem and marca_oem.upper() != BUSINESS_BRAND.upper():
        ficha_lineas.append(f"• Aplicación OEM: {marca_oem.title()}")

    # Fichas técnicas detalladas
    fichas_ord = sorted(
        fichas or [],
        key=lambda f: (f.get("orden", 99), f.get("clave", "")),
    )
    for f in fichas_ord:
        clave = (f.get("clave") or "").strip()
        valor = (f.get("valor") or "").strip()
        unidad = (f.get("unidad") or "").strip()
        if not clave or not valor:
            continue
        linea_valor = f"{valor} {unidad}".strip() if unidad else valor
        ficha_lineas.append(f"• {clave.title()}: {linea_valor}")

    if ficha_lineas:
        bloques.append("✅ FICHA TÉCNICA\n" + "\n".join(ficha_lineas))

    # --- 2. COMPATIBILIDADES ---
    compat_lineas: list[str] = []
    for c in compatibilidades or []:
        partes_c = []
        if c.get("marca"):
            partes_c.append(str(c["marca"]).title())
        if c.get("modelo"):
            partes_c.append(str(c["modelo"]))
        linea = " ".join(partes_c)
        d = c.get("anio_desde") or ""
        h = c.get("anio_hasta") or ""
        if d and h:
            linea += f" ({d}-{h})"
        elif d:
            linea += f" (desde {d})"
        elif h:
            linea += f" (hasta {h})"
        if c.get("motor"):
            linea += f" · motor {c['motor']}"
        if linea.strip():
            compat_lineas.append(f"• {linea.strip()}")
    if compat_lineas:
        bloques.append("✅ COMPATIBILIDADES\n" + "\n".join(compat_lineas))

    # --- 3. VENDEDOR ---
    bloques.append(
        f"✅ VENDIDO POR {BUSINESS_NAME.upper()}®\n"
        "Somos especialistas en repuestos de arranque y alternadores. "
        "Trabajamos con primeras marcas (ZEN, Bosch, Magneti Marelli, Orion) y "
        "despachamos siempre con factura A o B."
    )

    # --- 4. GARANTÍA ---
    bloques.append(
        "✅ GARANTÍA\n"
        f"{DEFAULT_WARRANTY_TIME} de {DEFAULT_WARRANTY_TYPE.lower()} contra vicios de fabricación. "
        "No cubre daños por instalación incorrecta ni uso indebido del repuesto."
    )

    # --- 5. HORARIOS ---
    bloques.append(
        "✅ HORARIOS DE ATENCIÓN\n"
        "Lunes a Viernes de 9 a 18 hs · Sábados de 9 a 13 hs"
    )

    # --- 6. ENVÍOS ---
    bloques.append(
        "✅ ENVÍO GRATIS A TODO EL PAÍS\n"
        "• GBA / CABA: despacho en el día hábil siguiente.\n"
        "• Interior del país: despacho en 24-48 hs hábiles vía Mercado Envíos.\n"
        "• También podés pagar hasta en 12 cuotas con interés bajo."
    )

    # --- 7. RETIRO ---
    bloques.append(
        "✅ PUNTO DE RETIRO\n"
        f"{PICKUP_LOCATION}, Buenos Aires (coordinar previamente por mensaje)."
    )

    # --- Cierre ---
    bloques.append(
        "¿Dudas? Consultanos por mensaje — respondemos dentro del horario comercial."
    )

    return "\n\n".join(bloques).strip()


# ==========================================================
# Categoría ML (predicción)
# ==========================================================
def predecir_categoria_ml(titulo: str, site_id: str = "MLA") -> Optional[str]:
    """
    Predice la categoría ML para un título. Usa el nuevo endpoint
    /sites/{SITE}/domain_discovery/search (el antiguo /category_predictor/predict
    fue descontinuado por Mercado Libre en 2024-2025).

    Devuelve el category_id ("MLAxxxxx") o None si falla.
    """
    # 1) Intentar con domain_discovery/search (oficial, reemplazo del predictor)
    try:
        resp = ml_client.get(
            f"/sites/{site_id}/domain_discovery/search",
            params={"q": titulo, "limit": 1},
        )
    except Exception as e:
        log.warning(f"domain_discovery falló para '{titulo}': {e}")
        resp = None

    if resp:
        if isinstance(resp, list):
            resp = resp[0] if resp else {}
        if isinstance(resp, dict):
            cat_id = resp.get("category_id")
            if cat_id:
                log.info(f"Categoría predicha para '{titulo}' → {cat_id} "
                         f"({resp.get('category_name', '?')})")
                return cat_id

    # 2) Fallback: el endpoint viejo (por si ML lo reactiva en algún site)
    try:
        resp = ml_client.get(
            f"/sites/{site_id}/category_predictor/predict",
            params={"title": titulo},
        )
        if resp:
            if isinstance(resp, list):
                resp = resp[0] if resp else {}
            if isinstance(resp, dict) and resp.get("id"):
                return resp["id"]
    except Exception:
        pass  # endpoint descontinuado; ya avisamos arriba

    log.warning(f"No se pudo predecir categoría para '{titulo}'")
    return None


def obtener_atributos_categoria(category_id: str) -> list[dict]:
    """
    Devuelve la lista de atributos definidos para esa categoría.
    Delega en ml.atributos.obtener_atributos_categoria que cachea a disco
    (TTL 7 días en data/ml_attrs/<cat>.json).
    """
    try:
        return mlattrs.obtener_atributos_categoria(category_id)
    except Exception as e:
        log.warning(f"No se pudieron leer atributos de {category_id}: {e}")
        return []


# ==========================================================
# Compatibilidades desde DB
# ==========================================================
def _leer_compatibilidades(producto_id: int) -> list[dict]:
    with db.conexion() as c:
        rows = c.execute(
            """
            SELECT marca, modelo, anio_desde, anio_hasta, motor, notas
            FROM compatibilidades_vehiculares
            WHERE producto_id = ?
            ORDER BY marca, modelo, anio_desde
            """,
            (producto_id,),
        ).fetchall()
    return [dict(r) for r in rows]


# ==========================================================
# Ficha técnica desde DB (tabla fichas_tecnicas — alimentada por zen_pdf)
# ==========================================================
def _leer_fichas_tecnicas(producto_id: int) -> list[dict]:
    """Lee los campos de ficha técnica para construir la descripción."""
    try:
        with db.conexion() as c:
            rows = c.execute(
                """
                SELECT clave, valor, unidad, orden
                FROM fichas_tecnicas
                WHERE producto_id = ?
                ORDER BY orden, clave
                """,
                (producto_id,),
            ).fetchall()
        return [dict(r) for r in rows]
    except Exception as e:
        # Si la tabla todavía no existe (schema viejo), no rompemos el draft.
        log.warning(f"No se pudieron leer fichas_tecnicas para producto {producto_id}: {e}")
        return []


# ==========================================================
# Mapeo de atributos locales → atributos ML
# ==========================================================
def _mapear_atributos(
    producto: dict,
    compatibilidades: list[dict],
    atributos_categoria: list[dict],
    *,
    fichas: Optional[list[dict]] = None,
    titulo: Optional[str] = None,
) -> list[dict]:
    """
    Wrapper histórico que ahora delega en ml.atributos.construir_atributos,
    el cual:
      - Resuelve value_id para atributos tipo list (respeta allowed_values).
      - Usa value_struct {"number": X, "unit": ...} para number_unit.
      - Mapea alias de fichas_tecnicas → attribute_ids de ML (DIENTES→TEETH_NUMBER, etc.).
      - Nunca pisa un atributo ya respondido (productos > fichas > compatibilidades).

    `titulo` se acepta por compatibilidad pero ya no se usa (family_name lo
    arma el payload).

    Además inyectamos al mapper:
      - vehicle_type heurístico (Auto/Camioneta vs Línea Pesada).
      - part_number formateado "ZE0097" para alinear con publicación referencia.
    """
    return mlattrs.construir_atributos(
        producto=producto,
        fichas=fichas or [],
        compatibilidades=compatibilidades or [],
        cat_atts=atributos_categoria or [],
        vehicle_type=deducir_vehicle_type(producto, compatibilidades),
        part_number_override=construir_part_number(producto),
        brand_override=BUSINESS_BRAND,
    )


# ==========================================================
# Builder principal
# ==========================================================
def construir_draft(producto_id: int, *, predecir_cat: bool = True) -> DraftPublicacion:
    """
    Construye un DraftPublicacion para el producto_id dado.
    Si `predecir_cat=False`, no llama a ML (útil para tests offline).
    """
    with db.conexion() as c:
        row = c.execute("SELECT * FROM productos WHERE id = ?", (producto_id,)).fetchone()
        if row is None:
            raise PublicacionError(f"Producto id={producto_id} no existe.")
        producto = dict(row)

    compatibilidades = _leer_compatibilidades(producto_id)
    fichas = _leer_fichas_tecnicas(producto_id)

    titulo = construir_titulo(producto, compatibilidades)
    descripcion = construir_descripcion(producto, compatibilidades, fichas)

    category_id_ml: Optional[str] = None
    atributos: list[dict] = []
    score_info: Optional[dict] = None
    if predecir_cat:
        category_id_ml = predecir_categoria_ml(titulo)
        if category_id_ml:
            atts_cat = obtener_atributos_categoria(category_id_ml)
            atributos = _mapear_atributos(
                producto, compatibilidades, atts_cat,
                fichas=fichas, titulo=titulo,
            )
            # Score de calidad (para visibilidad en logs / dashboard)
            try:
                score_info = mlattrs.evaluar_completitud(atts_cat, atributos)
                log.info(
                    f"Producto {producto_id} '{titulo[:40]}' → {category_id_ml} "
                    f"score={score_info['score']}%  "
                    f"req={score_info['required_completados']}/{score_info['required_total']} "
                    f"recom={score_info['recomm_completados']}/{score_info['recomm_total']}"
                )
                if score_info["faltantes_required"]:
                    log.warning(
                        f"  REQ faltantes: {', '.join(score_info['faltantes_required'][:10])}"
                    )
            except Exception as e:
                log.warning(f"No se pudo calcular score de atributos: {e}")

    precio = float(producto.get("precio_venta") or 0)
    stock_actual = int(producto.get("stock_actual") or 0)
    stock = max(stock_actual, 1)

    mensaje_error: Optional[str] = None
    if precio <= 0:
        mensaje_error = "Precio en 0 — hay que setearlo antes de publicar."

    draft = DraftPublicacion(
        producto_id=producto_id,
        titulo=titulo,
        descripcion=descripcion,
        category_id_ml=category_id_ml,
        listing_type_id=DEFAULT_LISTING_TYPE,
        condition_ml=DEFAULT_CONDITION,
        precio=precio,
        currency=DEFAULT_CURRENCY,
        stock=stock,
        atributos=atributos,
        pictures=[],
        shipping_mode=DEFAULT_SHIPPING_MODE,
        free_shipping=DEFAULT_FREE_SHIPPING,
        local_pick_up=DEFAULT_LOCAL_PICK_UP,
        warranty_type=DEFAULT_WARRANTY_TYPE,
        warranty_time=DEFAULT_WARRANTY_TIME,
        tags=list(DEFAULT_CAMPAIGN_TAGS),
        mensaje_error=mensaje_error,
        estado="borrador",
    )
    return draft


# ==========================================================
# Persistencia en publicaciones_drafts
# ==========================================================
def guardar_draft(draft: DraftPublicacion) -> int:
    """Guarda el draft en publicaciones_drafts y devuelve su id."""
    with db.conexion() as c:
        cur = c.execute(
            """
            INSERT INTO publicaciones_drafts (
                producto_id, titulo, descripcion, category_id_ml,
                listing_type_id, condition_ml, precio, currency, stock,
                atributos_json, pictures_json, shipping_mode,
                warranty_type, warranty_time, estado, mensaje_error
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                draft.producto_id,
                draft.titulo,
                draft.descripcion,
                draft.category_id_ml,
                draft.listing_type_id,
                draft.condition_ml,
                draft.precio,
                draft.currency,
                draft.stock,
                json.dumps(draft.atributos, ensure_ascii=False),
                json.dumps(draft.pictures, ensure_ascii=False),
                draft.shipping_mode,
                draft.warranty_type,
                draft.warranty_time,
                draft.estado,
                draft.mensaje_error,
            ),
        )
        nuevo_id = cur.lastrowid
    draft.id = nuevo_id
    log.info(f"Draft #{nuevo_id} guardado para producto {draft.producto_id} — '{draft.titulo}'")
    return nuevo_id


def obtener_draft(draft_id: int) -> Optional[DraftPublicacion]:
    with db.conexion() as c:
        row = c.execute("SELECT * FROM publicaciones_drafts WHERE id = ?", (draft_id,)).fetchone()
    if row is None:
        return None
    d = dict(row)
    draft = DraftPublicacion(
        producto_id=d["producto_id"],
        titulo=d["titulo"],
        descripcion=d["descripcion"] or "",
        category_id_ml=d.get("category_id_ml"),
        listing_type_id=d["listing_type_id"],
        condition_ml=d["condition_ml"],
        precio=float(d["precio"] or 0),
        currency=d["currency"],
        stock=int(d["stock"] or 0),
        atributos=json.loads(d["atributos_json"] or "[]"),
        pictures=json.loads(d["pictures_json"] or "[]"),
        shipping_mode=d["shipping_mode"] or DEFAULT_SHIPPING_MODE,
        warranty_type=d.get("warranty_type") or DEFAULT_WARRANTY_TYPE,
        warranty_time=d.get("warranty_time") or DEFAULT_WARRANTY_TIME,
        mensaje_error=d.get("mensaje_error"),
        id=d["id"],
        estado=d["estado"],
    )
    return draft


def listar_drafts(*, estado: Optional[str] = None, limite: int = 100) -> list[dict]:
    with db.conexion() as c:
        sql = [
            """
            SELECT d.id, d.producto_id, p.sku_master, p.sku_ml,
                   d.titulo, d.category_id_ml, d.precio, d.stock,
                   d.estado, d.mensaje_error, d.fecha_creacion
            FROM publicaciones_drafts d
            JOIN productos p ON p.id = d.producto_id
            WHERE 1=1
            """
        ]
        params: list = []
        if estado:
            sql.append("AND d.estado = ?")
            params.append(estado)
        sql.append("ORDER BY d.id DESC LIMIT ?")
        params.append(limite)
        return [dict(r) for r in c.execute(" ".join(sql), params).fetchall()]


# ==========================================================
# Helper combinado: construir + guardar
# ==========================================================
def preparar_draft(producto_id: int, *, predecir_cat: bool = True, guardar: bool = True) -> DraftPublicacion:
    """Construye y (opcionalmente) guarda en un solo paso."""
    draft = construir_draft(producto_id, predecir_cat=predecir_cat)
    if guardar:
        guardar_draft(draft)
    return draft
