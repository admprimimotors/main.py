"""
ml/atributos.py
===============
Mapeo rico de los datos locales (productos + fichas_tecnicas + compatibilidades)
hacia los atributos estructurados que Mercado Libre exige por categoría.

Por qué existe este módulo
--------------------------
El indicador "calidad de la publicación" de ML (que en nuestro target debe
llegar a ≥70% = "profesional") se mueve, entre otros factores, por:

  - Cantidad de atributos REQUIRED y RECOMMENDED completados en `attributes`.
  - Usar `value_id` en lugar de `value_name` cuando el atributo es tipo lista
    (tiene `allowed_values`). Si mandás value_name libre en un atributo list,
    ML lo toma mal o lo ignora, y la calidad no suma.
  - Respetar `value_struct` para atributos numéricos con unidad (DIAMETER,
    WEIGHT, POWER, etc.) — formato {"number": X, "unit": "mm"}.
  - Mandar BRAND, MODEL, PART_NUMBER, VEHICLE_MAKE/MODEL/YEAR cuando aplique.

Responsabilidades
-----------------
1. Cachear a disco /categories/{id}/attributes (son MBs de JSON, cambian
   pocas veces al mes). Cache TTL = 7 días, en data/ml_attrs/<cat_id>.json.
2. Mapear alias de nombres de campos del negocio ("dientes", "voltaje",
   "sentido de giro", "diámetro") → attribute_id de ML (TEETH_NUMBER,
   VOLTAGE, ROTATION_DIRECTION, DIAMETER).
3. Resolver value_type correcto:
     - list      → value_id (match contra allowed_values por nombre).
     - number    → value_name con el número en string.
     - number_unit → value_struct {"number": X, "unit": "mm"}.
     - string, boolean → value_name.
4. Evaluar completitud: cuántos required/recomm están respondidos.
"""

from __future__ import annotations

import json
import re
import time
import unicodedata
from pathlib import Path
from typing import Any, Optional

import config
from logger import get_logger
from ml import client as ml_client

log = get_logger(__name__)


# ==========================================================
# Cache de atributos de categoría
# ==========================================================
CACHE_DIR = config.DATA_DIR / "ml_attrs"
CACHE_TTL_SECONDS = 7 * 24 * 3600   # 7 días


def _cache_path(category_id: str) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"{category_id}.json"


def obtener_atributos_categoria(
    category_id: str,
    *,
    use_cache: bool = True,
    ttl: int = CACHE_TTL_SECONDS,
) -> list[dict]:
    """
    Devuelve la lista de definiciones de atributos de la categoría.
    Cacheada a disco con TTL. Si la llamada a ML falla y hay cache (aunque
    esté vencido), se devuelve el cache como fallback.
    """
    path = _cache_path(category_id)

    if use_cache and path.exists():
        try:
            mtime = path.stat().st_mtime
            edad = time.time() - mtime
            if edad < ttl:
                data = json.loads(path.read_text(encoding="utf-8"))
                return data
        except Exception as e:
            log.warning(f"cache ml_attrs {category_id} ilegible: {e} — refrescando")

    try:
        resp = ml_client.get(f"/categories/{category_id}/attributes") or []
        if isinstance(resp, list):
            path.write_text(
                json.dumps(resp, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            log.info(f"Atributos ML cacheados: {category_id} ({len(resp)} atributos)")
            return resp
    except Exception as e:
        log.error(f"No pude obtener atributos de {category_id}: {e}")
        # Fallback: cache viejo si existe
        if path.exists():
            try:
                return json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                pass
    return []


# ==========================================================
# Normalización de claves para matchear alias
# ==========================================================
def _normalizar(s: str) -> str:
    """'DIÁMETRO (mm)' → 'diametro'  — saca tildes, unidades, signos, lowercase."""
    if not s:
        return ""
    s = s.strip()
    # Sacar unidad entre paréntesis: "POTENCIA (kW)" -> "POTENCIA"
    s = re.sub(r"\([^)]*\)", "", s)
    # Sacar unidad común pegada al final: "DIAMETRO MM" -> "DIAMETRO"
    s = re.sub(r"\b(mm|cm|kw|kg|gr|volt|voltios?|v|hp|amp|a|nm)\b", "", s, flags=re.IGNORECASE)
    # Normalizar unicode (saca tildes)
    s = unicodedata.normalize("NFD", s)
    s = s.encode("ascii", "ignore").decode("ascii")
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")


# ==========================================================
# Diccionario de ALIAS: nombre del campo local → attribute_id(s) ML
# ==========================================================
# Claves normalizadas (sin tildes, lowercase) → tupla de attribute_ids posibles.
#
# IMPORTANTE: los IDs de atributo en ML son ESPECÍFICOS de cada categoría.
# Por ejemplo, "Dientes" puede ser:
#   - TEETH_NUMBER         (correas, piñones, poleas dentadas)
#   - BENDIX_GEAR_TEETH    (burros de arranque / impulsores — MLA61009)
# Por eso el valor de cada alias es una TUPLA con todos los IDs posibles.
# construir_atributos() recorre la tupla y usa el primero que exista en la
# categoría (los que no existan los descarta silenciosamente).
ALIAS_A_ATTR_ML: dict[str, tuple[str, ...]] = {
    # Identificación (BRAND es la marca del vendedor — ZEN para nosotros —
    # NUNCA del fabricante original de la aplicación OEM)
    "marca": ("BRAND",),
    "modelo": ("MODEL",),
    "sku": ("SELLER_SKU",),
    "codigo": ("PART_NUMBER", "MPN"),
    "codigo_fabricante": ("PART_NUMBER", "MPN"),
    "numero_de_parte": ("PART_NUMBER", "MPN"),
    "numero_parte": ("PART_NUMBER", "MPN"),
    "part_number": ("PART_NUMBER", "MPN"),
    "referencia": ("PART_NUMBER",),
    # OEM / fabricante original (Delco Remy, Bosch, Valeo, etc.) — va al
    # atributo OEM de ML o a APPLICATION_BRAND si existe. NUNCA a BRAND.
    "oem": ("OEM",),
    "codigo_oem": ("OEM",),
    "marca_oem": ("OEM",),
    "marca_original": ("OEM",),
    "marca_aplicacion": ("OEM", "APPLICATION_BRAND"),
    "marca_de_aplicacion": ("OEM", "APPLICATION_BRAND"),
    "aplicacion": ("APPLICATION_BRAND",),
    "fabricante": ("OEM", "MANUFACTURER"),
    "fabricante_original": ("OEM", "MANUFACTURER"),
    "mpn": ("MPN",),
    "gtin": ("GTIN",),
    "ean": ("GTIN",),
    "alternativo": ("ALTERNATIVE_PART_NUMBER",),

    # Características físicas
    "diametro": ("DIAMETER",),
    "diametro_exterior": ("EXTERNAL_DIAMETER", "DIAMETER"),
    "diametro_interior": ("INTERNAL_DIAMETER",),
    "largo": ("LENGTH",),
    "longitud": ("LENGTH",),
    "ancho": ("WIDTH",),
    "alto": ("HEIGHT",),
    "altura": ("HEIGHT",),
    "espesor": ("THICKNESS",),
    "peso": ("WEIGHT",),

    # Motor / eléctrico — impulsores/arranque usan STARTER_VOLTAGE
    "voltaje": ("STARTER_VOLTAGE", "VOLTAGE"),
    "tension": ("STARTER_VOLTAGE", "VOLTAGE"),
    "potencia": ("POWER",),
    "kw": ("POWER",),
    "amperaje": ("AMPERAGE",),
    "frecuencia": ("FREQUENCY",),

    # Dentados — impulsores usan BENDIX_GEAR_TEETH; otros usan TEETH_NUMBER
    "dientes": ("BENDIX_GEAR_TEETH", "TEETH_NUMBER"),
    "n_dientes": ("BENDIX_GEAR_TEETH", "TEETH_NUMBER"),
    "cantidad_de_dientes": ("BENDIX_GEAR_TEETH", "TEETH_NUMBER"),
    "z": ("BENDIX_GEAR_TEETH", "TEETH_NUMBER"),

    # Rotación — impulsores usan DIRECTION_ROTATION; otros ROTATION_DIRECTION
    "sentido_de_giro": ("DIRECTION_ROTATION", "ROTATION_DIRECTION"),
    "sentido_de_rotacion": ("DIRECTION_ROTATION", "ROTATION_DIRECTION"),
    "giro": ("DIRECTION_ROTATION", "ROTATION_DIRECTION"),
    "rotacion": ("DIRECTION_ROTATION", "ROTATION_DIRECTION"),

    # Compatibilidades vehiculares (además leemos tabla compat aparte)
    "marca_del_vehiculo": ("VEHICLE_BRAND", "VEHICLE_MAKE"),
    "marca_vehiculo": ("VEHICLE_BRAND", "VEHICLE_MAKE"),
    "modelo_del_vehiculo": ("VEHICLE_MODEL",),
    "modelo_vehiculo": ("VEHICLE_MODEL",),
    "anio": ("VEHICLE_YEAR",),
    "año": ("VEHICLE_YEAR",),

    # Combustible / motor
    "combustible": ("FUEL_TYPE",),
    "tipo_de_motor": ("ENGINE_TYPE",),
    "cilindrada": ("ENGINE_DISPLACEMENT",),

    # Materiales y colores
    "material": ("MATERIAL",),
    "color": ("MAIN_COLOR", "COLOR"),
    "colores": ("MAIN_COLOR", "COLOR"),

    # Origen / garantía / fiscal / condición
    "origen": ("ORIGIN",),
    "pais_de_origen": ("ORIGIN",),
    "garantia": ("WARRANTY_TIME",),
    "iva": ("VALUE_ADDED_TAX",),
    "alicuota_iva": ("VALUE_ADDED_TAX",),
    "impuesto_interno": ("IMPORT_DUTY",),
}


def _resolver_attr_ids_por_clave(clave_local: str) -> tuple[str, ...]:
    """
    Dado el texto de una clave local, devuelve la TUPLA de attribute_ids
    candidatos (uno o más). Vacía si no hay match.

    Matching:
      1. Exacto contra ALIAS_A_ATTR_ML.
      2. Prefijo, pero iterando en orden DECRECIENTE por longitud del alias,
         para que un alias largo y específico (ej. 'marca_del_vehiculo')
         matchee ANTES que uno genérico ('marca'). Esto evita el bug en el
         que 'Marca OEM' → 'marca_oem' terminaba mapeado a BRAND solo porque
         comparte prefijo con 'marca'.
      3. Nada.
    """
    k = _normalizar(clave_local)
    if not k:
        return ()
    if k in ALIAS_A_ATTR_ML:
        return ALIAS_A_ATTR_ML[k]
    # Match por prefijo ordenado: alias largos primero (los más específicos).
    # Requerimos además un alias mínimo de 4 chars para que letras sueltas
    # como 'z' no matcheen cualquier cosa.
    for alias in sorted(ALIAS_A_ATTR_ML.keys(), key=len, reverse=True):
        if len(alias) < 4:
            continue
        if k.startswith(alias) or alias.startswith(k):
            return ALIAS_A_ATTR_ML[alias]
    # Último recurso: reintentar para aliases cortos (z, etc.) solo si
    # el match es EXACTO (ya cubierto arriba), no por prefijo.
    return ()


# Wrapper legacy por si algo ajeno importa el nombre viejo
def _resolver_attr_id_por_clave(clave_local: str) -> Optional[str]:
    t = _resolver_attr_ids_por_clave(clave_local)
    return t[0] if t else None


# ==========================================================
# Resolución de value_id para atributos tipo list
# ==========================================================
def _match_allowed_value(
    valor: str, allowed_values: list[dict]
) -> Optional[dict]:
    """
    Busca el allowed_value cuyo nombre más se parece al `valor` dado.
    Devuelve {"id": ..., "name": ...} o None.
    """
    if not valor or not allowed_values:
        return None
    v_norm = _normalizar(valor)
    # 1. Exacto por nombre normalizado
    for av in allowed_values:
        if _normalizar(av.get("name", "")) == v_norm:
            return av
    # 2. Prefijo
    for av in allowed_values:
        av_n = _normalizar(av.get("name", ""))
        if av_n and (v_norm.startswith(av_n) or av_n.startswith(v_norm)):
            return av
    # 3. "contiene"
    for av in allowed_values:
        av_n = _normalizar(av.get("name", ""))
        if av_n and av_n in v_norm:
            return av
    return None


# ==========================================================
# Valor → estructura correcta según value_type del atributo
# ==========================================================
_NUM_RE = re.compile(r"-?\d+[.,]?\d*")


def _extraer_numero(valor: str) -> Optional[float]:
    if valor is None:
        return None
    m = _NUM_RE.search(str(valor))
    if not m:
        return None
    s = m.group(0).replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def _normalizar_unidad(u: Optional[str], attr_def: dict) -> Optional[str]:
    """
    Intenta devolver una unidad válida según el atributo ML:
      - si el atributo define `allowed_units`, elegimos la que coincida.
      - si no, devolvemos la unidad dada (minúsculas, sin puntos).
    """
    if not u:
        return attr_def.get("default_unit")
    u_clean = str(u).strip().lower().strip(".")
    allowed = attr_def.get("allowed_units") or []
    if allowed:
        for a in allowed:
            if str(a.get("id", "")).lower() == u_clean or str(a.get("name", "")).lower() == u_clean:
                return a.get("id") or a.get("name")
        # si no matcheó, usar la default
        return attr_def.get("default_unit")
    return u_clean


def _armar_valor(
    valor_local: str,
    unidad_local: Optional[str],
    attr_def: dict,
) -> Optional[dict]:
    """
    Dado un valor local (texto libre) y la definición del atributo en ML,
    devuelve el dict {id, value_name/value_id/value_struct} correcto, o None.
    """
    if not valor_local:
        return None
    vt = (attr_def.get("value_type") or "").lower()
    attr_id = attr_def["id"]

    # 1) list / boolean con allowed_values → resolver value_id
    allowed = attr_def.get("values") or attr_def.get("allowed_values") or []
    if vt in ("list", "boolean") or allowed:
        match = _match_allowed_value(str(valor_local), allowed) if allowed else None
        if match and match.get("id"):
            return {"id": attr_id, "value_id": match["id"], "value_name": match.get("name")}
        # Si la categoría no permite valores libres, descartamos; si permite, mandamos value_name
        if attr_def.get("value_max_length") or not allowed:
            return {"id": attr_id, "value_name": str(valor_local)[:255]}
        return None

    # 2) number_unit → value_struct
    if vt in ("number_unit",):
        num = _extraer_numero(valor_local)
        if num is None:
            return None
        unidad = _normalizar_unidad(unidad_local, attr_def)
        if not unidad:
            # Sin unidad válida — mandamos como string, peor es nada
            return {"id": attr_id, "value_name": f"{num:g}"}
        # ML espera string en value_name también, por compatibilidad
        return {
            "id": attr_id,
            "value_struct": {"number": num, "unit": unidad},
            "value_name": f"{num:g} {unidad}",
        }

    # 3) number puro
    if vt in ("number",):
        num = _extraer_numero(valor_local)
        if num is None:
            return None
        # entero si la fracción es 0
        v = int(num) if num.is_integer() else num
        return {"id": attr_id, "value_name": str(v)}

    # 4) string / default
    return {"id": attr_id, "value_name": str(valor_local)[:255]}


# ==========================================================
# Defaults comerciales / fiscales (se aplican SOLO si la categoría tiene
# el atributo y nadie más lo respondió).
# ==========================================================
# IVA Argentina: 21% general. Monotributistas/exentos se pueden setear
# manualmente en config local cambiando estos defaults.
DEFAULT_VALUE_ADDED_TAX = "21%"
DEFAULT_IMPORT_DUTY = "0 %"   # impuesto interno 0% para la mayoría de repuestos

# Mapeo marca_proveedor → país de origen (para el atributo ORIGIN).
# ZEN es brasilera; para nacionales (p.ej. Orion) sería Argentina; etc.
ORIGEN_POR_MARCA: dict[str, str] = {
    "ZEN": "Brasil",
    "MAGNETTI MARELLI": "Brasil",
    "BOSCH": "Alemania",
    "ORION": "Argentina",
    "ARGAL": "Argentina",
}


def _deducir_origen(marca_proveedor: Optional[str]) -> Optional[str]:
    if not marca_proveedor:
        return None
    k = marca_proveedor.strip().upper()
    if k in ORIGEN_POR_MARCA:
        return ORIGEN_POR_MARCA[k]
    # Por defecto asumimos Argentina (reemplazar si el catálogo cambia)
    return "Argentina"


# ==========================================================
# Construcción de la lista final `attributes` para POST /items
# ==========================================================
def construir_atributos(
    producto: dict,
    fichas: list[dict],
    compatibilidades: list[dict],
    cat_atts: list[dict],
    *,
    vehicle_type: Optional[str] = None,
    part_number_override: Optional[str] = None,
    brand_override: Optional[str] = None,
) -> list[dict]:
    """
    Devuelve la lista `attributes` ya lista para el payload POST /items.

    Parámetros opcionales:
      - vehicle_type: "Auto/Camioneta" o "Línea Pesada". Si no se pasa,
        se usa "Auto/Camioneta" por default.
      - part_number_override: permite forzar el PART_NUMBER con un formato
        definido por el negocio (ej. "ZE0097" para ZEN/Primi). Si no se
        pasa, se cae a sku_proveedor → sku_master → sku_ml.
      - brand_override: marca del artículo publicado (no del OEM). Primi
        Motors publica todo bajo "ZEN" — aunque la columna marca_proveedor
        del producto venga con el OEM ("DELCO REMY", "BOSCH", etc.).
        Cuando se pasa este parámetro, se usa para BRAND y se ignora
        marca_proveedor. Si no se pasa, caemos al comportamiento viejo
        (usar marca_proveedor).

    Reglas:
      - Solo incluimos atributos cuyo `id` esté en la categoría (cat_atts).
      - Nunca pisamos un atributo ya respondido (primera fuente gana:
        productos → fichas_tecnicas → compatibilidades → defaults).
      - Para value_type list, resolvemos value_id contra allowed_values.
      - Para number_unit, usamos value_struct.
      - Cuando un alias tiene múltiples attribute_ids candidatos, usamos el
        primero que exista en la categoría (permite adaptarse a categorías
        específicas como BENDIX_GEAR_TEETH en burros de arranque).
    """
    if not cat_atts:
        return []

    # Índice por attribute_id para acceso rápido y para saber cuáles existen
    por_id: dict[str, dict] = {a["id"]: a for a in cat_atts if a.get("id")}
    resultados: dict[str, dict] = {}  # attr_id → {id, value_name/value_id/value_struct}

    def set_attr(attr_id: str, valor: str, unidad: Optional[str] = None) -> bool:
        """Intenta completar UN attribute_id. Devuelve True si lo agregó."""
        if not attr_id or attr_id in resultados:
            return False
        attr_def = por_id.get(attr_id)
        if not attr_def:
            return False
        v = _armar_valor(valor, unidad, attr_def)
        if v:
            resultados[attr_id] = v
            return True
        return False

    def set_first(attr_ids, valor: str, unidad: Optional[str] = None) -> bool:
        """Intenta una tupla de candidatos, usa el primero que matchea."""
        for aid in (attr_ids or ()):
            if set_attr(aid, valor, unidad):
                return True
        return False

    # ---- 1. Desde columnas fijas de productos ----
    marca_prov = (producto.get("marca_proveedor") or "").strip()
    # BRAND = marca del ARTÍCULO publicado. Si el caller nos pasó un override
    # (típicamente "ZEN" para Primi Motors), lo usamos siempre; así ignoramos
    # marca_proveedor aunque traiga el OEM ("DELCO REMY") por catálogo sucio.
    # Si no hay override, fallback al comportamiento legacy (usar columna).
    marca_articulo = (brand_override or "").strip() or marca_prov
    if marca_articulo:
        set_attr("BRAND", marca_articulo.title())

    sku_ml = (producto.get("sku_ml") or "").strip()
    sku_master = (producto.get("sku_master") or "").strip()
    sku_prov = (producto.get("sku_proveedor") or "").strip()
    # PART_NUMBER: si el caller nos pasó un formato específico (ej "ZE0097"),
    # lo usamos; si no, nos caemos a sku_proveedor → sku_master → sku_ml.
    part_number = (part_number_override or "").strip() or sku_prov or sku_master or sku_ml
    if part_number:
        # PART_NUMBER puede existir como PART_NUMBER o como MPN según categoría
        set_attr("PART_NUMBER", part_number)
        set_attr("MPN", part_number)
        set_attr("SELLER_SKU", sku_master or sku_ml or part_number)

    # Condición — para categorías que lo exponen como atributo
    set_attr("ITEM_CONDITION", "Nuevo")

    # ---- 2. Desde fichas_tecnicas (alias → attr_ids posibles) ----
    for f in fichas or []:
        clave = f.get("clave") or ""
        valor = f.get("valor") or ""
        unidad = f.get("unidad") or ""
        if not clave or not valor:
            continue
        ids = _resolver_attr_ids_por_clave(clave)
        if ids:
            set_first(ids, str(valor), unidad if unidad else None)

    # ---- 3. Desde compatibilidades (solo si hay 1 clara) ----
    if len(compatibilidades or []) == 1:
        c = compatibilidades[0]
        if c.get("marca"):
            set_attr("VEHICLE_BRAND", str(c["marca"]).title())
            # Algunas categorías usan VEHICLE_MAKE en vez de VEHICLE_BRAND
            set_attr("VEHICLE_MAKE", str(c["marca"]).title())
        if c.get("modelo"):
            set_attr("VEHICLE_MODEL", str(c["modelo"]).title())
        if c.get("anio_desde"):
            set_attr("VEHICLE_YEAR", str(c["anio_desde"]))

    # ---- 4. Defaults comerciales / fiscales (última palabra) ----
    # Tipo de vehículo: lo deducimos en la capa de publicaciones (heurística
    # por marca/compatibilidades); acá respetamos lo que nos hayan pasado.
    vt_final = vehicle_type or "Auto/Camioneta"
    set_attr("VEHICLE_TYPE", vt_final)
    # IVA e Impuesto interno: valores típicos del mercado argentino
    set_attr("VALUE_ADDED_TAX", DEFAULT_VALUE_ADDED_TAX)
    set_attr("IMPORT_DUTY", DEFAULT_IMPORT_DUTY)
    # Origen deducido de la MARCA DEL ARTÍCULO (no del OEM). Si publicamos
    # como ZEN (brasilera), el ORIGIN es Brasil — aunque la compatibilidad
    # OEM sea Delco Remy (EE.UU.). Usamos brand_override cuando existe.
    origen = _deducir_origen(marca_articulo)
    if origen:
        set_attr("ORIGIN", origen)

    return list(resultados.values())


# ==========================================================
# Completitud / "score" de calidad de atributos
# ==========================================================
def evaluar_completitud(
    cat_atts: list[dict],
    atributos_completados: list[dict],
) -> dict:
    """
    Devuelve un resumen de cuántos atributos required / recomm / total
    fueron respondidos. Útil para reportar antes de publicar.
    """
    ids_completados = {a.get("id") for a in (atributos_completados or []) if a.get("id")}

    total = 0
    req_total = 0
    req_ok = 0
    recom_total = 0
    recom_ok = 0
    faltantes_req: list[str] = []
    faltantes_recom: list[str] = []

    for a in cat_atts or []:
        total += 1
        tags = a.get("tags") or {}
        nombre = a.get("name") or a.get("id") or ""
        aid = a.get("id")
        if tags.get("required"):
            req_total += 1
            if aid in ids_completados:
                req_ok += 1
            else:
                faltantes_req.append(f"{aid} ({nombre})")
        elif tags.get("catalog_required") or tags.get("conditional_required"):
            # tratarlos como required "suave"
            req_total += 1
            if aid in ids_completados:
                req_ok += 1
            else:
                faltantes_req.append(f"{aid} ({nombre})")
        else:
            recom_total += 1
            if aid in ids_completados:
                recom_ok += 1
            else:
                faltantes_recom.append(f"{aid} ({nombre})")

    pct_req = (req_ok / req_total * 100) if req_total else 100.0
    pct_recom = (recom_ok / recom_total * 100) if recom_total else 0.0
    # Score ponderado: required pesa 70%, recomm 30%
    score = pct_req * 0.7 + pct_recom * 0.3

    return {
        "total_atributos": total,
        "required_total": req_total,
        "required_completados": req_ok,
        "required_pct": round(pct_req, 1),
        "recomm_total": recom_total,
        "recomm_completados": recom_ok,
        "recomm_pct": round(pct_recom, 1),
        "score": round(score, 1),
        "faltantes_required": faltantes_req,
        "faltantes_recomm": faltantes_recom[:20],
    }
