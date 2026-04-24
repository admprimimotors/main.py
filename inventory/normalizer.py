"""
inventory/normalizer.py
=======================
Funciones puras para normalizar datos sucios de los archivos de proveedores
antes de guardarlos en la base.

No toca la base de datos. Todo lo que está acá se puede testear aislado.
"""

from __future__ import annotations

import re
import unicodedata


# ==========================================================
# Normalización general de texto
# ==========================================================
def quitar_acentos(texto: str) -> str:
    """Quita tildes y diéresis. 'árbol' → 'arbol'."""
    if not texto:
        return ""
    nfkd = unicodedata.normalize("NFKD", texto)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def limpiar_texto(texto: str | None) -> str:
    """Strip + colapsa espacios múltiples. None → ''."""
    if texto is None:
        return ""
    s = str(texto).strip()
    return re.sub(r"\s+", " ", s)


# ==========================================================
# SKU — normalización de núcleo
# ==========================================================
# Regla descubierta analizando los archivos de ZEN:
#   - Archivo de PRECIOS: "0013ZEN"  → núcleo "0013"  (quitar sufijo ZEN)
#   - Archivo de STOCK:   "ZE0013"   → núcleo "0013"  (quitar prefijo ZE)
#   - "ZE0020R"  → núcleo "0020R" (sufijo R = reacondicionado)
#   - "ZE0105.4" → núcleo "0105.4" (variante)

SUFIJOS_MARCA_EN_CODIGO = ("ZEN",)   # suffixes que aparecen en el archivo de precios
PREFIJOS_MARCA_EN_CODIGO = ("ZE",)   # prefixes que aparecen en el archivo de stock


def normalizar_sku(codigo: str | None, quitar_sufijo_R: bool = False) -> str:
    """
    Devuelve el núcleo normalizado del SKU.

    Args:
        codigo: el código crudo ('0013ZEN', 'ZE0013', etc.)
        quitar_sufijo_R: si True, 'ZE0020R' → '0020' (mismo producto base).
                         Si False (default), 'ZE0020R' → '0020R' (producto separado).
    """
    if not codigo:
        return ""
    s = str(codigo).upper().strip()
    s = re.sub(r"\s+", "", s)  # quitar espacios internos

    # 1) Quitar sufijo de marca en códigos tipo "0013ZEN"
    for suf in SUFIJOS_MARCA_EN_CODIGO:
        if s.endswith(suf):
            s = s[: -len(suf)]
            break

    # 2) Quitar prefijo de marca en códigos tipo "ZE0013"
    for pre in PREFIJOS_MARCA_EN_CODIGO:
        if s.startswith(pre):
            # chequear que lo que queda parezca numérico (para no comerse letras de otras marcas)
            resto = s[len(pre):]
            if resto and resto[0].isdigit():
                s = resto
                break

    # 3) Opcional: quitar sufijo R (reacondicionado)
    if quitar_sufijo_R and s.endswith("R") and len(s) > 1 and s[-2].isdigit():
        s = s[:-1]

    return s


def es_reacondicionado(codigo: str | None) -> bool:
    """True si el código termina en 'R' después de un dígito (ZE0020R → True)."""
    if not codigo:
        return False
    s = str(codigo).upper().strip()
    # Quitar prefijo ZE para chequear la 'R' final
    if s.startswith("ZE"):
        s = s[2:]
    return len(s) > 1 and s.endswith("R") and s[-2].isdigit()


# ==========================================================
# Detección de (X PED) — "por pedido"
# ==========================================================
_REGEX_POR_PEDIDO = re.compile(r"\(\s*X\s*PED\s*\)", re.IGNORECASE)


def es_por_pedido(descripcion: str | None) -> bool:
    """True si la descripción contiene '(X PED)' (por pedido)."""
    if not descripcion:
        return False
    return bool(_REGEX_POR_PEDIDO.search(str(descripcion)))


def limpiar_por_pedido(descripcion: str | None) -> str:
    """Devuelve la descripción sin el '(X PED)' para que quede limpia para el título."""
    if not descripcion:
        return ""
    return _REGEX_POR_PEDIDO.sub("", str(descripcion)).strip()


# ==========================================================
# Detección de marca de auto en descripciones
# ==========================================================
# El archivo de ZEN usa descripciones como:
#   "IMP.APL.CHEV.APACHE,C20 DELCO REMY"
#   "IMP.APL.FORD-VW-RENAULT 9 1.6-CHEV.-CHEVETTE BOSCH"
#   "IMP.APL.SCANIA P93 BOSCH"
#
# Estas son las abreviaturas que se usan en los archivos, mapeadas al nombre largo.

ABREVIATURAS_MARCAS = {
    # abreviatura en archivo -> nombre canónico
    "CHEV": "CHEVROLET",
    "CHEV.": "CHEVROLET",
    "CHEVROLET": "CHEVROLET",
    "FORD": "FORD",
    "FIAT": "FIAT",
    "VW": "VOLKSWAGEN",
    "VOLKSWAGEN": "VOLKSWAGEN",
    "RENAULT": "RENAULT",
    "PEUGEOT": "PEUGEOT",
    "PEUG": "PEUGEOT",
    "CITROEN": "CITROEN",
    "CITROËN": "CITROEN",
    "TOYOTA": "TOYOTA",
    "HONDA": "HONDA",
    "NISSAN": "NISSAN",
    "SUZUKI": "SUZUKI",
    "MITSUBISHI": "MITSUBISHI",
    "HYUNDAI": "HYUNDAI",
    "KIA": "KIA",
    "MERCEDES": "MERCEDES-BENZ",
    "M.BENZ": "MERCEDES-BENZ",
    "MBB": "MERCEDES-BENZ",
    "BENZ": "MERCEDES-BENZ",
    "IVECO": "IVECO",
    "SCANIA": "SCANIA",
    "VOLVO": "VOLVO",
    "MAN": "MAN",
    "DAF": "DAF",
    "HINO": "HINO",
    "ISUZU": "ISUZU",
    "AUDI": "AUDI",
    "BMW": "BMW",
    "JEEP": "JEEP",
    "DODGE": "DODGE",
    "RAM": "RAM",
    "CHRYSLER": "CHRYSLER",
    "JD": "JOHN DEERE",
    "JOHN": "JOHN DEERE",
    "DEERE": "JOHN DEERE",
    "CASE": "CASE",
    "AGRALE": "AGRALE",
    "IKA": "IKA",
    "MASSEY": "MASSEY FERGUSON",
    "FERGUSON": "MASSEY FERGUSON",
    "VALTRA": "VALTRA",
    "DEUTZ": "DEUTZ",
    "MWM": "MWM",
    "PERKINS": "PERKINS",
    "CUMMINS": "CUMMINS",
}


def detectar_marcas_auto(descripcion: str | None) -> list[str]:
    """
    Detecta TODAS las marcas de auto mencionadas en la descripción.
    Algunas publicaciones aplican a varios (ej: 'FORD-VW-RENAULT').
    Devuelve una lista ordenada y deduplicada.
    """
    if not descripcion:
        return []

    texto = quitar_acentos(str(descripcion).upper())
    # Reemplazar separadores comunes por espacios para que las abreviaturas queden aisladas
    texto = re.sub(r"[.,;/\-_]", " ", texto)
    tokens = set(texto.split())

    encontradas: list[str] = []
    for token in tokens:
        canonical = ABREVIATURAS_MARCAS.get(token)
        if canonical and canonical not in encontradas:
            encontradas.append(canonical)

    return sorted(encontradas)


def detectar_marca_auto_principal(descripcion: str | None) -> str | None:
    """Devuelve la primera marca detectada (para publicaciones con una sola marca)."""
    marcas = detectar_marcas_auto(descripcion)
    return marcas[0] if marcas else None


# ==========================================================
# Detección de marca del repuesto (BOSCH, DELCO REMY, WAPSA, etc.)
# ==========================================================
# En las descripciones suele venir al final el fabricante del repuesto, como
# "DELCO REMY", "BOSCH", "WAPSA", "NAGARES". Lo extraemos heurísticamente.

MARCAS_REPUESTO = [
    "DELCO REMY", "DELCO", "BOSCH", "WAPSA", "NAGARES", "VALEO",
    "HITACHI", "DENSO", "MITSUBISHI ELECTRIC", "LUCAS", "MAGNETI MARELLI",
    "PRESTOLITE", "ISKRA", "ZEN", "IJS", "RAMSA", "MARELLI",
]


def detectar_marca_repuesto(descripcion: str | None) -> str | None:
    """Busca marcas de fabricante dentro de la descripción."""
    if not descripcion:
        return None
    texto = str(descripcion).upper()
    for marca in MARCAS_REPUESTO:
        if marca in texto:
            return marca
    return None


# ==========================================================
# Mapeo de rubros de proveedor → categorías internas
# ==========================================================
MAPEO_RUBRO_CATEGORIA = {
    "IMPULSORES DE ARRANQUE":                  "Impulsores de arranque",
    "POLEAS DE ALTERNADOR Y OTROS":            "Poleas de alternador",
    "CUBRE IMPULSORES DE ARRANQUE":            "Cubre impulsores",
    "TAPA/PORT.ESCO/BARRILIT":                 "Tapas/portaescobillas/barrilito",
    "DESPIECE DE ALTERNADOR":                  "Despiece de alternador",
    "DESPIECE DE ARRANQUE":                    "Despiece de arranque",
    "RODAMIENTO TENSOR DE CORREA INA/SKF/":    "Rodamientos",
    "CAMPOS DE ARRANQUES VARIOS":              "Campos de arranque",
    "HERRAMIENTAS/FERRETERIA/BULONERA/ALE":    "Herramientas / ferretería",
    "AMORTIGUADORES Y TREN DELANTERO":         "Suspensión",
    "DAEMA - ARTICULOS SIN UBICAR":            "Sin categoría",
    "TALLER":                                  "Taller",
}


def mapear_rubro_a_categoria(rubro: str | None) -> str:
    """Mapea el rubro del proveedor a una categoría interna."""
    if not rubro:
        return "Sin categoría"
    key = str(rubro).strip().upper()
    # match directo
    if key in MAPEO_RUBRO_CATEGORIA:
        return MAPEO_RUBRO_CATEGORIA[key]
    # match flexible: si el rubro empieza con alguna clave conocida
    for clave, categoria in MAPEO_RUBRO_CATEGORIA.items():
        if key.startswith(clave[:20]):
            return categoria
    return "Sin categoría"


# ==========================================================
# Detección automática de categoría por descripción (fallback)
# ==========================================================
# Cuando no tenemos rubro (ej: venta por archivo de PDF sin rubro),
# tratamos de inferir la categoría mirando palabras clave en la descripción.

_REGLAS_CATEGORIA_POR_DESCRIPCION: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\bIMP(ULSOR)?\b",            re.IGNORECASE),  "Impulsores de arranque"),
    (re.compile(r"\bPOLEA\b",                  re.IGNORECASE),  "Poleas de alternador"),
    (re.compile(r"\bCUBRE\s*IMP",              re.IGNORECASE),  "Cubre impulsores"),
    (re.compile(r"\bBARRILIT[OA]?\b",          re.IGNORECASE),  "Tapas/portaescobillas/barrilito"),
    (re.compile(r"\bPORT\.?ESCO|ESCOBILLA",    re.IGNORECASE),  "Tapas/portaescobillas/barrilito"),
    (re.compile(r"\bTAPA\b",                   re.IGNORECASE),  "Tapas/portaescobillas/barrilito"),
    (re.compile(r"\bPI[ÑN]ON\b",               re.IGNORECASE),  "Despiece de arranque"),
    (re.compile(r"\bCAMPO\b",                  re.IGNORECASE),  "Campos de arranque"),
    (re.compile(r"\bRODAMIENTO|TENSOR",        re.IGNORECASE),  "Rodamientos"),
    (re.compile(r"\bPIST[OÓ]N|PISTONES",       re.IGNORECASE),  "Pistones"),
    (re.compile(r"\bBLOCK\b",                  re.IGNORECASE),  "Blocks de motor"),
    (re.compile(r"\bTAPA\s*DE\s*CIL",          re.IGNORECASE),  "Tapas de cilindro"),
    (re.compile(r"\b[AÁ]RBOL\s*DE\s*LEVA",     re.IGNORECASE),  "Árboles de levas"),
    (re.compile(r"\bEMBRAGUE|CLUTCH|PLATO\s*DE\s*EMBRAGUE", re.IGNORECASE), "Embragues"),
    (re.compile(r"\bAMORTIGUADOR|SUSPEN",      re.IGNORECASE),  "Suspensión"),
    (re.compile(r"\bEXTREMO|R[OÓ]TULA|BIELETA|TREN\s*DELANTERO", re.IGNORECASE), "Tren delantero"),
    (re.compile(r"\bPASTILLA|DISCO\s*DE\s*FRENO|TAMBOR|CILINDRO\s*DE\s*FRENO|FRENO", re.IGNORECASE), "Frenos"),
]


def detectar_categoria_por_descripcion(descripcion: str | None) -> str | None:
    """Fallback: si no tenemos rubro, intentamos adivinar por palabras clave."""
    if not descripcion:
        return None
    for patron, categoria in _REGLAS_CATEGORIA_POR_DESCRIPCION:
        if patron.search(str(descripcion)):
            return categoria
    return None


# ==========================================================
# Conversión de números (para PRCOSTO que viene como "29276,84")
# ==========================================================
def parsear_numero_ar(valor) -> float:
    """
    Convierte un número en formato argentino a float.
    '29276,84'      → 29276.84
    '29.276,84'     → 29276.84
    29276.84        → 29276.84 (si ya es número, devuelve tal cual)
    '$ 29.276,84'   → 29276.84
    '' / None / NaN → 0.0
    """
    if valor is None:
        return 0.0
    if isinstance(valor, (int, float)):
        try:
            return float(valor)
        except (TypeError, ValueError):
            return 0.0
    s = str(valor).strip()
    if not s:
        return 0.0
    # Sacar símbolos que no son dígitos, coma, punto o signo negativo
    s = re.sub(r"[^\d,.\-]", "", s)
    if not s:
        return 0.0
    # Si tiene coma y punto: el punto es separador de miles, la coma es decimal
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return 0.0


def parsear_entero(valor) -> int:
    """Convierte un valor a entero, con 0 como default."""
    try:
        return int(parsear_numero_ar(valor))
    except (TypeError, ValueError):
        return 0
