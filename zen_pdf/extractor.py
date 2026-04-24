"""
zen_pdf/extractor.py
====================
Extrae fichas técnicas y fotos del PDF de ZEN (ImpulsoresdePartida.pdf).

Layout del PDF:
  - El catálogo de productos empieza en la página 284 (aprox).
  - 16 productos por página en grilla 4x4.
  - Columnas de SKU en x ≈ 57.8 / 194.5 / 331.1 / 467.7.
  - Filas de SKU en y ≈ 111.5 / 275.9 / 440.3 / 604.7.
  - Arriba de cada SKU hay una foto del producto.
  - Debajo del SKU hay 8 líneas de ficha técnica:
      T (dientes), G (ø piñón mm), L (largo mm), SPL (estrías),
      ID (ø bucha mm), D (ø capa mm), FAM (familia), CW/CCW (giro).

Persiste:
  - Ficha técnica → tabla fichas_tecnicas (clave-valor por producto).
  - Fotos        → data/fotos/<sku>/zen.png

Uso desde otros módulos:
    from zen_pdf import extractor as zpe
    resultado = zpe.extraer(max_productos=10)
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import pdfplumber

import config
import db
from logger import get_logger

log = get_logger(__name__)


# ==========================================================
# Configuración
# ==========================================================
# Busca el PDF en los siguientes paths (en orden). El primero que exista gana.
# Si ninguno existe, queda el primero como "default" (el mensaje de error
# indicará al usuario dónde ponerlo o cómo pasar --pdf).
_PDF_CANDIDATES = [
    config.DATA_DIR / "ImpulsoresdePartida.pdf",                                 # data/ (recomendado)
    config.BASE_DIR / "ImpulsoresdePartida.pdf",                                 # raíz del código
    Path("/sessions/youthful-bold-faraday/mnt/uploads/ImpulsoresdePartida.pdf"),  # sandbox
]


def _resolver_pdf_default() -> Path:
    for p in _PDF_CANDIDATES:
        if p.exists():
            return p
    return _PDF_CANDIDATES[0]  # no existe; devolvemos el "amigable" para el error


PDF_PATH_DEFAULT = _resolver_pdf_default()
FOTOS_DIR = config.DATA_DIR / "fotos"
PAGINA_INICIO_CATALOGO = 284        # 1-indexed
PAGINA_FIN_CATALOGO_DEFAULT = 368   # las tablas "vehículos/motores" ya pasaron

# Mapeo abreviatura ZEN → (display legible, unidad, orden)
ETIQUETAS_FICHA: dict[str, tuple[str, str, int]] = {
    "T":      ("Dientes del piñón",     "",   1),
    "G":      ("ø externo del piñón",   "mm", 2),
    "L":      ("Largo total",           "mm", 3),
    "SPL":    ("Estrías",               "",   4),
    "ID":     ("ø interno (bucha)",     "mm", 5),
    "D":      ("ø de la capa",          "mm", 6),
    "FAM":    ("Familia ZEN",           "",   7),
    "CW/CCW": ("Sentido de giro",       "",   8),
}

# Regex para detectar un SKU ZEN: 4 dígitos exactos
RE_SKU = re.compile(r"^\d{4}$")

# Encabezado que marca una página de catálogo de productos
HEADER_CATALOGO = "PRODUTOS (Impulsor de Partida)"


# ==========================================================
# Estructuras
# ==========================================================
@dataclass
class FichaTecnica:
    sku: str
    datos: dict[str, str] = field(default_factory=dict)  # clave abreviada → valor

    def to_lista(self) -> list[tuple[str, str, str, int]]:
        """Devuelve lista (display, valor, unidad, orden) en orden ZEN."""
        out = []
        for clave_abrev, (display, unidad, orden) in ETIQUETAS_FICHA.items():
            if clave_abrev in self.datos:
                out.append((display, self.datos[clave_abrev], unidad, orden))
        return out


@dataclass
class ResultadoExtraccion:
    paginas_procesadas: int = 0
    productos_vistos: int = 0
    fichas_creadas: int = 0
    fotos_guardadas: int = 0
    skus_no_encontrados_en_db: list[str] = field(default_factory=list)
    errores: list[str] = field(default_factory=list)


# ==========================================================
# Helpers
# ==========================================================
def _es_pagina_catalogo(page) -> bool:
    """¿Esta página es del catálogo de productos?"""
    try:
        txt = page.extract_text() or ""
    except Exception:
        return False
    return HEADER_CATALOGO in txt


def _agrupar_en_lineas(palabras: list[dict], tol: float = 3.0) -> list[list[dict]]:
    """Agrupa palabras por su top aproximado (línea visual)."""
    if not palabras:
        return []
    ordenadas = sorted(palabras, key=lambda w: (w["top"], w["x0"]))
    lineas: list[list[dict]] = []
    linea_actual: list[dict] = []
    linea_top: Optional[float] = None
    for w in ordenadas:
        if linea_top is None or abs(w["top"] - linea_top) < tol:
            linea_actual.append(w)
            linea_top = w["top"] if linea_top is None else linea_top
        else:
            lineas.append(linea_actual)
            linea_actual = [w]
            linea_top = w["top"]
    if linea_actual:
        lineas.append(linea_actual)
    return lineas


def _parsear_bloque_producto(bloque: list[dict]) -> dict[str, str]:
    """
    Dada la lista de palabras dentro del bloque de un producto, devuelve
    el dict de datos de ficha técnica.
    """
    datos: dict[str, str] = {}
    for linea in _agrupar_en_lineas(bloque):
        if not linea:
            continue
        primera = linea[0]["text"]
        if primera in ETIQUETAS_FICHA:
            valor = " ".join(w["text"] for w in linea[1:]).strip()
            # Limpiar pegados tipo "CW/CCWHorário"
            if primera == "CW/CCW" and not valor:
                # Puede venir pegado con la siguiente palabra
                pass
            if valor:
                datos[primera] = valor
    return datos


def _parse_pagina(page) -> list[tuple[FichaTecnica, dict]]:
    """
    Extrae todos los productos de una página de catálogo.
    Devuelve una lista de (FichaTecnica, sku_word_dict) donde sku_word_dict
    trae las coordenadas del SKU para poder recortar la foto después.
    """
    palabras = page.extract_words()
    # SKUs candidatos: 4 dígitos y que estén en las 4 filas esperadas del layout.
    # Rows típicas (top): ~111.5, 275.9, 440.3, 604.7. Dejamos una tolerancia de 10.
    FILAS_Y = [111.5, 275.9, 440.3, 604.7]
    TOL = 10.0

    def _en_fila(w) -> bool:
        return any(abs(w["top"] - y) <= TOL for y in FILAS_Y)

    skus = [w for w in palabras if RE_SKU.match(w["text"]) and _en_fila(w)]
    if not skus:
        return []

    resultado: list[tuple[FichaTecnica, dict]] = []
    for sku_w in skus:
        col_left = sku_w["x0"] - 20
        col_right = sku_w["x0"] + 125
        row_top = sku_w["top"] + 5     # un poco después del SKU
        row_bottom = sku_w["top"] + 160  # alto máximo de una fila

        bloque = [
            w for w in palabras
            if col_left <= w["x0"] <= col_right
               and row_top <= w["top"] <= row_bottom
        ]
        datos = _parsear_bloque_producto(bloque)
        ficha = FichaTecnica(sku=sku_w["text"], datos=datos)
        resultado.append((ficha, sku_w))
    return resultado


# ==========================================================
# Extracción de fotos
# ==========================================================
def _recortar_foto(page, sku_w: dict, out_path: Path, resolution: int = 150) -> bool:
    """
    Recorta la región justo ARRIBA del SKU y la guarda como PNG.
    """
    x0 = max(0.0, sku_w["x0"] - 20)
    x1 = min(float(page.width), sku_w["x0"] + 110)
    y1 = max(0.0, sku_w["top"] - 5)        # foto termina ~5px antes del SKU
    y0 = max(0.0, y1 - 140)                # alto típico ~100-130
    if x1 <= x0 or y1 <= y0:
        return False

    try:
        cropped = page.crop((x0, y0, x1, y1))
        img = cropped.to_image(resolution=resolution)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        img.save(str(out_path), format="PNG")
        return True
    except Exception as e:
        log.warning(f"No se pudo recortar foto SKU {sku_w.get('text')}: {e}")
        return False


# ==========================================================
# Persistencia
# ==========================================================
def _guardar_ficha(producto_id: int, ficha: FichaTecnica) -> int:
    """Upsert en fichas_tecnicas. Devuelve cantidad de campos escritos."""
    n = 0
    with db.conexion() as c:
        for display, valor, unidad, orden in ficha.to_lista():
            c.execute(
                """
                INSERT INTO fichas_tecnicas (producto_id, clave, valor, unidad, orden)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(producto_id, clave) DO UPDATE SET
                    valor  = excluded.valor,
                    unidad = excluded.unidad,
                    orden  = excluded.orden
                """,
                (producto_id, display, valor, unidad, orden),
            )
            n += 1
    return n


# ==========================================================
# Entry point público
# ==========================================================
def extraer(
    pdf_path: Path = PDF_PATH_DEFAULT,
    *,
    pagina_inicio: int = PAGINA_INICIO_CATALOGO,
    pagina_fin: Optional[int] = None,
    max_productos: Optional[int] = None,
    extraer_fotos: bool = True,
    guardar_db: bool = True,
    dry_run: bool = False,
    fotos_dir: Path = FOTOS_DIR,
) -> ResultadoExtraccion:
    """
    Recorre el PDF ZEN y extrae ficha técnica + fotos.

    Args:
        pdf_path       : path al PDF (default: uploads/ImpulsoresdePartida.pdf)
        pagina_inicio  : primera página 1-indexed.
        pagina_fin     : última página (None = hasta PAGINA_FIN_CATALOGO_DEFAULT).
        max_productos  : corta después de N productos (para tests).
        extraer_fotos  : si True, guarda PNG a fotos_dir/<sku>/zen.png
        guardar_db     : si True, hace upsert a fichas_tecnicas.
        dry_run        : si True, ni DB ni fotos — solo cuenta y reporta.
        fotos_dir      : destino de las fotos.
    """
    resultado = ResultadoExtraccion()

    if not pdf_path.exists():
        resultado.errores.append(f"PDF no existe: {pdf_path}")
        return resultado

    # Productos en DB por SKU master
    with db.conexion() as c:
        rows = c.execute("SELECT id, sku_master FROM productos").fetchall()
    sku_a_id = {r["sku_master"]: r["id"] for r in rows}
    log.info(f"Productos en DB: {len(sku_a_id)}")

    with pdfplumber.open(pdf_path) as pdf:
        total_pag = len(pdf.pages)
        fin = min(pagina_fin or PAGINA_FIN_CATALOGO_DEFAULT, total_pag)

        for idx in range(pagina_inicio - 1, fin):
            if max_productos and resultado.productos_vistos >= max_productos:
                break
            page = pdf.pages[idx]
            if not _es_pagina_catalogo(page):
                continue

            try:
                productos = _parse_pagina(page)
            except Exception as e:
                resultado.errores.append(f"Pág {idx+1}: {e}")
                log.warning(f"Pág {idx+1} falló: {e}")
                continue

            if not productos:
                continue

            resultado.paginas_procesadas += 1
            log.info(f"Pág {idx+1}: {len(productos)} productos encontrados")

            for ficha, sku_w in productos:
                if max_productos and resultado.productos_vistos >= max_productos:
                    break
                resultado.productos_vistos += 1

                producto_id = sku_a_id.get(ficha.sku)
                if producto_id is None:
                    resultado.skus_no_encontrados_en_db.append(ficha.sku)
                    continue

                # Ficha técnica
                if guardar_db and not dry_run and ficha.datos:
                    try:
                        _guardar_ficha(producto_id, ficha)
                        resultado.fichas_creadas += 1
                    except Exception as e:
                        resultado.errores.append(f"SKU {ficha.sku}: {e}")
                        log.warning(f"SKU {ficha.sku}: {e}")

                # Foto
                if extraer_fotos and not dry_run:
                    out = fotos_dir / ficha.sku / "zen.png"
                    if _recortar_foto(page, sku_w, out):
                        resultado.fotos_guardadas += 1

    return resultado


def leer_ficha(producto_id: int) -> list[dict]:
    """Lee los campos de ficha técnica de un producto (para construir descripción)."""
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
