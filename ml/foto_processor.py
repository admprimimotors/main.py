"""
ml/foto_processor.py
====================
Procesador de fotos para cumplir los lineamientos de imagen de Mercado Libre.

Reglas que aplicamos (basadas en la guía oficial de ML para fotos de producto):
  1. Fondo blanco puro (255, 255, 255). Sin texturas, grises ni degradados.
  2. Sin texto, códigos, marcas de agua, logos ni callouts sobre la imagen.
  3. Sin bordes, marcos, flechas, círculos ni collages.
  4. El producto centrado, ocupando ~85% del área (el resto es padding blanco).
  5. Proporción 1:1 (cuadrada).
  6. Tamaño mínimo 500×500 px — nosotros entregamos 1200×1200 para permitir zoom.
  7. JPEG alta calidad, sin EXIF (ML lo rechaza en algunos casos).

El PDF de ZEN suele presentar cada producto como una imagen compuesta:
    ┌──────────────┬──────────┐
    │              │ T    9   │
    │  [producto]  │ G    29.6│
    │              │ L    73.5│
    │              │ ...      │
    └──────────────┴──────────┘
Es decir, producto a la izquierda y tabla de ficha técnica a la derecha
(o arriba/abajo, dependiendo del PDF). Este módulo DETECTA la franja blanca
que separa ambas zonas y descarta la tabla antes de pasarla a ML.

Pipeline:
    preparar_foto_profesional_ml(path) →
        open → flatten fondo blanco → recortar tabla lateral/superior/inferior →
        trim bordes blancos → cuadrar 1:1 con padding blanco → upscale 1200 →
        JPEG q=95 sin EXIF → (bytes, nombre, mime)
"""

from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Optional

from logger import get_logger

log = get_logger(__name__)

try:
    from PIL import Image  # type: ignore
    _PIL_OK = True
except Exception:  # pragma: no cover
    _PIL_OK = False


# ==========================================================
# Parámetros
# ==========================================================
CANVAS_SIDE = 1200            # tamaño final del canvas cuadrado
PRODUCTO_OCUPA = 0.85         # el producto llena 85% del canvas (resto = padding blanco)
WHITE_PIXEL_THRESHOLD = 245   # un pixel cuyo L >= 245 cuenta como "blanco"
COL_PURE_WHITE_FRAC = 0.985   # columna/fila "completamente blanca" si >=98.5% sus pixeles son >=245
MIN_BANDA_BLANCA = 6          # la franja blanca separadora mide como mínimo N pixeles
INICIO_BUSQUEDA_FRAC = 0.30   # no buscamos separador antes del 30% del ancho
FIN_BUSQUEDA_FRAC = 0.95      # ni después del 95% del ancho (puede haber bordes blancos finales)
PAD_POST_TRIM = 8             # padding alrededor del contenido tras trim final
JPEG_QUALITY = 95


# ==========================================================
# Helpers de análisis por eje
# ==========================================================
def _fraccion_blanca_por_columna(img: "Image.Image") -> list[float]:
    """Para cada columna X, devuelve la fracción de pixeles cuyo L >= WHITE_PIXEL_THRESHOLD."""
    gray = img.convert("L")
    w, h = gray.size
    # Mapear a 0/1: 1 si pixel >= threshold
    bin_img = gray.point(lambda p: 1 if p >= WHITE_PIXEL_THRESHOLD else 0)
    # Suma por columna — Pillow no tiene np, iteramos columnas
    data = bin_img.tobytes()
    fracciones: list[float] = []
    for x in range(w):
        total = 0
        # getdata + stride: leemos píxeles de la columna x
        for y in range(h):
            total += data[y * w + x]
        fracciones.append(total / h)
    return fracciones


def _fraccion_blanca_por_fila(img: "Image.Image") -> list[float]:
    """Igual que _fraccion_blanca_por_columna pero por fila Y."""
    gray = img.convert("L")
    w, h = gray.size
    bin_img = gray.point(lambda p: 1 if p >= WHITE_PIXEL_THRESHOLD else 0)
    data = bin_img.tobytes()
    fracciones: list[float] = []
    for y in range(h):
        total = sum(data[y * w + x] for x in range(w))
        fracciones.append(total / w)
    return fracciones


def _mayor_banda(fracciones: list[float], inicio: int, fin: int) -> Optional[tuple[int, int]]:
    """
    Encuentra la franja consecutiva más larga donde fracciones[i] >= COL_PURE_WHITE_FRAC,
    dentro del rango [inicio, fin). Devuelve (start, end) o None si no hay ninguna
    de al menos MIN_BANDA_BLANCA de ancho.
    """
    mejor = None
    mejor_len = 0
    cur_ini = None
    cur_len = 0
    for i in range(inicio, fin):
        if fracciones[i] >= COL_PURE_WHITE_FRAC:
            if cur_ini is None:
                cur_ini = i
            cur_len += 1
        else:
            if cur_len >= MIN_BANDA_BLANCA and cur_len > mejor_len:
                mejor_len = cur_len
                mejor = (cur_ini, cur_ini + cur_len)
            cur_ini = None
            cur_len = 0
    if cur_len >= MIN_BANDA_BLANCA and cur_len > mejor_len:
        mejor = (cur_ini, cur_ini + cur_len)
    return mejor


# ==========================================================
# Recorte heurístico: eliminar panel de ficha técnica
# ==========================================================
def _recortar_panel_lateral(img: "Image.Image") -> "Image.Image":
    """
    Busca una franja vertical blanca que divida la imagen en dos zonas (producto
    vs. ficha/tabla) y se queda con el lado que tenga contenido más denso.
    Si no hay tal franja, devuelve la imagen sin cambios.

    Estrategia:
      1. Calcular fracción de blanco por columna.
      2. Buscar la franja blanca más ancha entre 30% y 95% del ancho.
      3. Si existe, decidir qué lado conservar: el que tenga MÁS píxeles de
         contenido (1 - fracción blanca promedio). Típicamente el producto a
         la izquierda es más denso porque es una pieza sólida, vs. la tabla
         que es mayormente espacios en blanco entre texto.
      4. Recortar.
    """
    w, h = img.size
    if w < 100 or h < 100:
        return img
    try:
        frac = _fraccion_blanca_por_columna(img)
    except Exception as e:
        log.warning(f"_recortar_panel_lateral: no pude analizar columnas: {e}")
        return img

    inicio = int(w * INICIO_BUSQUEDA_FRAC)
    fin = int(w * FIN_BUSQUEDA_FRAC)
    banda = _mayor_banda(frac, inicio, fin)
    if not banda:
        return img

    b_ini, b_fin = banda
    # Densidad del contenido a cada lado (menor fracción blanca = más contenido)
    lado_izq_frac = sum(frac[:b_ini]) / max(b_ini, 1)
    cols_der = max(w - b_fin, 1)
    lado_der_frac = sum(frac[b_fin:]) / cols_der

    # Si un lado está casi 100% blanco, el otro tiene el producto.
    # Conservamos el lado MÁS OSCURO (con menos blanco).
    if lado_izq_frac <= lado_der_frac:
        nueva = img.crop((0, 0, b_ini, h))
        log.info(f"Foto: panel lateral detectado en x∈[{b_ini},{b_fin}] — se conserva la IZQ ({b_ini}x{h}).")
    else:
        nueva = img.crop((b_fin, 0, w, h))
        log.info(f"Foto: panel lateral detectado en x∈[{b_ini},{b_fin}] — se conserva la DER ({w - b_fin}x{h}).")
    return nueva


def _recortar_panel_superior_inferior(img: "Image.Image") -> "Image.Image":
    """Igual que el lateral pero buscando una franja horizontal (tabla arriba o abajo)."""
    w, h = img.size
    if w < 100 or h < 100:
        return img
    try:
        frac = _fraccion_blanca_por_fila(img)
    except Exception as e:
        log.warning(f"_recortar_panel_sup_inf: no pude analizar filas: {e}")
        return img

    inicio = int(h * INICIO_BUSQUEDA_FRAC)
    fin = int(h * FIN_BUSQUEDA_FRAC)
    banda = _mayor_banda(frac, inicio, fin)
    if not banda:
        return img

    b_ini, b_fin = banda
    lado_sup_frac = sum(frac[:b_ini]) / max(b_ini, 1)
    lado_inf_frac = sum(frac[b_fin:]) / max(h - b_fin, 1)
    if lado_sup_frac <= lado_inf_frac:
        nueva = img.crop((0, 0, w, b_ini))
        log.info(f"Foto: panel horizontal detectado en y∈[{b_ini},{b_fin}] — se conserva ARRIBA ({w}x{b_ini}).")
    else:
        nueva = img.crop((0, b_fin, w, h))
        log.info(f"Foto: panel horizontal detectado en y∈[{b_ini},{b_fin}] — se conserva ABAJO ({w}x{h - b_fin}).")
    return nueva


# ==========================================================
# Trim duro de bordes blancos
# ==========================================================
def _trim_bordes(img: "Image.Image") -> "Image.Image":
    """Recorta todo lo que sea blanco puro alrededor del contenido."""
    try:
        gray = img.convert("L")
        mask = gray.point(lambda p: 0 if p >= WHITE_PIXEL_THRESHOLD else 255)
        bbox = mask.getbbox()
        if not bbox:
            return img
        x0, y0, x1, y1 = bbox
        pad = PAD_POST_TRIM
        x0 = max(0, x0 - pad)
        y0 = max(0, y0 - pad)
        x1 = min(img.width, x1 + pad)
        y1 = min(img.height, y1 + pad)
        if (x1 - x0) < 20 or (y1 - y0) < 20:
            return img
        return img.crop((x0, y0, x1, y1))
    except Exception as e:
        log.warning(f"_trim_bordes falló: {e}")
        return img


# ==========================================================
# Canvas cuadrado blanco con producto centrado al 85%
# ==========================================================
def _cuadrar_en_canvas_blanco(img: "Image.Image", lado: int = CANVAS_SIDE,
                              ocupacion: float = PRODUCTO_OCUPA) -> "Image.Image":
    """
    Compone la imagen sobre un canvas cuadrado blanco de `lado`x`lado`,
    escalando el contenido para que su lado mayor = lado * ocupacion.
    """
    contenido_max = int(lado * ocupacion)
    w, h = img.size
    mayor = max(w, h)
    escala = contenido_max / mayor
    nueva_w = max(1, int(round(w * escala)))
    nueva_h = max(1, int(round(h * escala)))
    contenido = img.resize((nueva_w, nueva_h), Image.LANCZOS)

    canvas = Image.new("RGB", (lado, lado), (255, 255, 255))
    off_x = (lado - nueva_w) // 2
    off_y = (lado - nueva_h) // 2
    canvas.paste(contenido, (off_x, off_y))
    return canvas


# ==========================================================
# Pipeline principal
# ==========================================================
def preparar_foto_profesional_ml(foto_path: Path) -> tuple[bytes, str, str]:
    """
    Pipeline completo para adaptar una foto cruda del PDF ZEN a los lineamientos
    de Mercado Libre.

    Devuelve (bytes_jpeg, nombre_sugerido, "image/jpeg"). Si Pillow no está
    instalado o el pipeline falla, devuelve el archivo original tal cual
    (fallback conservador).
    """
    if not _PIL_OK:
        log.warning("Pillow no disponible — subiendo foto sin procesar.")
        data = foto_path.read_bytes()
        mime = "image/png" if foto_path.suffix.lower() == ".png" else "image/jpeg"
        return data, foto_path.name, mime

    try:
        img = Image.open(foto_path)

        # 1) Flatten sobre fondo blanco (saca alpha/transparencia)
        if img.mode in ("RGBA", "LA") or (img.mode == "P" and "transparency" in img.info):
            fondo = Image.new("RGB", img.size, (255, 255, 255))
            try:
                fondo.paste(img, mask=img.convert("RGBA").split()[-1])
            except Exception:
                fondo.paste(img.convert("RGB"))
            img = fondo
        else:
            img = img.convert("RGB")

        tam_original = img.size

        # 2) Eliminar panel de ficha técnica (vertical: típico ZEN)
        img = _recortar_panel_lateral(img)
        # 3) Por si alguna venía con tabla horizontal
        img = _recortar_panel_superior_inferior(img)

        # 4) Trim bordes blancos
        img = _trim_bordes(img)
        tam_post_trim = img.size

        # 5) Canvas cuadrado con producto al 85%, fondo blanco puro
        img = _cuadrar_en_canvas_blanco(img, CANVAS_SIDE, PRODUCTO_OCUPA)

        # 6) Serializar JPEG sin EXIF
        buf = BytesIO()
        img.save(buf, format="JPEG", quality=JPEG_QUALITY, optimize=True, progressive=True)
        nombre = foto_path.stem + "_ml.jpg"
        log.info(
            f"Foto ML lista: {foto_path.name} "
            f"orig={tam_original[0]}x{tam_original[1]} "
            f"→ post-recorte={tam_post_trim[0]}x{tam_post_trim[1]} "
            f"→ final={CANVAS_SIDE}x{CANVAS_SIDE} "
            f"({len(buf.getvalue())} bytes)"
        )
        return buf.getvalue(), nombre, "image/jpeg"

    except Exception as e:
        log.error(f"preparar_foto_profesional_ml falló para {foto_path}: {e} — mando original.")
        data = foto_path.read_bytes()
        mime = "image/png" if foto_path.suffix.lower() == ".png" else "image/jpeg"
        return data, foto_path.name, mime


# ==========================================================
# Debug helper: guardar la foto procesada a disco (para revisar)
# ==========================================================
def guardar_preview(foto_path: Path, destino: Optional[Path] = None) -> Path:
    """
    Procesa la foto y la guarda en disco junto a la original (o en `destino`)
    con sufijo _ml.jpg. Útil para inspeccionar el resultado antes de subirlo.
    """
    data, nombre, _mime = preparar_foto_profesional_ml(foto_path)
    out = destino if destino else foto_path.parent / nombre
    out.write_bytes(data)
    return out
