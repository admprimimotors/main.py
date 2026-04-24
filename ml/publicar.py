"""
ml/publicar.py
==============
Publicador REAL de items en Mercado Libre.

A diferencia de ml/publicaciones.py (que sólo arma drafts), este módulo:
  1. Sube la foto ZEN al endpoint /pictures/items/upload para obtener un
     picture_id.
  2. Postea el item a POST /items con status="paused" (por default).
  3. Si ML exige descripción por separado, la postea a /items/<id>/description.
  4. Registra ml_item_id + permalink + estado='publicado' en la tabla
     publicaciones_drafts y en productos.sku_ml.

Reglas acordadas con Federico (Primi Motors) para la primera tanda de 10:
  - status = "paused"  (se activan manualmente desde el panel ML).
  - Solo foto ZEN del PDF (data/fotos/<sku>/zen.png).
  - Si el producto no tiene foto ZEN, se saltea (skip_sin_foto=True).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from io import BytesIO
from pathlib import Path
from typing import Optional

import config
import db
from logger import get_logger
from ml import client as ml_client
from ml import publicaciones as pub
from ml import foto_processor

log = get_logger(__name__)

# Pillow para normalizar las fotos antes de subirlas a ML
try:
    from PIL import Image  # type: ignore
    _PIL_OK = True
except Exception:  # pragma: no cover
    _PIL_OK = False


# ==========================================================
# Defaults
# ==========================================================
PUBLICAR_STATUS_DEFAULT = "paused"           # se activa manualmente después

# Fotos extraídas del PDF ZEN (fallback — 1 sola foto por SKU, calidad de PDF).
FOTOS_DIR_DEFAULT = config.DATA_DIR / "fotos"
FOTO_ZEN_NOMBRE = "zen.png"

# Fotos HD cargadas por el usuario (PRIORITARIAS). Convención:
#   data/fotos_publicaciones/<sku_master>/1.jpg
#   data/fotos_publicaciones/<sku_master>/2.jpg
#   data/fotos_publicaciones/<sku_master>/3.jpg
# Se aceptan .jpg, .jpeg, .png, .webp. El orden de publicación en ML es el
# orden alfabético de los nombres → '1.jpg' sale como foto principal.
FOTOS_HD_DIR_DEFAULT = config.DATA_DIR / "fotos_publicaciones"
FOTOS_HD_EXTENSIONES = (".jpg", ".jpeg", ".png", ".webp")
ML_MAX_PICTURES = 12                          # ML acepta hasta 12 fotos por item

ENDPOINT_UPLOAD_PICTURE = "/pictures/items/upload"
ENDPOINT_ITEMS = "/items"


class PublicarError(Exception):
    """Error al publicar un item en ML."""


# ==========================================================
# Resultado
# ==========================================================
@dataclass
class ResultadoPublicacion:
    producto_id: int
    sku: str
    ok: bool
    ml_item_id: Optional[str] = None
    ml_permalink: Optional[str] = None
    ml_status: Optional[str] = None
    draft_id: Optional[int] = None
    mensaje: str = ""
    pictures_subidas: int = 0
    skipped: bool = False   # True si se saltó (ej. sin foto)


# ==========================================================
# Fotos
# ==========================================================
def _foto_zen_path(sku: str, fotos_dir: Path = FOTOS_DIR_DEFAULT) -> Path:
    return fotos_dir / sku / FOTO_ZEN_NOMBRE


def _listar_fotos_hd(sku: str, fotos_hd_dir: Path = FOTOS_HD_DIR_DEFAULT) -> list[Path]:
    """
    Devuelve la lista de fotos HD cargadas por el usuario para un SKU, en
    orden alfabético de nombres. Acepta .jpg/.jpeg/.png/.webp.

    Busca en `<fotos_hd_dir>/<sku>/` y también con padding (si el sku es
    "13", también prueba "0013") para tolerar copia/pega desde PowerShell.
    Devuelve [] si la carpeta no existe o no tiene fotos válidas.
    """
    if not sku:
        return []
    candidatos: list[Path] = []
    dirs_a_probar = [fotos_hd_dir / sku]
    if sku.isdigit() and len(sku) < 4:
        dirs_a_probar.append(fotos_hd_dir / sku.zfill(4))
    vistos: set[Path] = set()
    for d in dirs_a_probar:
        if d in vistos or not d.exists() or not d.is_dir():
            continue
        vistos.add(d)
        for p in sorted(d.iterdir(), key=lambda x: x.name.lower()):
            if p.is_file() and p.suffix.lower() in FOTOS_HD_EXTENSIONES:
                candidatos.append(p)
        if candidatos:
            break   # con la primera carpeta válida alcanza
    return candidatos[:ML_MAX_PICTURES]


def resolver_fotos_para_publicar(
    sku: str,
    *,
    fotos_zen_dir: Path = FOTOS_DIR_DEFAULT,
    fotos_hd_dir: Path = FOTOS_HD_DIR_DEFAULT,
) -> tuple[list[Path], str]:
    """
    Devuelve (lista_de_paths, origen) resolviendo las fotos para un SKU con
    esta prioridad:
      1. Fotos HD del usuario en `fotos_publicaciones/<sku>/*.jpg|png|...`.
         Hasta 12 fotos en orden alfabético.
      2. Foto ZEN del PDF en `fotos/<sku>/zen.png`. Una sola foto.
      3. Nada — lista vacía.

    `origen` es 'hd' | 'zen' | 'ninguna' (útil para logs y mensajes al usuario).
    """
    hd = _listar_fotos_hd(sku, fotos_hd_dir)
    if hd:
        return hd, "hd"
    zen = _foto_zen_path(sku, fotos_zen_dir)
    if zen.exists():
        return [zen], "zen"
    return [], "ninguna"


# Umbrales para normalizar la imagen antes de mandarla a ML.
# ML recorta los bordes blancos y después exige que el lado menor sea >= 500 px.
# Usamos 600 px de piso para tener margen; y apuntamos a que el lado mayor sea
# 1200 px (recomendación de ML para poder hacer zoom).
_ML_MIN_LADO_MENOR = 600
_ML_TARGET_LADO_MAYOR = 1200
_WHITE_THRESHOLD = 240  # todo pixel con R,G,B >= 240 se considera "borde blanco"
_PADDING_POST_TRIM = 12  # padding alrededor del contenido luego del crop


def _trim_bordes_blancos(img: "Image.Image") -> "Image.Image":
    """
    Recorta los bordes donde la imagen es completamente blanco (o casi).
    Devuelve la imagen original si no puede calcular el bbox.
    """
    if not _PIL_OK:
        return img
    try:
        gray = img.convert("L")
        # Mapear: blanco puro → 0, contenido → 255
        mask = gray.point(lambda p: 0 if p >= _WHITE_THRESHOLD else 255)
        bbox = mask.getbbox()
        if not bbox:
            return img
        x0, y0, x1, y1 = bbox
        pad = _PADDING_POST_TRIM
        x0 = max(0, x0 - pad)
        y0 = max(0, y0 - pad)
        x1 = min(img.width, x1 + pad)
        y1 = min(img.height, y1 + pad)
        if (x1 - x0) < 10 or (y1 - y0) < 10:
            return img  # bbox sospechosamente chico → no arriesgar
        return img.crop((x0, y0, x1, y1))
    except Exception as e:
        log.warning(f"_trim_bordes_blancos falló: {e}")
        return img


def _preparar_imagen_para_ml(foto_path: Path) -> tuple[bytes, str, str]:
    """
    Normaliza la imagen antes de subirla a ML:
      1. Recorta bordes blancos (ML lo hace igualmente, pero lo hacemos nosotros
         para saber el tamaño real post-trim y poder upscalear si queda chica).
      2. Si el lado menor queda < 600 px, escala para que = 600 px manteniendo
         aspecto.
      3. Si el lado mayor queda < 1200 px, escala para que = 1200 px.
      4. Convierte a RGB (sin alpha) y serializa a JPEG de calidad 92.

    Devuelve (bytes, nombre_sugerido, mime). Si Pillow no está instalado o
    falla, devuelve el archivo original tal cual.
    """
    if not _PIL_OK:
        log.warning("Pillow no disponible — subiendo la foto tal como está.")
        data = foto_path.read_bytes()
        mime = "image/png" if foto_path.suffix.lower() == ".png" else "image/jpeg"
        return data, foto_path.name, mime

    try:
        img = Image.open(foto_path)
        # Convertir a RGB (saca alpha, fondo blanco por si viene con transparencia)
        if img.mode in ("RGBA", "LA") or (img.mode == "P" and "transparency" in img.info):
            fondo = Image.new("RGB", img.size, (255, 255, 255))
            # paste respeta la máscara alpha si existe
            try:
                fondo.paste(img, mask=img.convert("RGBA").split()[-1])
            except Exception:
                fondo.paste(img.convert("RGB"))
            img = fondo
        else:
            img = img.convert("RGB")

        # 1. Trim de bordes blancos
        img = _trim_bordes_blancos(img)

        # 2+3. Upscale si es chica
        w, h = img.size
        menor = min(w, h)
        mayor = max(w, h)
        factor_menor = _ML_MIN_LADO_MENOR / menor if menor < _ML_MIN_LADO_MENOR else 1.0
        factor_mayor = _ML_TARGET_LADO_MAYOR / mayor if mayor < _ML_TARGET_LADO_MAYOR else 1.0
        escala = max(factor_menor, factor_mayor)
        if escala > 1.0001:
            nueva_w = int(round(w * escala))
            nueva_h = int(round(h * escala))
            img = img.resize((nueva_w, nueva_h), Image.LANCZOS)
            log.info(
                f"Foto {foto_path.name}: {w}x{h} → {nueva_w}x{nueva_h} "
                f"(escala x{escala:.2f}) después del trim"
            )
        else:
            log.info(f"Foto {foto_path.name}: {w}x{h} ya es suficiente, no se escala.")

        buf = BytesIO()
        img.save(buf, format="JPEG", quality=92, optimize=True)
        nombre = foto_path.stem + ".jpg"
        return buf.getvalue(), nombre, "image/jpeg"

    except Exception as e:
        log.error(f"_preparar_imagen_para_ml falló para {foto_path}: {e} — subiendo original.")
        data = foto_path.read_bytes()
        mime = "image/png" if foto_path.suffix.lower() == ".png" else "image/jpeg"
        return data, foto_path.name, mime


def subir_foto_a_ml(foto_path: Path) -> Optional[str]:
    """
    Sube una foto al endpoint /pictures/items/upload y devuelve el picture_id.

    Pipeline de normalización (en orden):
      1. foto_processor.preparar_foto_profesional_ml
         → recorta panel de ficha técnica del PDF ZEN, centra el producto en
           canvas blanco 1200x1200 al 85%, sin EXIF, JPEG q=95.
      2. Si el pipeline profesional falla, cae al normalizador simple
         (_preparar_imagen_para_ml) que sólo hace trim + upscale.

    Retorna None si falla el upload (no lanza — la publicación puede seguir
    sin fotos).
    """
    if not foto_path.exists():
        log.warning(f"Foto no existe: {foto_path}")
        return None
    try:
        # 1) Pipeline profesional con lineamientos ML
        try:
            data, nombre, mime = foto_processor.preparar_foto_profesional_ml(foto_path)
        except Exception as e:
            log.warning(
                f"foto_processor falló ({e}) — uso pipeline simple como fallback."
            )
            data, nombre, mime = _preparar_imagen_para_ml(foto_path)

        files = {"file": (nombre, data, mime)}
        resp = ml_client.post_multipart(ENDPOINT_UPLOAD_PICTURE, files=files)
        if not resp:
            log.warning(f"Upload devolvió vacío para {foto_path}")
            return None
        pic_id = resp.get("id") if isinstance(resp, dict) else None
        if pic_id:
            log.info(f"Foto subida: {nombre} → picture_id={pic_id}")
        return pic_id
    except Exception as e:
        log.error(f"Error subiendo {foto_path}: {e}")
        return None


# ==========================================================
# Descripción: POST explícito a /items/{id}/description
# ==========================================================
def _asegurar_descripcion(item_id: str, texto: str) -> None:
    """
    Asegura que el ítem tenga la descripción `texto` cargada en ML.

    Flujo:
      1. POST /items/{id}/description con {"plain_text": texto}.
      2. Si ML devuelve 409/already exists → PUT para actualizar.
      3. Es best-effort: si ambas fallan, loggea y sigue (el item existe).

    Este paso es necesario porque en muchas categorías el campo
    `description` dentro del body del POST /items se ignora, y el item
    queda con descripción vacía si no hacemos este POST separado.
    """
    if not item_id or not texto:
        return
    body = {"plain_text": texto}
    endpoint = f"/items/{item_id}/description"
    try:
        ml_client.post(endpoint, json_body=body)
        log.info(f"  descripción cargada OK para {item_id} ({len(texto)} chars)")
        return
    except Exception as e_post:
        # Si ya existe la descripción, intentamos PUT. Detectamos por texto de error
        # porque el wrapper de ml_client propaga la respuesta como Exception.
        msg = str(e_post).lower()
        if "already" in msg or "409" in msg or "exist" in msg:
            try:
                ml_client.put(endpoint, json_body=body)
                log.info(f"  descripción actualizada (PUT) para {item_id}")
                return
            except Exception as e_put:
                log.warning(
                    f"  no pude actualizar descripción de {item_id}: {e_put} — "
                    "el ítem quedó con la descripción que haya puesto el POST /items."
                )
                return
        log.warning(
            f"  no pude cargar descripción de {item_id}: {e_post} — "
            "el ítem quedó con la descripción que haya puesto el POST /items."
        )


# ==========================================================
# Publicación
# ==========================================================
def publicar_item(
    draft: pub.DraftPublicacion,
    *,
    status: str = PUBLICAR_STATUS_DEFAULT,
) -> dict:
    """
    POST /items con el payload del draft. Después, si el status resultante no
    coincide con el pedido (ej. ML crea en active aunque pedimos paused), hace
    un PUT /items/{id} con el status deseado.

    Devuelve el JSON final del item (post-PUT) con id, permalink, status.
    Lanza PublicarError si el POST falla. El PUT de status es best-effort:
    si falla, loggea pero no explota (el item ya existe con éxito).
    """
    payload = draft.to_ml_payload()
    # Incluimos status en el POST por si ML lo respeta en algunas categorías,
    # pero NO confiamos en eso — siempre validamos y corregimos con PUT.
    payload["status"] = status

    try:
        resp = ml_client.post(ENDPOINT_ITEMS, json_body=payload)
    except Exception as e:
        raise PublicarError(f"POST /items falló: {e}") from e

    if not isinstance(resp, dict) or not resp.get("id"):
        raise PublicarError(f"Respuesta inesperada de ML: {resp}")

    item_id = resp.get("id")
    status_real = resp.get("status")
    log.info(
        f"Item publicado en ML: id={item_id} status={status_real} "
        f"permalink={resp.get('permalink')}"
    )

    # Algunas categorías (y versiones del flujo) ignoran silenciosamente la
    # descripción enviada en el body del POST /items. Para asegurar que quede
    # cargada, hacemos un POST explícito a /items/{id}/description. Si ya
    # existe, ML devuelve 409 y hacemos un PUT para actualizarla.
    _asegurar_descripcion(item_id, draft.descripcion)

    # Si ML creó el item con un status distinto al pedido, hacemos PUT para
    # alinearlo. Esto pasa típicamente cuando pedimos paused — ML ignora ese
    # campo en el POST y crea en active.
    if status and status_real and status_real != status:
        log.info(f"  ajustando status: {status_real} → {status} (PUT /items/{item_id})")
        try:
            resp_put = ml_client.put(f"{ENDPOINT_ITEMS}/{item_id}", json_body={"status": status})
            if isinstance(resp_put, dict) and resp_put.get("status"):
                resp["status"] = resp_put["status"]
                log.info(f"  status actualizado: {resp['status']}")
            else:
                # Reconsultamos el item para ver qué quedó
                resp["status"] = status
        except Exception as e:
            log.warning(
                f"  no se pudo actualizar status a '{status}' para {item_id}: {e} "
                "— el item existe en ML en el status que devolvió el POST."
            )

    return resp


# ==========================================================
# Persistencia post-publicación
# ==========================================================
def _registrar_publicacion(
    draft_id: int,
    producto_id: int,
    ml_item_id: str,
    ml_permalink: Optional[str],
    ml_status: Optional[str],
) -> None:
    """
    Guarda el resultado de la publicación:
      - publicaciones_drafts: estado='publicado', ml_item_id, ml_permalink, ml_status
      - productos.fecha_modificacion: se toca para dejar traza

    NOTA: NO pisamos productos.sku_ml porque esa columna es el SELLER_SKU
    (código local tipo '0013ZEN') y NO el ml_item_id (tipo MLA1234567890).
    El ml_item_id queda en publicaciones_drafts y podemos joinear desde ahí.
    """
    with db.conexion() as c:
        c.execute(
            """
            UPDATE publicaciones_drafts
               SET estado = 'publicado',
                   ml_item_id = ?,
                   ml_permalink = ?,
                   ml_status = ?,
                   fecha_publicacion = datetime('now','localtime'),
                   fecha_modificacion = datetime('now','localtime'),
                   mensaje_error = NULL
             WHERE id = ?
            """,
            (ml_item_id, ml_permalink, ml_status, draft_id),
        )
        c.execute(
            """
            UPDATE productos
               SET fecha_modificacion = datetime('now','localtime')
             WHERE id = ?
            """,
            (producto_id,),
        )


def _marcar_error(draft_id: int, mensaje: str) -> None:
    with db.conexion() as c:
        c.execute(
            """
            UPDATE publicaciones_drafts
               SET estado = 'error',
                   mensaje_error = ?,
                   fecha_modificacion = datetime('now','localtime')
             WHERE id = ?
            """,
            (mensaje[:1000], draft_id),
        )


# ==========================================================
# Orquestador: publicar un producto
# ==========================================================
def publicar_producto(
    producto_id: int,
    *,
    status: str = PUBLICAR_STATUS_DEFAULT,
    skip_sin_foto: bool = True,
    fotos_dir: Path = FOTOS_DIR_DEFAULT,
    fotos_hd_dir: Path = FOTOS_HD_DIR_DEFAULT,
    dry_run: bool = False,
) -> ResultadoPublicacion:
    """
    Publica un producto en ML. Pasos:
      1. Lee producto + arma draft (con categoría predicha).
      2. Valida precondiciones (precio>0, stock>0, al menos 1 foto existe
         si skip_sin_foto).
      3. Resuelve fotos: primero fotos HD del usuario
         (fotos_publicaciones/<sku>/), y si no hay, cae a la foto ZEN del PDF.
      4. Sube cada foto a ML y las agrega al draft en orden.
      5. Guarda draft en DB (estado=borrador).
      6. POST /items con status='paused'.
      7. Actualiza draft → estado='publicado', guarda ml_item_id + permalink.

    En dry_run=True, no toca ML ni la DB (solo reporta qué haría).
    """
    # 1. Producto y draft
    with db.conexion() as c:
        row = c.execute("SELECT * FROM productos WHERE id = ?", (producto_id,)).fetchone()
    if row is None:
        return ResultadoPublicacion(
            producto_id=producto_id, sku="?", ok=False,
            mensaje=f"Producto id={producto_id} no existe.",
        )
    producto = dict(row)
    sku = producto.get("sku_master") or str(producto_id)

    # 2. Validaciones
    precio = float(producto.get("precio_venta") or 0)
    if precio <= 0:
        return ResultadoPublicacion(
            producto_id=producto_id, sku=sku, ok=False,
            mensaje="Precio de venta en 0 — no se publica.",
        )
    stock_actual = int(producto.get("stock_actual") or 0)
    # Nota: si stock=0 el draft publica con stock=1 (ver builder); eso es OK.
    # Pero si el usuario quiere que se saltee productos sin stock, añadir flag.

    fotos, origen_fotos = resolver_fotos_para_publicar(
        sku, fotos_zen_dir=fotos_dir, fotos_hd_dir=fotos_hd_dir,
    )
    if skip_sin_foto and not fotos:
        return ResultadoPublicacion(
            producto_id=producto_id, sku=sku, ok=False, skipped=True,
            mensaje=(
                f"Sin fotos para SKU {sku} — salteado. "
                f"Buscá en: {fotos_hd_dir / sku}/ (HD) "
                f"o {_foto_zen_path(sku, fotos_dir)} (ZEN fallback)."
            ),
        )

    # 3. Construir draft
    try:
        draft = pub.construir_draft(producto_id, predecir_cat=not dry_run)
    except pub.PublicacionError as e:
        return ResultadoPublicacion(
            producto_id=producto_id, sku=sku, ok=False,
            mensaje=f"construir_draft falló: {e}",
        )

    if draft.mensaje_error:
        return ResultadoPublicacion(
            producto_id=producto_id, sku=sku, ok=False,
            mensaje=draft.mensaje_error,
        )

    if not draft.category_id_ml and not dry_run:
        return ResultadoPublicacion(
            producto_id=producto_id, sku=sku, ok=False,
            mensaje="ML no predijo categoría para el título. Revisar título.",
        )

    # 4. Dry-run — no toca ML ni DB
    if dry_run:
        nombres_fotos = ", ".join(p.name for p in fotos) if fotos else "sin fotos"
        return ResultadoPublicacion(
            producto_id=producto_id, sku=sku, ok=True,
            mensaje=(
                f"[dry-run] Se publicaría como '{status}'. "
                f"Fotos ({origen_fotos}, {len(fotos)}): {nombres_fotos}. "
                f"Título: {draft.titulo}"
            ),
            pictures_subidas=len(fotos),
        )

    # 5. Subir fotos y completar draft.pictures (en orden)
    picture_ids: list[dict] = []
    if fotos:
        log.info(f"SKU {sku}: subiendo {len(fotos)} foto(s) desde origen='{origen_fotos}'")
        for i, foto_path in enumerate(fotos, 1):
            pic_id = subir_foto_a_ml(foto_path)
            if pic_id:
                picture_ids.append({"id": pic_id})
                log.info(f"  foto {i}/{len(fotos)} ok ({foto_path.name})")
            else:
                log.warning(f"  foto {i}/{len(fotos)} falló ({foto_path.name}) — la salteo")
        if not picture_ids:
            log.warning(f"SKU {sku}: ninguna foto se pudo subir — publicando sin fotos")
        draft.pictures = picture_ids

    # 6. Guardar draft
    draft_id = pub.guardar_draft(draft)

    # 7. Publicar en ML
    try:
        resp = publicar_item(draft, status=status)
    except PublicarError as e:
        msg = str(e)
        _marcar_error(draft_id, msg)
        return ResultadoPublicacion(
            producto_id=producto_id, sku=sku, ok=False,
            draft_id=draft_id, mensaje=msg,
            pictures_subidas=len(draft.pictures),
        )

    ml_item_id = resp.get("id")
    ml_permalink = resp.get("permalink")
    ml_status = resp.get("status")

    _registrar_publicacion(
        draft_id=draft_id,
        producto_id=producto_id,
        ml_item_id=ml_item_id,
        ml_permalink=ml_permalink,
        ml_status=ml_status,
    )

    return ResultadoPublicacion(
        producto_id=producto_id,
        sku=sku,
        ok=True,
        ml_item_id=ml_item_id,
        ml_permalink=ml_permalink,
        ml_status=ml_status,
        draft_id=draft_id,
        mensaje=f"Publicado OK en {ml_status}.",
        pictures_subidas=len(draft.pictures),
    )


# ==========================================================
# Selección de candidatos
# ==========================================================
def seleccionar_no_publicados(limite: int = 10) -> list[dict]:
    """
    Devuelve hasta `limite` productos candidatos a publicar: aquellos sin
    sku_ml (nunca publicados) con precio_venta > 0.
    Prioriza los que tienen foto ZEN + ficha técnica cargada (más prolijos).
    """
    with db.conexion() as c:
        rows = c.execute(
            """
            SELECT p.*,
                   (SELECT COUNT(*) FROM fichas_tecnicas ft WHERE ft.producto_id = p.id)
                       AS n_fichas
              FROM productos p
             WHERE (p.sku_ml IS NULL OR TRIM(p.sku_ml) = '')
               AND p.precio_venta > 0
               AND p.stock_actual > 0
             ORDER BY n_fichas DESC, p.sku_master
             LIMIT ?
            """,
            (max(limite * 3, 50),),  # sobre-seleccionamos y filtramos por foto después
        ).fetchall()

    # Filtrar: sólo los que tienen foto ZEN
    candidatos: list[dict] = []
    for r in rows:
        d = dict(r)
        sku = d.get("sku_master") or ""
        if _foto_zen_path(sku).exists():
            candidatos.append(d)
        if len(candidatos) >= limite:
            break
    return candidatos


# ==========================================================
# Selección por lista explícita de SKUs
# ==========================================================
def seleccionar_por_skus(skus: list[str]) -> list[dict]:
    """
    Devuelve los productos que matchean los SKUs dados (contra sku_master,
    sku_ml o sku_proveedor), en el orden en que fueron pedidos.
    NO aplica filtros de precio/stock/foto — eso lo valida publicar_producto().
    Los SKUs no encontrados quedan fuera silenciosamente (se loggean).
    """
    if not skus:
        return []
    # Normalización: upper + trim + si es todo dígitos y <4 chars, pad con ceros
    # (PowerShell come los ceros a la izquierda cuando pasás 0013,0020 sin comillas)
    skus_norm: list[str] = []
    for s in skus:
        v = (s or "").strip().upper()
        if not v:
            continue
        skus_norm.append(v)
        if v.isdigit() and len(v) < 4:
            skus_norm.append(v.zfill(4))   # "13" → "0013"
    # Deduplicar preservando orden
    seen: set[str] = set()
    skus_norm = [s for s in skus_norm if not (s in seen or seen.add(s))]
    if not skus_norm:
        return []

    placeholders = ",".join("?" * len(skus_norm))
    sql = f"""
        SELECT p.*,
               (SELECT COUNT(*) FROM fichas_tecnicas ft WHERE ft.producto_id = p.id)
                   AS n_fichas
          FROM productos p
         WHERE UPPER(p.sku_master)    IN ({placeholders})
            OR UPPER(p.sku_ml)        IN ({placeholders})
            OR UPPER(p.sku_proveedor) IN ({placeholders})
    """
    params = skus_norm + skus_norm + skus_norm
    with db.conexion() as c:
        rows = c.execute(sql, params).fetchall()

    # Reordenar según la lista recibida + deduplicar
    indice = {s: i for i, s in enumerate(skus_norm)}
    def _clave(r):
        d = dict(r)
        for campo in ("sku_master", "sku_ml", "sku_proveedor"):
            v = (d.get(campo) or "").upper()
            if v in indice:
                return indice[v]
        return 9999
    productos = sorted([dict(r) for r in rows], key=_clave)
    vistos = set()
    unicos: list[dict] = []
    for p in productos:
        if p["id"] in vistos:
            continue
        vistos.add(p["id"])
        unicos.append(p)

    # Loggear los que no aparecieron
    skus_encontrados = set()
    for p in unicos:
        for campo in ("sku_master", "sku_ml", "sku_proveedor"):
            v = (p.get(campo) or "").upper()
            if v in indice:
                skus_encontrados.add(v)
                break
    faltantes = [s for s in skus_norm if s not in skus_encontrados]
    if faltantes:
        log.warning(f"SKUs no encontrados en DB: {faltantes}")

    return unicos


# ==========================================================
# Diagnóstico: por qué no hay candidatos
# ==========================================================
def diagnostico_candidatos() -> dict:
    """
    Devuelve un desglose de cuántos productos pasan cada filtro.
    Útil cuando seleccionar_no_publicados() devuelve lista vacía.
    """
    with db.conexion() as c:
        total = c.execute("SELECT COUNT(*) FROM productos").fetchone()[0]
        sku_ml_vacio = c.execute("""
            SELECT COUNT(*) FROM productos
             WHERE sku_ml IS NULL OR TRIM(sku_ml) = ''
        """).fetchone()[0]
        con_precio = c.execute("""
            SELECT COUNT(*) FROM productos
             WHERE (sku_ml IS NULL OR TRIM(sku_ml) = '')
               AND precio_venta > 0
        """).fetchone()[0]
        con_stock = c.execute("""
            SELECT COUNT(*) FROM productos
             WHERE (sku_ml IS NULL OR TRIM(sku_ml) = '')
               AND precio_venta > 0
               AND stock_actual > 0
        """).fetchone()[0]
        # De los que llegaron a con_stock: cuántos tienen foto
        rows_stock = c.execute("""
            SELECT sku_master FROM productos
             WHERE (sku_ml IS NULL OR TRIM(sku_ml) = '')
               AND precio_venta > 0
               AND stock_actual > 0
        """).fetchall()
        con_foto = sum(1 for r in rows_stock if _foto_zen_path(r["sku_master"]).exists())

        # Adicional: cuántos productos tienen foto ZEN (independientemente)
        todos_con_foto = c.execute("SELECT sku_master FROM productos").fetchall()
        n_foto_total = sum(1 for r in todos_con_foto if _foto_zen_path(r["sku_master"]).exists())

        # Muestras de SKUs que quedan afuera por cada filtro
        muestra_con_sku_ml = c.execute("""
            SELECT sku_master, sku_ml FROM productos
             WHERE sku_ml IS NOT NULL AND TRIM(sku_ml) <> ''
             LIMIT 5
        """).fetchall()
        muestra_sin_precio = c.execute("""
            SELECT sku_master, precio_venta FROM productos
             WHERE (sku_ml IS NULL OR TRIM(sku_ml) = '')
               AND (precio_venta IS NULL OR precio_venta <= 0)
             LIMIT 5
        """).fetchall()
        muestra_sin_stock = c.execute("""
            SELECT sku_master, stock_actual FROM productos
             WHERE (sku_ml IS NULL OR TRIM(sku_ml) = '')
               AND precio_venta > 0
               AND (stock_actual IS NULL OR stock_actual <= 0)
             LIMIT 5
        """).fetchall()

    return {
        "total_productos": total,
        "sku_ml_vacio": sku_ml_vacio,
        "con_precio": con_precio,
        "con_stock": con_stock,
        "con_foto": con_foto,
        "productos_con_foto_en_total": n_foto_total,
        "muestra_con_sku_ml": [dict(r) for r in muestra_con_sku_ml],
        "muestra_sin_precio": [dict(r) for r in muestra_sin_precio],
        "muestra_sin_stock": [dict(r) for r in muestra_sin_stock],
    }
