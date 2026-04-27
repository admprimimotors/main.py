"""
app/storage.py
==============
Cliente de storage para Cloudflare R2 (API S3-compatible).

Configuración via env vars en Render:
  - R2_ACCOUNT_ID            (32 chars hex, del dashboard de Cloudflare)
  - R2_ACCESS_KEY_ID         (del API Token creado en R2)
  - R2_SECRET_ACCESS_KEY     (del API Token, se muestra una sola vez)
  - R2_BUCKET                (nombre del bucket, ej: "primi-motors-fotos")
  - R2_PUBLIC_URL            (base pública del bucket, sin trailing slash,
                              ej: "https://pub-abc123.r2.dev")

Funciones:
  - is_configured()          → True si todas las env vars están seteadas
  - get_public_url(key)      → arma la URL pública para un object key
  - optimize_image(bytes)    → resize + JPEG quality 85 (preserva orientación EXIF)
  - upload_photo(...)        → optimiza + sube a R2 + devuelve metadatos
  - delete_photo(key)        → borra de R2 (best-effort)

Por qué boto3: R2 es S3-compatible y boto3 es el cliente oficial de AWS.
Funciona con cualquier endpoint S3 cambiando `endpoint_url`.
"""

from __future__ import annotations

import io
import os
import re
from datetime import datetime, timezone
from typing import Optional

from PIL import Image, ImageOps


# =============================================================
# Cliente R2 (lazy: solo se inicializa cuando hace falta)
# =============================================================

def _get_client():
    """Crea un cliente boto3 apuntando a R2. Devuelve None si falta config."""
    import boto3
    from botocore.config import Config

    account_id = os.environ.get("R2_ACCOUNT_ID")
    access_key = os.environ.get("R2_ACCESS_KEY_ID")
    secret = os.environ.get("R2_SECRET_ACCESS_KEY")

    if not (account_id and access_key and secret):
        return None

    return boto3.client(
        "s3",
        endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret,
        region_name="auto",
        config=Config(signature_version="s3v4"),
    )


def is_configured() -> bool:
    """¿Están todas las env vars seteadas?"""
    return all(
        os.environ.get(k)
        for k in (
            "R2_ACCOUNT_ID",
            "R2_ACCESS_KEY_ID",
            "R2_SECRET_ACCESS_KEY",
            "R2_BUCKET",
            "R2_PUBLIC_URL",
        )
    )


def get_public_url(storage_key: str) -> str:
    """Construye la URL pública de un object."""
    base = (os.environ.get("R2_PUBLIC_URL") or "").rstrip("/")
    if not base:
        return ""
    return f"{base}/{storage_key}"


# =============================================================
# Procesamiento de imágenes
# =============================================================

MAX_DIMENSION = 1200    # px en el lado mayor — alcanza para ML y mantiene peso bajo
JPEG_QUALITY = 85       # buen balance calidad/peso


def optimize_image(image_bytes: bytes) -> tuple[bytes, int, int]:
    """
    Recibe bytes de imagen, devuelve (jpeg_bytes, width, height) optimizada.
      - Aplica orientación EXIF (fotos del celu suelen venir rotadas)
      - Resize si el lado mayor > MAX_DIMENSION (preserva aspect ratio)
      - Convierte a RGB sobre fondo blanco (descarta alpha de PNGs)
      - Re-encoda como JPEG quality 85 con optimize=True
    """
    img = Image.open(io.BytesIO(image_bytes))

    # Auto-rotar según EXIF (importante para fotos de celular)
    try:
        img = ImageOps.exif_transpose(img)
    except Exception:
        pass

    # Redimensionar (in-place; preserva ratio)
    img.thumbnail((MAX_DIMENSION, MAX_DIMENSION), Image.LANCZOS)

    # Convertir a RGB sobre fondo blanco si tiene alpha
    if img.mode in ("RGBA", "LA"):
        bg = Image.new("RGB", img.size, (255, 255, 255))
        bg.paste(img, mask=img.split()[-1])
        img = bg
    elif img.mode == "P":
        img = img.convert("RGBA")
        bg = Image.new("RGB", img.size, (255, 255, 255))
        bg.paste(img, mask=img.split()[-1])
        img = bg
    elif img.mode != "RGB":
        img = img.convert("RGB")

    out = io.BytesIO()
    img.save(out, format="JPEG", quality=JPEG_QUALITY, optimize=True)
    return out.getvalue(), img.width, img.height


# =============================================================
# Helpers
# =============================================================

def _slugify(s: str) -> str:
    """Genera un slug filesystem/URL-safe."""
    s = re.sub(r"[^a-zA-Z0-9]+", "-", (s or "").lower()).strip("-")
    return s[:60] or "x"


# =============================================================
# Upload / delete
# =============================================================

def upload_photo(image_bytes: bytes, sku: str, filename: str = "") -> dict:
    """
    Sube una foto a R2 (optimizada). Devuelve dict con:
      - storage_key, url, bytes_size, width_px, height_px

    Lanza RuntimeError si R2 no está configurado.
    """
    client = _get_client()
    bucket = os.environ.get("R2_BUCKET")
    if client is None or not bucket:
        raise RuntimeError("R2 no está configurado en las env vars")

    optimized_bytes, width, height = optimize_image(image_bytes)

    # Key: productos/{sku-slug}/{timestamp-microsec}-{filename-slug}.jpg
    # El timestamp con microsec evita colisiones en uploads simultáneos.
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S-%f")
    name_part = filename.rsplit(".", 1)[0] if filename else "foto"
    storage_key = f"productos/{_slugify(sku)}/{timestamp}-{_slugify(name_part)}.jpg"

    client.put_object(
        Bucket=bucket,
        Key=storage_key,
        Body=optimized_bytes,
        ContentType="image/jpeg",
        # Cache agresivo (1 año, immutable) — R2 sirve esto en el header.
        CacheControl="public, max-age=31536000, immutable",
    )

    return {
        "storage_key": storage_key,
        "url": get_public_url(storage_key),
        "bytes_size": len(optimized_bytes),
        "width_px": width,
        "height_px": height,
    }


def delete_photo(storage_key: str) -> bool:
    """Borra una foto de R2. Devuelve True si fue OK (o si R2 no está configurado)."""
    client = _get_client()
    bucket = os.environ.get("R2_BUCKET")
    if client is None or not bucket:
        return False
    try:
        client.delete_object(Bucket=bucket, Key=storage_key)
        return True
    except Exception:
        return False
