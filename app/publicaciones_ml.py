"""
app/publicaciones_ml.py
=======================
Servicio para `ProductoPublicacionML` — la tabla nueva (F1 del refactor de
"1 SKU = N publicaciones"). Cada Producto puede tener N filas acá, cada una
representando una publicación distinta en ML.

Diferencia con `app/publicaciones.py`:
  - `publicaciones.py` (legacy) opera sobre los campos `ml_*` que viven en
    Producto. Trata la publicación primaria solamente. Sigue funcionando para
    los flujos viejos.
  - `publicaciones_ml.py` (este) opera sobre la nueva tabla. Las nuevas
    publicaciones de un mismo SKU se insertan acá. La pantalla /publicaciones
    refactorizada lista desde acá (1 fila por publicación, no por producto).

En F1 el stock es compartido: cualquier sync push el stock del producto a
TODAS sus publicaciones. F2 va a usar `ml_stock_asignado` por publicación.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

from sqlalchemy import func as sql_func, or_, select
from sqlalchemy.orm import Session, selectinload

from .models import FotoProducto, Producto, ProductoPublicacionML


PAGE_SIZE = 100


# =============================================================
# Lookups
# =============================================================

def get_by_id(db: Session, pub_id: int) -> Optional[ProductoPublicacionML]:
    return db.execute(
        select(ProductoPublicacionML).where(ProductoPublicacionML.id == pub_id)
    ).scalar_one_or_none()


def get_by_ml_item_id(
    db: Session,
    ml_item_id: str,
) -> Optional[ProductoPublicacionML]:
    """
    Busca una publicación por su `ml_item_id`. Es la forma canónica de mapear
    una venta ML → producto local (usado en ml_orders.sync).
    """
    if not ml_item_id:
        return None
    return db.execute(
        select(ProductoPublicacionML).where(
            ProductoPublicacionML.ml_item_id == ml_item_id
        )
    ).scalar_one_or_none()


def list_by_producto(
    db: Session,
    producto_id: int,
) -> list[ProductoPublicacionML]:
    """Todas las publicaciones de un producto, ordenadas por fecha de creación."""
    return list(db.execute(
        select(ProductoPublicacionML)
        .where(ProductoPublicacionML.producto_id == producto_id)
        .order_by(ProductoPublicacionML.created_at.asc())
    ).scalars().all())


def list_by_sku(db: Session, sku: str) -> list[ProductoPublicacionML]:
    """Atajo: todas las publicaciones del SKU dado (case-sensitive)."""
    if not sku:
        return []
    return list(db.execute(
        select(ProductoPublicacionML)
        .join(Producto, Producto.id == ProductoPublicacionML.producto_id)
        .where(Producto.sku == sku)
        .order_by(ProductoPublicacionML.created_at.asc())
    ).scalars().all())


# =============================================================
# Mutaciones
# =============================================================

def create_publicacion(
    db: Session,
    *,
    producto_id: int,
    ml_item_id: str,
    ml_variation_id: Optional[str] = None,
    ml_permalink: Optional[str] = None,
    ml_status: Optional[str] = None,
    ml_category_id: Optional[str] = None,
    ml_listing_type: Optional[str] = None,
    ml_shipping_mode: Optional[str] = None,
    ml_catalog_listing: bool = False,
    ml_titulo: Optional[str] = None,
    ml_precio: Optional[Decimal] = None,
    ml_stock_asignado: Optional[int] = None,
    ml_stock_snapshot: Optional[int] = None,
    ml_raw_attributes: Optional[list] = None,
    commit: bool = True,
) -> ProductoPublicacionML:
    """
    Inserta una row nueva en producto_publicaciones_ml. Llamada por
    ml_publisher después del POST /items exitoso. `ml_item_id` ya es el
    devuelto por ML (no NULL).
    """
    pub = ProductoPublicacionML(
        producto_id=producto_id,
        ml_item_id=ml_item_id,
        ml_variation_id=ml_variation_id,
        ml_permalink=ml_permalink,
        ml_status=ml_status,
        ml_category_id=ml_category_id,
        ml_listing_type=ml_listing_type,
        ml_shipping_mode=ml_shipping_mode,
        ml_catalog_listing=ml_catalog_listing,
        ml_titulo=ml_titulo,
        ml_precio=ml_precio,
        ml_stock_asignado=ml_stock_asignado,
        ml_stock_snapshot=ml_stock_snapshot,
        ml_raw_attributes=ml_raw_attributes,
        ml_last_synced_at=datetime.now(timezone.utc),
    )
    db.add(pub)
    if commit:
        db.commit()
        db.refresh(pub)
    else:
        db.flush()  # para que pub.id quede populado
    return pub


def update_from_ml_response(
    db: Session,
    pub: ProductoPublicacionML,
    ml_response: dict,
    *,
    commit: bool = True,
) -> None:
    """
    Actualiza una publicación con la respuesta cruda de GET /items/{id} de ML.
    Refresca: ml_status, ml_permalink, ml_stock_snapshot, ml_precio,
    ml_raw_attributes, ml_category_id, ml_shipping_mode, ml_catalog_listing,
    ml_listing_type, ml_titulo, ml_last_synced_at.
    """
    if not isinstance(ml_response, dict):
        return
    pub.ml_status = ml_response.get("status") or pub.ml_status
    pub.ml_permalink = ml_response.get("permalink") or pub.ml_permalink
    if "available_quantity" in ml_response:
        try:
            pub.ml_stock_snapshot = int(ml_response.get("available_quantity") or 0)
        except (TypeError, ValueError):
            pass
    if "price" in ml_response and ml_response["price"] is not None:
        try:
            pub.ml_precio = Decimal(str(ml_response["price"]))
        except (TypeError, ValueError):
            pass
    if ml_response.get("category_id"):
        pub.ml_category_id = ml_response["category_id"]
    if ml_response.get("listing_type_id"):
        pub.ml_listing_type = ml_response["listing_type_id"]
    if ml_response.get("shipping"):
        shipping = ml_response["shipping"]
        if isinstance(shipping, dict):
            pub.ml_shipping_mode = shipping.get("mode") or pub.ml_shipping_mode
    if ml_response.get("catalog_listing") is not None:
        pub.ml_catalog_listing = bool(ml_response["catalog_listing"])
    if ml_response.get("title"):
        pub.ml_titulo = ml_response["title"]
    if ml_response.get("attributes"):
        pub.ml_raw_attributes = ml_response["attributes"]
    pub.ml_last_synced_at = datetime.now(timezone.utc)
    if commit:
        db.commit()


def update_status(
    db: Session,
    pub: ProductoPublicacionML,
    new_status: str,
    *,
    commit: bool = True,
) -> None:
    pub.ml_status = new_status
    pub.ml_last_synced_at = datetime.now(timezone.utc)
    if commit:
        db.commit()


# =============================================================
# Listado para pantalla /publicaciones (1 fila por publicación)
# =============================================================

def list_publicaciones(
    db: Session,
    *,
    search: str = "",
    status: str = "",
    categoria: str = "",
    marca: str = "",
    drift: str = "",
    page: int = 1,
) -> tuple[list[dict], int, dict]:
    """
    Lista publicaciones ML — 1 fila por row en producto_publicaciones_ml,
    JOIN con Producto para traer SKU/título/etc.

    Mismos filtros que la versión legacy: search por SKU/título/marca/cat/item_id,
    filtro de status, categoría, marca, y "drift" (stock o precio entre DB y ML
    no coinciden).

    Devuelve (rows, total, stats).
    """
    base_q = (
        select(ProductoPublicacionML, Producto)
        .join(Producto, Producto.id == ProductoPublicacionML.producto_id)
        .options(selectinload(Producto.fotos))
    )
    count_q = (
        select(sql_func.count(ProductoPublicacionML.id))
        .join(Producto, Producto.id == ProductoPublicacionML.producto_id)
    )

    extra = []
    if search and search.strip():
        like = f"%{search.strip()}%"
        extra.append(or_(
            Producto.sku.ilike(like),
            Producto.titulo.ilike(like),
            Producto.marca.ilike(like),
            Producto.categoria.ilike(like),
            ProductoPublicacionML.ml_item_id.ilike(like),
            ProductoPublicacionML.ml_titulo.ilike(like),
        ))
    if status:
        extra.append(ProductoPublicacionML.ml_status == status)
    if categoria:
        extra.append(Producto.categoria == categoria)
    if marca:
        extra.append(Producto.marca == marca)

    # Drift: stock del producto != stock_snapshot, OR precio_final del
    # producto != ml_precio de la publicación.
    if drift in ("si", "no"):
        stock_drift_cond = (
            ProductoPublicacionML.ml_stock_snapshot.is_not(None)
            & Producto.stock_actual.is_not(None)
            & (ProductoPublicacionML.ml_stock_snapshot != Producto.stock_actual)
        )
        precio_drift_cond = (
            ProductoPublicacionML.ml_precio.is_not(None)
            & Producto.precio_final.is_not(None)
            & (ProductoPublicacionML.ml_precio != Producto.precio_final)
        )
        drift_cond = stock_drift_cond | precio_drift_cond
        if drift == "si":
            extra.append(drift_cond)
        else:
            extra.append(~drift_cond)

    if extra:
        base_q = base_q.where(*extra)
        count_q = count_q.where(*extra)

    total = db.execute(count_q).scalar() or 0

    # Orden: más viejas en última sync arriba (las que más necesitan refresh)
    base_q = (
        base_q
        .order_by(ProductoPublicacionML.ml_last_synced_at.asc().nulls_first())
        .limit(PAGE_SIZE)
        .offset((page - 1) * PAGE_SIZE)
    )

    rows: list[dict] = []
    for pub, prod in db.execute(base_q).all():
        stock_drift = (
            pub.ml_stock_snapshot is not None
            and prod.stock_actual is not None
            and pub.ml_stock_snapshot != prod.stock_actual
        )
        precio_drift = (
            pub.ml_precio is not None
            and prod.precio_final is not None
            and pub.ml_precio != prod.precio_final
        )
        rows.append({
            "pub_id": pub.id,
            "id": prod.id,
            "sku": prod.sku,
            "titulo": pub.ml_titulo or prod.titulo,
            "categoria": prod.categoria,
            "marca": prod.marca,
            "stock_actual": prod.stock_actual,
            "ml_stock": pub.ml_stock_snapshot,
            "ml_stock_asignado": pub.ml_stock_asignado,
            "stock_drift": stock_drift,
            "precio_final": prod.precio_final,
            "ml_precio": pub.ml_precio,
            "precio_drift": precio_drift,
            "ml_item_id": pub.ml_item_id,
            "ml_variation_id": pub.ml_variation_id,
            "ml_status": pub.ml_status,
            "ml_permalink": pub.ml_permalink,
            "ml_listing_type": pub.ml_listing_type,
            "ml_shipping_mode": pub.ml_shipping_mode,
            "ml_catalog_listing": pub.ml_catalog_listing,
            "ml_last_synced_at": pub.ml_last_synced_at,
            "foto_url": prod.fotos[0].url if prod.fotos else None,
        })

    # Stats globales por status (sin paginación, sin filtro de status/drift —
    # los tiles muestran absolutos para "X activas").
    stats_q = (
        select(ProductoPublicacionML.ml_status, sql_func.count(ProductoPublicacionML.id))
        .join(Producto, Producto.id == ProductoPublicacionML.producto_id)
        .group_by(ProductoPublicacionML.ml_status)
    )
    stats_filters = []
    if search and search.strip():
        like = f"%{search.strip()}%"
        stats_filters.append(or_(
            Producto.sku.ilike(like),
            Producto.titulo.ilike(like),
            Producto.marca.ilike(like),
            Producto.categoria.ilike(like),
            ProductoPublicacionML.ml_item_id.ilike(like),
            ProductoPublicacionML.ml_titulo.ilike(like),
        ))
    if categoria:
        stats_filters.append(Producto.categoria == categoria)
    if marca:
        stats_filters.append(Producto.marca == marca)
    if stats_filters:
        stats_q = stats_q.where(*stats_filters)
    raw = {row[0] or "unknown": row[1] for row in db.execute(stats_q).all()}
    stats = {
        "total": sum(raw.values()),
        "active": raw.get("active", 0),
        "paused": raw.get("paused", 0),
        "closed": raw.get("closed", 0),
        "under_review": raw.get("under_review", 0),
        "inactive": raw.get("inactive", 0),
    }

    # Drift global
    drift_count_q = (
        select(sql_func.count(ProductoPublicacionML.id))
        .join(Producto, Producto.id == ProductoPublicacionML.producto_id)
        .where(
            (ProductoPublicacionML.ml_stock_snapshot.is_not(None)
             & Producto.stock_actual.is_not(None)
             & (ProductoPublicacionML.ml_stock_snapshot != Producto.stock_actual))
            |
            (ProductoPublicacionML.ml_precio.is_not(None)
             & Producto.precio_final.is_not(None)
             & (ProductoPublicacionML.ml_precio != Producto.precio_final))
        )
    )
    if stats_filters:
        drift_count_q = drift_count_q.where(*stats_filters)
    stats["drift"] = db.execute(drift_count_q).scalar() or 0

    return rows, total, stats


def count_publicaciones_for_producto(db: Session, producto_id: int) -> int:
    """Helper rápido — cuántas publicaciones tiene un producto en ML."""
    return db.execute(
        select(sql_func.count(ProductoPublicacionML.id))
        .where(ProductoPublicacionML.producto_id == producto_id)
    ).scalar() or 0
