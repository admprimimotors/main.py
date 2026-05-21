"""
app/ml_orders.py
================
Sincronización de órdenes de venta de Mercado Libre.

Flujo:
  1. Periódicamente (al cargar el home dashboard o vía botón manual),
     llamamos a `sync_orders(db)`.
  2. Detectamos desde qué fecha empezar a traer (ej: max date_created en
     ml_orders, o 7 días atrás si nunca corrió).
  3. Paginamos /orders/search hasta vaciar.
  4. Para cada (order, item) que llega:
     - Si es nuevo y está "paid"/"confirmed" → creamos MLOrder + decrementamos
       stock del producto correspondiente vía ml_item_id.
     - Si ya existía y cambió a "cancelled"/"refunded" → revertimos stock
       (re-incrementamos lo que habíamos decrementado).
     - Si ya existía y sigue igual → no hacemos nada.

Rate limit: ML acepta ~5-10 req/seg sostenido. Como pagina de a 50, el costo
es bajo (catálogo grande con muchas ventas = pocas requests).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional

from sqlalchemy import select, func as sql_func
from sqlalchemy.orm import Session

from . import ml_client
from .models import MLOrder, Producto


# Estados de orden que descuentan stock
STATUS_APLICA_STOCK = {"paid", "confirmed"}
# Estados que NO descuentan (cancelan o revierten una venta)
STATUS_REVIERTE_STOCK = {"cancelled", "invalid", "refunded"}


def _parse_ml_date(s: Optional[str]) -> Optional[datetime]:
    """ML devuelve fechas ISO con offset, ej '2026-05-19T10:35:42.000-00:00'."""
    if not s:
        return None
    try:
        # Python 3.10 strptime no maneja `.000-00:00` directo; usamos fromisoformat
        # con un cleanup del milli.
        s2 = str(s)
        # fromisoformat acepta "+HH:MM" pero no siempre los milis con 3 dígitos.
        # Limpiamos los millis (los descartamos, granularidad suficiente).
        if "." in s2 and ("+" in s2 or "-" in s2[-6:]):
            head, rest = s2.split(".", 1)
            # rest = "000-00:00" o "123+00:00"
            tz_idx = max(rest.find("+"), rest.rfind("-"))
            if tz_idx > 0:
                s2 = head + rest[tz_idx:]
            else:
                s2 = head
        return datetime.fromisoformat(s2)
    except Exception:
        return None


def _earliest_date_for_initial_sync() -> datetime:
    """Si nunca corrimos sync, arrancamos desde 7 días atrás (no traemos
    histórico viejo automático para evitar procesar 10k órdenes al primer run)."""
    return datetime.now(timezone.utc) - timedelta(days=7)


def _resolve_producto(
    db: Session,
    ml_item_id: str,
    ml_variation_id: Optional[str],
) -> Optional[Producto]:
    """Match producto local por ml_item_id + variation. Si hay variation,
    matchea exacto; si no, toma el producto sin variation_id."""
    q = select(Producto).where(Producto.ml_item_id == ml_item_id)
    if ml_variation_id:
        q = q.where(Producto.ml_variation_id == ml_variation_id)
    else:
        # Cuando la orden NO trae variation, buscamos por item sin variation
        # (publicación simple) — pero si no hay match, fallback al que sea.
        prod = db.execute(
            q.where(Producto.ml_variation_id.is_(None))
        ).scalar_one_or_none()
        if prod is not None:
            return prod
        # Fallback: cualquier producto con ese ml_item_id
        return db.execute(
            select(Producto).where(Producto.ml_item_id == ml_item_id).limit(1)
        ).scalar_one_or_none()
    return db.execute(q).scalar_one_or_none()


def _process_order_item(
    db: Session,
    order: dict,
    item_entry: dict,
) -> dict:
    """
    Procesa una línea de orden (1 item dentro de una order).
    Crea o actualiza MLOrder. Aplica/revierte stock si cambió el status.
    Devuelve dict con resumen: {sku, action, delta_stock, status}.
    """
    order_id = str(order.get("id") or "")
    if not order_id:
        return {"action": "skip", "reason": "no order_id"}
    item = item_entry.get("item") or {}
    ml_item_id = str(item.get("id") or "")
    if not ml_item_id:
        return {"action": "skip", "reason": "no item_id"}

    ml_variation_id = item.get("variation_id")
    if ml_variation_id is not None:
        ml_variation_id = str(ml_variation_id)
    cantidad = int(item_entry.get("quantity") or 0)
    if cantidad <= 0:
        return {"action": "skip", "reason": "quantity 0"}

    status = str(order.get("status") or "unknown").lower()
    precio_unit = item_entry.get("unit_price")
    if precio_unit is not None:
        try:
            precio_unit = Decimal(str(precio_unit))
        except Exception:
            precio_unit = None
    total_amount = order.get("total_amount")
    if total_amount is not None:
        try:
            total_amount = Decimal(str(total_amount))
        except Exception:
            total_amount = None
    moneda = (order.get("currency_id") or "ARS")[:3]

    buyer = (order.get("buyer") or {}).get("nickname")
    date_created = _parse_ml_date(order.get("date_created")) or datetime.now(timezone.utc)
    date_closed = _parse_ml_date(order.get("date_closed"))
    last_status_at = _parse_ml_date(order.get("last_updated")) or date_created

    # ¿Ya existe en local?
    existing = db.execute(
        select(MLOrder).where(
            MLOrder.ml_order_id == order_id,
            MLOrder.ml_item_id == ml_item_id,
        )
    ).scalar_one_or_none()

    # Resolver producto local (puede ser None si la publicación no existe en nuestro DB)
    prod = _resolve_producto(db, ml_item_id, ml_variation_id)

    desired_stock_applied = cantidad if status in STATUS_APLICA_STOCK else 0
    delta_stock = 0

    if existing is None:
        # Orden nueva
        existing = MLOrder(
            ml_order_id=order_id,
            ml_item_id=ml_item_id,
            ml_variation_id=ml_variation_id,
            producto_id=prod.id if prod else None,
            sku_snapshot=prod.sku if prod else None,
            titulo_snapshot=item.get("title"),
            cantidad=cantidad,
            precio_unitario=precio_unit,
            total_amount=total_amount,
            moneda=moneda,
            status=status,
            buyer_nickname=buyer,
            date_created=date_created,
            date_closed=date_closed,
            last_status_at=last_status_at,
            stock_applied=0,
        )
        db.add(existing)
        db.flush()
        action = "created"
    else:
        # Orden ya conocida: actualizamos meta + detectamos cambio de estado
        existing.status = status
        existing.last_status_at = last_status_at
        existing.date_closed = date_closed
        # No cambiamos cantidad ni precio — ML no debería modificarlos post-paid
        action = "updated"

    # Aplicar diff de stock = desired - current
    delta_stock = desired_stock_applied - existing.stock_applied
    if delta_stock != 0 and prod is not None:
        # Decremento (delta=+cantidad → stock_applied sube, stock_actual baja)
        # Incremento de reversa (delta=-cantidad → revertimos)
        nuevo_stock = (prod.stock_actual or 0) - delta_stock
        if nuevo_stock < 0:
            nuevo_stock = 0  # no permitimos stock negativo en local
        prod.stock_actual = nuevo_stock
        prod.stock_updated_at = datetime.now(timezone.utc)
        existing.stock_applied = desired_stock_applied
    elif delta_stock != 0 and prod is None:
        # Sin producto local, ni siquiera bajamos stock — solo registramos
        # el MLOrder. El user puede ver el "huérfano" en la lista.
        existing.stock_applied = desired_stock_applied

    return {
        "action": action,
        "sku": existing.sku_snapshot,
        "ml_item_id": ml_item_id,
        "qty": cantidad,
        "status": status,
        "delta_stock": delta_stock,
        "had_producto": prod is not None,
    }


def get_last_synced_date(db: Session) -> Optional[datetime]:
    """Fecha de creación más reciente que tenemos cacheada de una orden ML."""
    row = db.execute(
        select(sql_func.max(MLOrder.date_created))
    ).scalar_one_or_none()
    return row


def sync_orders(
    db: Session,
    *,
    since: Optional[datetime] = None,
    max_pages: int = 20,
    page_size: int = 50,
) -> dict:
    """
    Sincroniza órdenes de ML desde `since` (default: max(date_created) cacheado,
    o 7 días atrás si está vacío).

    Devuelve summary:
      {
        "fetched": int,         # cuántas órdenes/items procesamos
        "created": int,         # nuevos MLOrder rows
        "updated": int,         # MLOrder rows actualizados
        "stock_applied": int,   # total de unidades descontadas
        "stock_reverted": int,  # total revertido por cancelaciones
        "errors": [...],
        "from_date": str,       # fecha desde la cual buscamos
      }
    """
    if not ml_client.is_configured():
        return {"fetched": 0, "errors": ["ML no configurado"]}

    if since is None:
        since = get_last_synced_date(db)
    if since is None:
        since = _earliest_date_for_initial_sync()
    else:
        # Pequeño solapamiento de 1 hora para captar actualizaciones de status
        since = since - timedelta(hours=1)

    # Format ISO 8601 con timezone Z
    since_str = since.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000-00:00")

    summary = {
        "fetched": 0,
        "created": 0,
        "updated": 0,
        "stock_applied": 0,
        "stock_reverted": 0,
        "errors": [],
        "from_date": since_str,
    }

    seller_id = ml_client.get_user_id(db)
    offset = 0
    for _page in range(max_pages):
        try:
            resp = ml_client.search_orders(
                db,
                seller_id=seller_id,
                date_from=since_str,
                offset=offset,
                limit=page_size,
                sort="date_asc",  # de viejas a nuevas
            )
        except Exception as e:
            summary["errors"].append(f"page offset={offset}: {type(e).__name__}: {e}")
            break

        results = resp.get("results") or []
        if not results:
            break

        for order in results:
            for item_entry in (order.get("order_items") or []):
                try:
                    r = _process_order_item(db, order, item_entry)
                    summary["fetched"] += 1
                    if r.get("action") == "created":
                        summary["created"] += 1
                    elif r.get("action") == "updated":
                        summary["updated"] += 1
                    delta = r.get("delta_stock") or 0
                    if delta > 0:
                        summary["stock_applied"] += delta
                    elif delta < 0:
                        summary["stock_reverted"] += -delta
                except Exception as e:
                    summary["errors"].append(
                        f"order {order.get('id')}: {type(e).__name__}: {e}"
                    )

        # Commit incremental por página para no perder progreso si crashea
        try:
            db.commit()
        except Exception as e:
            db.rollback()
            summary["errors"].append(f"commit: {type(e).__name__}: {e}")
            break

        # Avanzar paginación
        paging = resp.get("paging") or {}
        total = int(paging.get("total") or 0)
        offset += len(results)
        if offset >= total:
            break

    return summary


# =============================================================
# Stats para el dashboard
# =============================================================

def get_sales_stats(
    db: Session,
    *,
    days: int = 30,
) -> dict:
    """
    Stats agregados de ventas en los últimos N días (ML + remitos juntos).
    Devuelve:
      {
        "ml": {
          "n_ventas": int,        # cantidad de órdenes (líneas)
          "stock_movido": int,    # unidades vendidas
          "monto_total": Decimal,
        },
        "remitos": {
          "n_ventas": int,
          "stock_movido": int,
          "monto_total": Decimal,
        },
        "top_skus": [{sku, titulo, cantidad, fuentes}, ...],  # top 5
        "days": days,
      }
    """
    from .models import RemitoItem, Remito
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    # ML stats (solo órdenes pagas, no canceladas)
    ml_q = (
        select(
            sql_func.count(MLOrder.id),
            sql_func.coalesce(sql_func.sum(MLOrder.cantidad), 0),
            sql_func.coalesce(sql_func.sum(MLOrder.total_amount), 0),
        )
        .where(MLOrder.date_created >= cutoff)
        .where(MLOrder.status.in_(["paid", "confirmed"]))
    )
    ml_n, ml_stock, ml_monto = db.execute(ml_q).one()

    # Remitos stats (solo emitidos, no anulados)
    rem_q = (
        select(
            sql_func.count(RemitoItem.id),
            sql_func.coalesce(sql_func.sum(RemitoItem.cantidad), 0),
            sql_func.coalesce(sql_func.sum(RemitoItem.subtotal), 0),
        )
        .join(Remito, Remito.id == RemitoItem.remito_id)
        .where(Remito.fecha >= cutoff.date())
        .where(Remito.estado == "emitido")
    )
    rem_n, rem_stock, rem_monto = db.execute(rem_q).one()

    # Top SKUs (combinado): ML + remitos
    # Para ML: agrupado por sku_snapshot
    top_ml_q = (
        select(
            MLOrder.sku_snapshot,
            MLOrder.titulo_snapshot,
            sql_func.sum(MLOrder.cantidad).label("qty"),
        )
        .where(MLOrder.date_created >= cutoff)
        .where(MLOrder.status.in_(["paid", "confirmed"]))
        .where(MLOrder.sku_snapshot.is_not(None))
        .group_by(MLOrder.sku_snapshot, MLOrder.titulo_snapshot)
    )
    top_rem_q = (
        select(
            RemitoItem.sku,
            RemitoItem.descripcion,
            sql_func.sum(RemitoItem.cantidad).label("qty"),
        )
        .join(Remito, Remito.id == RemitoItem.remito_id)
        .where(Remito.fecha >= cutoff.date())
        .where(Remito.estado == "emitido")
        .where(RemitoItem.sku.is_not(None))
        .group_by(RemitoItem.sku, RemitoItem.descripcion)
    )

    # Aglomerar en dict {sku → {qty, titulo, fuentes}}
    aglom: dict = {}
    for sku, titulo, qty in db.execute(top_ml_q).all():
        if not sku:
            continue
        if sku not in aglom:
            aglom[sku] = {"sku": sku, "titulo": titulo or "", "qty": 0, "fuentes": set()}
        aglom[sku]["qty"] += int(qty or 0)
        aglom[sku]["fuentes"].add("ML")
    for sku, desc, qty in db.execute(top_rem_q).all():
        if not sku:
            continue
        if sku not in aglom:
            aglom[sku] = {"sku": sku, "titulo": desc or "", "qty": 0, "fuentes": set()}
        aglom[sku]["qty"] += int(qty or 0)
        aglom[sku]["fuentes"].add("Remito")

    top_skus = sorted(aglom.values(), key=lambda x: -x["qty"])[:5]
    for t in top_skus:
        t["fuentes"] = sorted(t["fuentes"])  # set → list para JSON

    return {
        "ml": {
            "n_ventas": int(ml_n or 0),
            "stock_movido": int(ml_stock or 0),
            "monto_total": Decimal(str(ml_monto or 0)),
        },
        "remitos": {
            "n_ventas": int(rem_n or 0),
            "stock_movido": int(rem_stock or 0),
            "monto_total": Decimal(str(rem_monto or 0)),
        },
        "top_skus": top_skus,
        "days": days,
    }


def list_movimientos(
    db: Session,
    *,
    days: int = 30,
    limit: int = 100,
    fuente: str = "",   # "ml" / "remito" / "" (todo)
    sku: str = "",
    page: int = 1,
) -> tuple[list[dict], int]:
    """
    Lista cronológica unificada de movimientos de stock por ventas.
    Combina MLOrder + RemitoItem y devuelve dict con campos comunes.
    """
    from .models import RemitoItem, Remito
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    items: list[dict] = []
    # ML
    if fuente in ("", "ml"):
        q = (
            select(MLOrder)
            .where(MLOrder.date_created >= cutoff)
            .order_by(MLOrder.date_created.desc())
        )
        if sku:
            q = q.where(MLOrder.sku_snapshot == sku)
        for o in db.execute(q).scalars().all():
            items.append({
                "fuente": "ML",
                "fecha": o.date_created,
                "sku": o.sku_snapshot,
                "titulo": o.titulo_snapshot,
                "cantidad": o.cantidad,
                "stock_aplicado": o.stock_applied,
                "monto": float(o.total_amount) if o.total_amount else None,
                "status": o.status,
                "ml_order_id": o.ml_order_id,
                "buyer": o.buyer_nickname,
            })
    # Remitos
    if fuente in ("", "remito"):
        q = (
            select(RemitoItem, Remito)
            .join(Remito, Remito.id == RemitoItem.remito_id)
            .where(Remito.fecha >= cutoff.date())
            .order_by(Remito.fecha.desc(), RemitoItem.id.desc())
        )
        if sku:
            q = q.where(RemitoItem.sku == sku)
        for it, rem in db.execute(q).all():
            items.append({
                "fuente": "Remito",
                "fecha": datetime.combine(rem.fecha, datetime.min.time(), tzinfo=timezone.utc) if rem.fecha else None,
                "sku": it.sku,
                "titulo": it.descripcion,
                "cantidad": it.cantidad,
                "stock_aplicado": it.cantidad if rem.estado == "emitido" else 0,
                "monto": float(it.subtotal) if it.subtotal else None,
                "status": rem.estado,
                "ml_order_id": None,
                "buyer": rem.cliente_razon_social,
            })

    # Orden cronológico desc (las más nuevas primero)
    items.sort(key=lambda x: x["fecha"] or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    total = len(items)
    start = (page - 1) * limit
    return items[start:start + limit], total
