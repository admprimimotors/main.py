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
from .models import MLOrder, Producto, ProductoPublicacionML


# Estados de orden que descuentan stock
STATUS_APLICA_STOCK = {"paid", "confirmed"}
# Estados que NO descuentan (cancelan o revierten una venta)
STATUS_REVIERTE_STOCK = {"cancelled", "invalid", "refunded"}


def _refresh_ml_snapshot(
    db: Session,
    prod: Producto,
    ml_item_id: str,
    *,
    fallback_delta: int = 0,
) -> dict:
    """
    Refresca el snapshot ML del producto (ml_stock, ml_status, ml_precio,
    ml_last_synced_at) llamando a /items/{id}.

    Mantiene en sync el DB local con lo que ML ve realmente — sin esperar al
    "Sync desde ML" manual del usuario.

    También actualiza la fila correspondiente en producto_publicaciones_ml.

    Si la API de ML falla (timeout, rate limit, etc.), aplica un fallback
    aritmético: ml_stock -= fallback_delta. Esto mantiene la consistencia
    matemática aunque no la perfección absoluta.

    Devuelve dict con resumen para logging:
      {
        "ok": bool,
        "fonte": "api" | "fallback" | "skip",
        "ml_stock_anterior": int|None,
        "ml_stock_nuevo": int|None,
        "ml_status": str|None,
        "error": str|None,
      }
    """
    info = {
        "ok": False,
        "fonte": "skip",
        "ml_stock_anterior": prod.ml_stock,
        "ml_stock_nuevo": None,
        "ml_status": None,
        "error": None,
    }
    if not ml_item_id:
        return info

    try:
        item = ml_client.get_item(db, ml_item_id)
    except Exception as e:
        # Fallback aritmético: mantener consistencia local.
        if fallback_delta and prod.ml_stock is not None:
            nuevo = (prod.ml_stock or 0) - fallback_delta
            if nuevo < 0:
                nuevo = 0
            prod.ml_stock = nuevo
            info["ml_stock_nuevo"] = nuevo
            info["fonte"] = "fallback"
            info["ok"] = True
        info["error"] = f"{type(e).__name__}: {e}"
        return info

    # Snapshot al producto.
    new_status = item.get("status")
    new_aq = item.get("available_quantity")
    new_price = item.get("price")

    if new_status is not None:
        prod.ml_status = str(new_status)
        info["ml_status"] = str(new_status)
    if new_aq is not None:
        try:
            prod.ml_stock = int(new_aq)
            info["ml_stock_nuevo"] = int(new_aq)
        except (ValueError, TypeError):
            pass
    if new_price is not None:
        try:
            prod.ml_precio = Decimal(str(new_price))
        except Exception:
            pass
    prod.ml_last_synced_at = datetime.now(timezone.utc)

    # Snapshot también en producto_publicaciones_ml — esa tabla es la fuente
    # de verdad para el detalle de /publicaciones.
    try:
        pub = db.execute(
            select(ProductoPublicacionML).where(
                ProductoPublicacionML.ml_item_id == ml_item_id
            )
        ).scalar_one_or_none()
        if pub is not None:
            if new_status is not None:
                pub.ml_status = str(new_status)
            if new_aq is not None:
                try:
                    pub.ml_stock_snapshot = int(new_aq)
                except (ValueError, TypeError):
                    pass
            if new_price is not None:
                try:
                    pub.ml_precio = Decimal(str(new_price))
                except Exception:
                    pass
            pub.ml_last_synced_at = datetime.now(timezone.utc)
    except Exception:
        # No crashea el sync por esto — el snapshot principal ya quedó.
        pass

    info["ok"] = True
    info["fonte"] = "api"
    return info


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
    """
    Match producto local por ml_item_id + variation.

    Estrategia post-refactor (F1, 1 SKU = N publicaciones):
      1. Buscar la publicación en `producto_publicaciones_ml` por ml_item_id
         (con variation si la orden la trae). De ahí ir al producto.
      2. Fallback legacy: si la publicación no está en la tabla nueva
         (caso raro, no se migró), caer al lookup viejo por Producto.ml_item_id.
    """
    # Path nuevo: la fuente de verdad es producto_publicaciones_ml
    q = select(ProductoPublicacionML).where(
        ProductoPublicacionML.ml_item_id == ml_item_id
    )
    if ml_variation_id:
        pub = db.execute(
            q.where(ProductoPublicacionML.ml_variation_id == ml_variation_id)
        ).scalar_one_or_none()
    else:
        # Orden sin variation: preferimos la fila sin variation_id; si no
        # hay, tomamos cualquier publicación con ese ml_item_id.
        pub = db.execute(
            q.where(ProductoPublicacionML.ml_variation_id.is_(None))
        ).scalar_one_or_none()
        if pub is None:
            pub = db.execute(q).scalars().first()
    if pub is not None:
        return db.execute(
            select(Producto).where(Producto.id == pub.producto_id)
        ).scalar_one_or_none()

    # Fallback legacy (Producto.ml_item_id) — pre-migración o casos huérfanos
    legacy_q = select(Producto).where(Producto.ml_item_id == ml_item_id)
    if ml_variation_id:
        legacy_q = legacy_q.where(Producto.ml_variation_id == ml_variation_id)
        return db.execute(legacy_q).scalar_one_or_none()
    prod = db.execute(
        legacy_q.where(Producto.ml_variation_id.is_(None))
    ).scalar_one_or_none()
    if prod is not None:
        return prod
    return db.execute(
        select(Producto).where(Producto.ml_item_id == ml_item_id).limit(1)
    ).scalar_one_or_none()


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

    # ---- Fees REALES de la venta (para margen real por venta) ----
    # sale_fee viene en el item (comisión + costo de cuotas, todo junto).
    sale_fee = item_entry.get("sale_fee")
    try:
        sale_fee = Decimal(str(sale_fee)) if sale_fee is not None else None
    except Exception:
        sale_fee = None
    # Costo real de envío del vendedor: 1 llamada extra a ML, solo si la orden
    # es nueva o todavía no tiene el dato (backfill). Nunca rompe el sync.
    shipping_cost = None
    if existing is None or existing.ml_shipping_cost is None:
        _sid = (order.get("shipping") or {}).get("id")
        if _sid:
            try:
                from . import ml_client as _mlc
                _costs = _mlc.get_shipment_costs(db, str(_sid))
                _senders = _costs.get("senders") or []
                if _senders and _senders[0].get("cost") is not None:
                    shipping_cost = Decimal(str(_senders[0]["cost"]))
            except Exception:
                shipping_cost = None

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
            ml_sale_fee=sale_fee,
            ml_shipping_cost=shipping_cost,
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
        # Backfill de fees reales si la orden vieja no los tenía.
        if existing.ml_sale_fee is None and sale_fee is not None:
            existing.ml_sale_fee = sale_fee
        if existing.ml_shipping_cost is None and shipping_cost is not None:
            existing.ml_shipping_cost = shipping_cost
        # No cambiamos cantidad ni precio — ML no debería modificarlos post-paid
        action = "updated"

    # Aplicar diff de stock = desired - current
    delta_stock = desired_stock_applied - existing.stock_applied
    refresh_summary: Optional[dict] = None
    if delta_stock != 0 and prod is not None:
        # Decremento (delta=+cantidad → stock_applied sube, stock_actual baja)
        # Incremento de reversa (delta=-cantidad → revertimos)
        nuevo_stock = (prod.stock_actual or 0) - delta_stock
        if nuevo_stock < 0:
            nuevo_stock = 0  # no permitimos stock negativo en local
        prod.stock_actual = nuevo_stock
        prod.stock_updated_at = datetime.now(timezone.utc)
        existing.stock_applied = desired_stock_applied

        # ANTI-DRIFT: refrescamos el snapshot ML (ml_stock, ml_status, ml_precio)
        # automáticamente después de procesar la venta. Esto evita el caso
        # "DB:0 / ML:1" que aparecía hasta que el usuario corría sync manual.
        # Si la API falla, hacemos fallback aritmético sobre ml_stock.
        try:
            refresh_summary = _refresh_ml_snapshot(
                db, prod, ml_item_id, fallback_delta=delta_stock
            )
        except Exception as e:
            # Nunca rompemos el sync de orders por un fallo de refresh.
            refresh_summary = {"ok": False, "error": f"{type(e).__name__}: {e}"}
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
        "ml_refresh": refresh_summary,
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
        "ml_refresh_ok": 0,         # snapshots ML refrescados desde la API
        "ml_refresh_fallback": 0,   # caídas a fallback aritmético
        "ml_refresh_fail": 0,       # fallos sin fallback útil
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
                    # Contadores de refresh ML
                    rf = r.get("ml_refresh") or {}
                    if rf:
                        fonte = rf.get("fonte")
                        if fonte == "api":
                            summary["ml_refresh_ok"] += 1
                        elif fonte == "fallback":
                            summary["ml_refresh_fallback"] += 1
                        elif rf.get("error"):
                            summary["ml_refresh_fail"] += 1
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


def ventas_con_margen(db: Session, *, limit: int = 200) -> list[dict]:
    """
    Ventas con MARGEN REAL, usando los fees capturados de cada orden ML:
      neto   = total_amount - ml_sale_fee - ml_shipping_cost
      margen = neto - (precio_costo * cantidad)

    Solo órdenes pagadas/confirmadas. Las sincronizadas antes de esta feature
    salen con tiene_fees=False hasta el próximo sync (que las completa).
    """
    from .models import Producto as _Producto
    q = (
        select(MLOrder)
        .where(MLOrder.status.in_(("paid", "confirmed")))
        .order_by(MLOrder.date_created.desc())
        .limit(limit)
    )
    out: list[dict] = []
    for o in db.execute(q).scalars().all():
        total = Decimal(str(o.total_amount or 0))
        fee = Decimal(str(o.ml_sale_fee or 0))
        ship = Decimal(str(o.ml_shipping_cost or 0))
        neto = total - fee - ship
        costo = None
        if o.producto_id:
            prod = db.get(_Producto, o.producto_id)
            if prod is not None and prod.precio_costo is not None:
                costo = Decimal(str(prod.precio_costo)) * Decimal(int(o.cantidad or 1))
        margen = (neto - costo) if costo is not None else None
        margen_pct = None
        if margen is not None and neto and neto != 0:
            margen_pct = float(margen / neto * 100)
        out.append({
            "fecha": o.date_created,
            "titulo": o.titulo_snapshot,
            "sku": o.sku_snapshot,
            "cantidad": o.cantidad,
            "total": total,
            "sale_fee": fee,
            "envio": ship,
            "neto": neto,
            "costo": costo,
            "margen": margen,
            "margen_pct": margen_pct,
            "tiene_fees": o.ml_sale_fee is not None,
            "ml_order_id": o.ml_order_id,
        })
    return out


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
        from sqlalchemy.orm import selectinload as _selectinload
        from .models import Cliente as _Cliente  # noqa: F401  (forzar registro del mapper)
        q = (
            select(RemitoItem, Remito)
            .join(Remito, Remito.id == RemitoItem.remito_id)
            .options(_selectinload(Remito.cliente))
            .where(Remito.fecha >= cutoff.date())
            .order_by(Remito.fecha.desc(), RemitoItem.id.desc())
        )
        if sku:
            q = q.where(RemitoItem.sku == sku)
        for it, rem in db.execute(q).all():
            # rem.fecha puede ser date o datetime — normalizamos a datetime UTC.
            fecha_dt = None
            if rem.fecha:
                f = rem.fecha
                if isinstance(f, datetime):
                    fecha_dt = f if f.tzinfo else f.replace(tzinfo=timezone.utc)
                else:
                    fecha_dt = datetime.combine(f, datetime.min.time(), tzinfo=timezone.utc)
            items.append({
                "fuente": "Remito",
                "fecha": fecha_dt,
                "sku": it.sku,
                "titulo": it.descripcion,
                "cantidad": it.cantidad,
                "stock_aplicado": it.cantidad if rem.estado == "emitido" else 0,
                "monto": float(it.subtotal) if it.subtotal else None,
                "status": rem.estado,
                "ml_order_id": None,
                "buyer": (rem.cliente.razon_social if rem.cliente else None),
            })

    # Orden cronológico desc (las más nuevas primero)
    items.sort(key=lambda x: x["fecha"] or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    total = len(items)
    start = (page - 1) * limit
    return items[start:start + limit], total
