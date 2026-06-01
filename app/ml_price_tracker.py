"""
app/ml_price_tracker.py
=======================
Tracking de precios de Mercado Libre — captura snapshots periódicos para
construir el histórico de cambios de precio que ML no expone vía API.

Cómo funciona:
  1. Cada N horas (auto-sync al cargar el dashboard, igual que ml_orders.py)
     o vía endpoint manual, se ejecuta `snapshot_all_active(db)`.
  2. Lista todas las publicaciones activas del seller (~1000+).
  3. Trae precio actual de cada una desde ML (en lotes de 20 con multi-get).
  4. Compara contra el último snapshot del mismo item en `ml_price_snapshots`.
  5. Inserta una fila nueva por item. Marca `is_change=True` si el precio
     cambió respecto al snapshot anterior.

El histórico se consulta filtrando por `ml_item_id` y/o `is_change=True`.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

from sqlalchemy import select, func as sql_func
from sqlalchemy.orm import Session

from . import ml_client
from .models import MLPriceSnapshot, PrecioCambioLog


# =============================================================
# Audit log de cambios de precio del sistema (Primi → DB local / ML)
# =============================================================

def log_precio_cambio(
    db: Session,
    *,
    sku: str,
    precio_anterior: Optional[Decimal],
    precio_nuevo: Decimal,
    fonte: str,
    origen: Optional[str] = None,
    usuario: Optional[str] = None,
    nota: Optional[str] = None,
    pushed_to_ml: bool = False,
    producto_id: Optional[int] = None,
    ml_item_id: Optional[str] = None,
    titulo: Optional[str] = None,
) -> Optional[PrecioCambioLog]:
    """
    Registra un cambio de precio efectuado desde el sistema.

    fonte: una de
      "sistema_bulk"        → bulk update vía /precios apply
      "sistema_individual"  → edit individual en /catalogo/{sku}
      "ml_push"             → push manual a ML
      "ml_sync_in"          → detectado al sincronizar desde ML (drift caught)

    No registra si `precio_anterior == precio_nuevo` (no hubo cambio real).
    No falla en caller — devuelve None si la inserción falla.
    """
    try:
        if precio_anterior is not None and Decimal(str(precio_anterior)) == Decimal(str(precio_nuevo)):
            return None
        row = PrecioCambioLog(
            producto_id=producto_id,
            sku=(sku or "").strip()[:64] or None,
            ml_item_id=(ml_item_id or "").strip()[:64] or None,
            titulo_snapshot=(titulo or "").strip()[:500] or None,
            precio_anterior=(
                Decimal(str(precio_anterior)) if precio_anterior is not None else None
            ),
            precio_nuevo=Decimal(str(precio_nuevo)),
            fonte=(fonte or "desconocido")[:40],
            origen=(origen or "")[:80] or None,
            usuario=(usuario or "")[:80] or None,
            nota=(nota or "")[:300] or None,
            pushed_to_ml=bool(pushed_to_ml),
        )
        db.add(row)
        db.flush()
        return row
    except Exception as e:
        print(f"[log_precio_cambio] no se pudo registrar: {type(e).__name__}: {e}")
        return None


def cambios_log_recientes(
    db: Session,
    *,
    days: int = 30,
    limit: int = 200,
    sku: Optional[str] = None,
    fonte: Optional[str] = None,
) -> list[PrecioCambioLog]:
    """Lista las filas de precio_cambios_log de los últimos N días."""
    from datetime import timedelta
    desde = datetime.now(timezone.utc) - timedelta(days=days)
    stmt = (
        select(PrecioCambioLog)
        .where(PrecioCambioLog.created_at >= desde)
        .order_by(PrecioCambioLog.created_at.desc())
        .limit(limit)
    )
    if sku:
        stmt = stmt.where(PrecioCambioLog.sku == sku)
    if fonte:
        stmt = stmt.where(PrecioCambioLog.fonte == fonte)
    return list(db.execute(stmt).scalars().all())


def historial_unificado(
    db: Session,
    *,
    days: int = 30,
    limit: int = 500,
    sku: Optional[str] = None,
    ml_item_id: Optional[str] = None,
    fuentes: Optional[list[str]] = None,
) -> list[dict]:
    """
    Devuelve histórico unificado de cambios de precio combinando 2 fuentes:
      - PrecioCambioLog          → "Sistema" (cambios disparados desde Primi)
      - MLPriceSnapshot          → "ML snapshot" (lecturas automáticas)

    Cada elemento devuelto es un dict con shape:
      {
        "fecha": datetime,
        "fuente": "sistema" | "ml_snapshot" | "ml_venta",
        "fuente_detalle": str,        # más específico ("sistema_bulk", "ml_push", etc.)
        "sku": str|None,
        "ml_item_id": str|None,
        "titulo": str|None,
        "precio_anterior": Decimal|None,
        "precio_nuevo": Decimal|None,
        "delta": Decimal|None,
        "delta_pct": float|None,
        "nota": str|None,
        "usuario": str|None,
        "pushed_to_ml": bool|None,
      }

    Ordenado por fecha desc.
    """
    from datetime import timedelta
    desde = datetime.now(timezone.utc) - timedelta(days=days)
    fuentes = fuentes or ["sistema", "ml_snapshot"]

    items: list[dict] = []

    # --- PrecioCambioLog ---
    if "sistema" in fuentes:
        stmt = (
            select(PrecioCambioLog)
            .where(PrecioCambioLog.created_at >= desde)
            .order_by(PrecioCambioLog.created_at.desc())
            .limit(limit)
        )
        if sku:
            stmt = stmt.where(PrecioCambioLog.sku == sku)
        if ml_item_id:
            stmt = stmt.where(PrecioCambioLog.ml_item_id == ml_item_id)
        for r in db.execute(stmt).scalars().all():
            delta = None
            delta_pct = None
            if r.precio_anterior is not None and r.precio_anterior != 0:
                try:
                    delta = Decimal(str(r.precio_nuevo)) - Decimal(str(r.precio_anterior))
                    delta_pct = float(delta / Decimal(str(r.precio_anterior)) * 100)
                except Exception:
                    delta = None
                    delta_pct = None
            items.append({
                "fecha": r.created_at,
                "fuente": "sistema",
                "fuente_detalle": r.fonte,
                "sku": r.sku,
                "ml_item_id": r.ml_item_id,
                "titulo": r.titulo_snapshot,
                "precio_anterior": r.precio_anterior,
                "precio_nuevo": r.precio_nuevo,
                "delta": delta,
                "delta_pct": delta_pct,
                "nota": r.nota,
                "usuario": r.usuario,
                "pushed_to_ml": r.pushed_to_ml,
            })

    # --- MLPriceSnapshot (snapshots automáticos vía API ML) ---
    if "ml_snapshot" in fuentes:
        stmt = (
            select(MLPriceSnapshot)
            .where(MLPriceSnapshot.captured_at >= desde)
            .where(MLPriceSnapshot.is_change.is_(True))
            .order_by(MLPriceSnapshot.captured_at.desc())
            .limit(limit)
        )
        if sku:
            stmt = stmt.where(MLPriceSnapshot.sku == sku)
        if ml_item_id:
            stmt = stmt.where(MLPriceSnapshot.ml_item_id == ml_item_id)

        snaps_rows = db.execute(stmt).scalars().all()

        # Buscar precio anterior para cada snapshot (snapshot previo del mismo item).
        for s in snaps_rows:
            prev_q = (
                select(MLPriceSnapshot.price)
                .where(MLPriceSnapshot.ml_item_id == s.ml_item_id)
                .where(MLPriceSnapshot.captured_at < s.captured_at)
                .order_by(MLPriceSnapshot.captured_at.desc())
                .limit(1)
            )
            prev_price = db.execute(prev_q).scalar()
            delta = None
            delta_pct = None
            if prev_price is not None and prev_price != 0:
                try:
                    delta = Decimal(str(s.price)) - Decimal(str(prev_price))
                    delta_pct = float(delta / Decimal(str(prev_price)) * 100)
                except Exception:
                    delta = None
                    delta_pct = None

            items.append({
                "fecha": s.captured_at,
                "fuente": "ml_snapshot",
                "fuente_detalle": "ml_api_snapshot",
                "sku": s.sku,
                "ml_item_id": s.ml_item_id,
                "titulo": s.title,
                "precio_anterior": prev_price,
                "precio_nuevo": s.price,
                "delta": delta,
                "delta_pct": delta_pct,
                "nota": None,
                "usuario": None,
                "pushed_to_ml": None,
            })

    # Ordenar por fecha desc.
    items.sort(key=lambda x: x["fecha"] or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    return items[:limit]


# (Eliminado: backfill desde MLOrders v0.52.0 — confusión con la intención del
#  usuario. Reemplazado por scraping del endpoint real del seller hub de ML.
#  Ver app/ml_seller_history.py.)


def _seller_id() -> Optional[int]:
    """Obtiene el user_id del seller actual desde ML."""
    try:
        me = ml_client.api_get("/users/me")
        return me.get("id")
    except Exception:
        return None


def _listar_ids_activos(user_id: int) -> list[str]:
    """Scan completo de items activos via /users/{id}/items/search."""
    ids: list[str] = []
    scroll_id: Optional[str] = None
    while True:
        params = {"search_type": "scan", "limit": 100, "status": "active"}
        if scroll_id:
            params["scroll_id"] = scroll_id
        r = ml_client.api_get(f"/users/{user_id}/items/search", params=params)
        batch = r.get("results", []) or []
        if not batch:
            break
        ids.extend(batch)
        scroll_id = r.get("scroll_id")
        if not scroll_id:
            break
    return ids


def _traer_meta_lotes(ids: list[str]) -> dict[str, dict]:
    """Multi-get de items en lotes de 20."""
    out: dict[str, dict] = {}
    attrs = "id,title,price,base_price,original_price,currency_id,status,available_quantity,sold_quantity,seller_sku,seller_custom_field,attributes"
    for i in range(0, len(ids), 20):
        batch = ids[i:i + 20]
        r = ml_client.api_get("/items", params={"ids": ",".join(batch), "attributes": attrs})
        for entry in r:
            if entry.get("code") != 200:
                continue
            b = entry["body"]
            sku = b.get("seller_sku") or b.get("seller_custom_field")
            if not sku:
                for a in b.get("attributes", []) or []:
                    if a.get("id") == "SELLER_SKU":
                        sku = a.get("value_name")
                        break
            out[b["id"]] = {
                "title": b.get("title"),
                "sku": sku,
                "price": b.get("price"),
                "base_price": b.get("base_price"),
                "original_price": b.get("original_price"),
                "currency": b.get("currency_id"),
                "status": b.get("status"),
                "available_quantity": b.get("available_quantity"),
                "sold_quantity": b.get("sold_quantity"),
            }
    return out


def _ultimo_precio(db: Session, ml_item_id: str) -> Optional[Decimal]:
    """Devuelve el precio del último snapshot del item (o None si no hay)."""
    r = db.execute(
        select(MLPriceSnapshot.price)
        .where(MLPriceSnapshot.ml_item_id == ml_item_id)
        .order_by(MLPriceSnapshot.captured_at.desc(), MLPriceSnapshot.id.desc())
        .limit(1)
    ).scalar_one_or_none()
    return r


def snapshot_all_active(db: Session, *, changed_only: bool = False) -> dict:
    """
    Captura snapshots de TODAS las publicaciones activas.

    Args:
        db: sesión SQLAlchemy.
        changed_only: si True, solo inserta filas para items donde el precio
                      cambió respecto al snapshot anterior. Por defecto False
                      (insertamos siempre, manteniendo el histórico completo).

    Returns:
        dict con resumen: total_items, n_nuevos, n_cambios, n_guardados.
    """
    user_id = _seller_id()
    if user_id is None:
        return {"ok": False, "error": "No se pudo obtener seller_id (¿ML configurado?)"}

    ids = _listar_ids_activos(user_id)
    meta = _traer_meta_lotes(ids)

    now = datetime.now(timezone.utc)
    n_guardados = 0
    n_nuevos = 0
    n_cambios = 0
    cambios_sample: list[dict] = []

    for iid, m in meta.items():
        precio = m.get("price")
        if precio is None:
            continue
        precio_dec = Decimal(str(precio))
        prev = _ultimo_precio(db, iid)
        es_cambio = (prev is None) or (abs(precio_dec - prev) > Decimal("0.005"))
        if prev is None:
            n_nuevos += 1
        if es_cambio and prev is not None:
            n_cambios += 1
            if len(cambios_sample) < 10:
                cambios_sample.append({
                    "iid": iid,
                    "title": m.get("title", "")[:80],
                    "antes": float(prev),
                    "ahora": float(precio_dec),
                    "delta_pct": float((precio_dec - prev) / prev * 100) if prev else 0,
                })

        if changed_only and not es_cambio:
            continue

        snap = MLPriceSnapshot(
            ml_item_id=iid,
            title=m.get("title"),
            sku=m.get("sku"),
            price=precio_dec,
            base_price=Decimal(str(m["base_price"])) if m.get("base_price") is not None else None,
            original_price=Decimal(str(m["original_price"])) if m.get("original_price") is not None else None,
            currency=m.get("currency"),
            status=m.get("status"),
            available_quantity=m.get("available_quantity"),
            sold_quantity=m.get("sold_quantity"),
            is_change=es_cambio,
            captured_at=now,
        )
        db.add(snap)
        n_guardados += 1

    db.commit()
    return {
        "ok": True,
        "captured_at": now.isoformat(),
        "total_items_activos": len(meta),
        "n_guardados": n_guardados,
        "n_nuevos": n_nuevos,
        "n_cambios": n_cambios,
        "cambios_sample": cambios_sample,
    }


def historial_de_item(db: Session, ml_item_id: str, *, only_changes: bool = True, limit: int = 100) -> list[MLPriceSnapshot]:
    """
    Devuelve el histórico de precios de un item, ordenado de más reciente
    a más viejo. Por defecto solo trae cambios (is_change=True).
    """
    stmt = (
        select(MLPriceSnapshot)
        .where(MLPriceSnapshot.ml_item_id == ml_item_id)
        .order_by(MLPriceSnapshot.captured_at.desc(), MLPriceSnapshot.id.desc())
        .limit(limit)
    )
    if only_changes:
        stmt = stmt.where(MLPriceSnapshot.is_change.is_(True))
    return list(db.execute(stmt).scalars().all())


def cambios_recientes(db: Session, *, days: int = 7, limit: int = 50) -> list[MLPriceSnapshot]:
    """Lista los cambios de precio detectados en los últimos N días."""
    from datetime import timedelta
    desde = datetime.now(timezone.utc) - timedelta(days=days)
    return list(db.execute(
        select(MLPriceSnapshot)
        .where(MLPriceSnapshot.captured_at >= desde)
        .where(MLPriceSnapshot.is_change.is_(True))
        .order_by(MLPriceSnapshot.captured_at.desc())
        .limit(limit)
    ).scalars().all())
