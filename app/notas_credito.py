"""
app/notas_credito.py
====================
Servicio de notas de crédito.

Inverso del remito: al crear una NC, el stock SUBE (devolución/reincorporación).
Al anular, el stock vuelve a bajar.

Una NC puede o no estar linkeada a un Remito de origen. Linkearla es útil para
trazabilidad (ej: remito 1490 generó NC 203 por devolución de un item).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional

from sqlalchemy import func as sql_func, or_, select
from sqlalchemy.orm import Session, selectinload

from .models import Cliente, NotaCredito, NotaCreditoItem, Producto, Remito
from .remitos import ItemInput, RemitoError, _calc_subtotal, _to_decimal


PAGE_SIZE = 50

# Punto de partida si la tabla está vacía. Última NC del histórico viejo: 202.
NUMERO_INICIAL_NC = 203

MOTIVOS_NC = [
    "Devolución",
    "Bonificación",
    "Error de facturación",
    "Garantía",
    "Ajuste de precio",
    "Otro",
]


def next_nc_numero(db: Session) -> int:
    last = db.execute(select(sql_func.max(NotaCredito.numero))).scalar()
    if last is None:
        return NUMERO_INICIAL_NC
    return int(last) + 1


# =============================================================
# Crear / anular
# =============================================================

def crear_nc(
    db: Session,
    cliente_id: int,
    items: list[ItemInput],
    *,
    fecha: Optional[datetime] = None,
    motivo: Optional[str] = None,
    remito_origen_id: Optional[int] = None,
    descuento_general: Decimal = Decimal("0"),
    observaciones: Optional[str] = None,
) -> NotaCredito:
    """
    Crea una NC con sus items. Suma stock atómicamente para los items con producto.
    """
    if not items:
        raise RemitoError("La NC necesita al menos un item.")

    cliente = db.execute(
        select(Cliente).where(Cliente.id == cliente_id)
    ).scalar_one_or_none()
    if cliente is None:
        raise RemitoError(f"Cliente ID {cliente_id} no existe.")

    # Validar remito de origen si se especificó
    if remito_origen_id:
        remito = db.execute(
            select(Remito).where(Remito.id == remito_origen_id)
        ).scalar_one_or_none()
        if remito is None:
            raise RemitoError(f"Remito de origen ID {remito_origen_id} no existe.")

    items_resolved: list[dict] = []
    for idx, it in enumerate(items):
        if it.cantidad <= 0:
            raise RemitoError(f"Item {idx + 1}: cantidad debe ser > 0.")

        producto: Optional[Producto] = None
        sku_norm = (it.sku or "").strip() or None
        if sku_norm:
            producto = db.execute(
                select(Producto).where(Producto.sku == sku_norm)
            ).scalar_one_or_none()

        es_linea_libre = producto is None
        descripcion = (it.descripcion or "").strip()
        if not descripcion and producto is not None:
            descripcion = producto.titulo
        if not descripcion:
            raise RemitoError(f"Item {idx + 1}: falta descripción.")

        precio = _to_decimal(it.precio_unitario)
        descuento = _to_decimal(it.descuento_porc)
        subtotal = _calc_subtotal(it.cantidad, precio, descuento)

        items_resolved.append({
            "producto": producto,
            "sku": (producto.sku if producto else sku_norm),
            "descripcion": descripcion[:500],
            "cantidad": int(it.cantidad),
            "precio_unitario": precio,
            "descuento_porc": descuento,
            "subtotal": subtotal,
            "orden": idx,
            "es_linea_libre": es_linea_libre,
        })

    subtotal_total = sum((it["subtotal"] for it in items_resolved), Decimal("0"))
    descuento_general_dec = _to_decimal(descuento_general)
    total = (subtotal_total - descuento_general_dec).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )

    numero = next_nc_numero(db)
    nc = NotaCredito(
        numero=numero,
        cliente_id=cliente_id,
        fecha=fecha or datetime.now(timezone.utc),
        motivo=(motivo or "").strip() or None,
        remito_origen_id=remito_origen_id,
        subtotal=subtotal_total.quantize(Decimal("0.01")),
        descuento_general=descuento_general_dec,
        total=total,
        observaciones=(observaciones or "").strip() or None,
        estado="emitida",
    )
    db.add(nc)
    db.flush()

    for it in items_resolved:
        prod = it["producto"]
        nci = NotaCreditoItem(
            nc_id=nc.id,
            producto_id=prod.id if prod else None,
            sku=it["sku"],
            descripcion=it["descripcion"],
            cantidad=it["cantidad"],
            precio_unitario=it["precio_unitario"],
            descuento_porc=it["descuento_porc"],
            subtotal=it["subtotal"],
            orden=it["orden"],
            es_linea_libre=it["es_linea_libre"],
        )
        db.add(nci)
        # SUMAR stock para items con producto (inverso del remito)
        if prod is not None:
            prod.stock_actual = prod.stock_actual + it["cantidad"]
            prod.stock_updated_at = datetime.now(timezone.utc)

    db.commit()
    db.refresh(nc)
    return nc


def anular_nc(
    db: Session,
    nc_id: int,
    motivo: Optional[str] = None,
) -> tuple[bool, str]:
    """Anula una NC: vuelve a RESTAR el stock que se había sumado."""
    nc = db.execute(
        select(NotaCredito).where(NotaCredito.id == nc_id).options(selectinload(NotaCredito.items))
    ).scalar_one_or_none()
    if nc is None:
        return False, f"No existe la NC ID {nc_id}"
    if nc.estado == "anulada":
        return False, f"La NC {nc.numero} ya está anulada"

    for it in nc.items:
        if it.producto_id is not None:
            prod = db.execute(
                select(Producto).where(Producto.id == it.producto_id)
            ).scalar_one_or_none()
            if prod is not None:
                prod.stock_actual = prod.stock_actual - it.cantidad
                prod.stock_updated_at = datetime.now(timezone.utc)

    nc.estado = "anulada"
    nc.fecha_anulacion = datetime.now(timezone.utc)
    nc.motivo_anulacion = (motivo or "").strip() or None
    db.commit()
    return True, f"✓ NC {nc.numero} anulada, stock revertido"


# =============================================================
# Consultas
# =============================================================

def get_nc(db: Session, nc_id: int) -> Optional[NotaCredito]:
    return db.execute(
        select(NotaCredito)
        .where(NotaCredito.id == nc_id)
        .options(
            selectinload(NotaCredito.items),
            selectinload(NotaCredito.cliente),
            selectinload(NotaCredito.remito_origen),
        )
    ).scalar_one_or_none()


def list_ncs(
    db: Session,
    *,
    search: str = "",
    cliente_id: Optional[int] = None,
    estado: str = "",
    page: int = 1,
) -> tuple[list[dict], int]:
    base_q = select(NotaCredito)
    count_q = select(sql_func.count(NotaCredito.id))
    needs_join = False

    extra_conds = []
    if search and search.strip():
        like = f"%{search.strip()}%"
        try:
            num_int = int(search.strip())
            extra_conds.append(or_(
                NotaCredito.numero == num_int,
                Cliente.razon_social.ilike(like),
            ))
            needs_join = True
        except ValueError:
            extra_conds.append(Cliente.razon_social.ilike(like))
            needs_join = True
    if cliente_id is not None:
        extra_conds.append(NotaCredito.cliente_id == cliente_id)
    if estado in ("emitida", "anulada"):
        extra_conds.append(NotaCredito.estado == estado)

    if needs_join:
        base_q = base_q.join(Cliente, Cliente.id == NotaCredito.cliente_id)
        count_q = count_q.join(Cliente, Cliente.id == NotaCredito.cliente_id)

    for cond in extra_conds:
        base_q = base_q.where(cond)
        count_q = count_q.where(cond)

    total = int(db.execute(count_q).scalar() or 0)
    page = max(1, page)
    base_q = (
        base_q
        .options(selectinload(NotaCredito.cliente))
        .order_by(NotaCredito.numero.desc())
        .limit(PAGE_SIZE)
        .offset((page - 1) * PAGE_SIZE)
    )

    rows: list[dict] = []
    for nc in db.execute(base_q).scalars().all():
        rows.append({
            "id": nc.id,
            "numero": nc.numero,
            "fecha": nc.fecha,
            "cliente_id": nc.cliente_id,
            "cliente_razon_social": nc.cliente.razon_social if nc.cliente else "",
            "motivo": nc.motivo,
            "remito_origen_id": nc.remito_origen_id,
            "subtotal": nc.subtotal,
            "descuento_general": nc.descuento_general,
            "total": nc.total,
            "estado": nc.estado,
        })
    return rows, total
