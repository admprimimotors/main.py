"""
app/remitos.py
==============
Servicio de remitos:
  - Crear remito con descuento automático de stock (atómico)
  - Anular remito con reingreso de stock
  - Consultar / listar remitos
  - Numeración correlativa continuando del histórico viejo

Items: cada línea puede ser un producto del catálogo (con producto_id) o una
línea libre (es_linea_libre=True, sin producto_id, no afecta stock).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Optional

from sqlalchemy import func as sql_func, or_, select
from sqlalchemy.orm import Session, selectinload

from .models import Cliente, Producto, Remito, RemitoItem


PAGE_SIZE = 50

# Punto de partida de la numeración si la tabla está vacía. El último remito del
# CSV viejo era el 1493 — arrancamos en 1494 para no pisar números.
NUMERO_INICIAL_REMITO = 1494


# =============================================================
# Tipos auxiliares
# =============================================================

@dataclass
class ItemInput:
    """Una línea propuesta del remito antes de persistir."""
    descripcion: str
    cantidad: int
    precio_unitario: Decimal = Decimal("0")
    descuento_porc: Decimal = Decimal("0")
    sku: Optional[str] = None


class RemitoError(Exception):
    """Error genérico al crear/anular remito."""


class StockInsuficienteError(RemitoError):
    def __init__(self, sku: str, disponible: int, pedido: int):
        self.sku = sku
        self.disponible = disponible
        self.pedido = pedido
        super().__init__(
            f"Stock insuficiente para SKU {sku}: hay {disponible}, se piden {pedido}"
        )


# =============================================================
# Helpers internos
# =============================================================

def _to_decimal(v, default: Decimal = Decimal("0")) -> Decimal:
    """Acepta int/float/str/Decimal/None y devuelve Decimal con 2 decimales."""
    if v is None or v == "":
        return default
    try:
        return Decimal(str(v).strip().replace(",", ".")).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
    except (InvalidOperation, ValueError):
        return default


def _calc_subtotal(cantidad: int, precio: Decimal, descuento_porc: Decimal) -> Decimal:
    """Subtotal redondeado a 2 decimales."""
    bruto = Decimal(cantidad) * precio
    factor = (Decimal("100") - descuento_porc) / Decimal("100")
    return (bruto * factor).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def next_remito_numero(db: Session) -> int:
    """Devuelve el siguiente número correlativo de remito."""
    last = db.execute(select(sql_func.max(Remito.numero))).scalar()
    if last is None:
        return NUMERO_INICIAL_REMITO
    return int(last) + 1


# =============================================================
# Crear / anular
# =============================================================

def crear_remito(
    db: Session,
    cliente_id: int,
    items: list[ItemInput],
    *,
    fecha: Optional[datetime] = None,
    condicion_venta: Optional[str] = None,
    forma_pago: Optional[str] = None,
    descuento_general: Decimal = Decimal("0"),
    observaciones: Optional[str] = None,
    permitir_stock_negativo: bool = False,
) -> Remito:
    """
    Crea un remito con sus items. Descuenta stock atómicamente.

    Si algún producto no tiene stock suficiente, levanta StockInsuficienteError
    y NO persiste nada. Para forzar el remito aunque el stock quede negativo,
    pasar permitir_stock_negativo=True.
    """
    if not items:
        raise RemitoError("El remito necesita al menos un item.")

    cliente = db.execute(
        select(Cliente).where(Cliente.id == cliente_id)
    ).scalar_one_or_none()
    if cliente is None:
        raise RemitoError(f"Cliente ID {cliente_id} no existe.")

    # Resolver cada item: si trae sku, buscar producto. Si no existe → línea libre.
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

    # Validar stock para items con producto
    if not permitir_stock_negativo:
        # Sumamos cantidades por producto en caso de líneas duplicadas
        cantidad_por_prod: dict[int, int] = {}
        for it in items_resolved:
            p = it["producto"]
            if p is None:
                continue
            cantidad_por_prod[p.id] = cantidad_por_prod.get(p.id, 0) + it["cantidad"]
        for prod_id, qty in cantidad_por_prod.items():
            prod = db.execute(
                select(Producto).where(Producto.id == prod_id)
            ).scalar_one()
            if prod.stock_actual < qty:
                raise StockInsuficienteError(prod.sku, prod.stock_actual, qty)

    # Totales
    subtotal_total = sum((it["subtotal"] for it in items_resolved), Decimal("0"))
    descuento_general_dec = _to_decimal(descuento_general)
    total = (subtotal_total - descuento_general_dec).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )

    # Crear remito + items en una transacción
    numero = next_remito_numero(db)
    remito = Remito(
        numero=numero,
        cliente_id=cliente_id,
        fecha=fecha or datetime.now(timezone.utc),
        condicion_venta=(condicion_venta or "").strip() or None,
        forma_pago=(forma_pago or "").strip() or None,
        subtotal=subtotal_total.quantize(Decimal("0.01")),
        descuento_general=descuento_general_dec,
        total=total,
        observaciones=(observaciones or "").strip() or None,
        estado="emitido",
    )
    db.add(remito)
    db.flush()  # para tener remito.id

    for it in items_resolved:
        prod = it["producto"]
        ri = RemitoItem(
            remito_id=remito.id,
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
        db.add(ri)
        # Descontar stock SOLO si tiene producto
        if prod is not None:
            prod.stock_actual = prod.stock_actual - it["cantidad"]

    db.commit()
    db.refresh(remito)
    return remito


def anular_remito(
    db: Session,
    remito_id: int,
    motivo: Optional[str] = None,
) -> tuple[bool, str, Optional[int]]:
    """
    Anula un remito y AUTOMÁTICAMENTE genera una Nota de Crédito espejo con
    los mismos items linkeada a este remito. La NC es la que reincorpora el
    stock — anular_remito en sí no toca stock para no duplicar el efecto.

    Devuelve (ok, mensaje, nc_id_o_None). El nc_id permite al caller redirigir
    al detalle de la NC creada.
    """
    remito = db.execute(
        select(Remito).where(Remito.id == remito_id).options(selectinload(Remito.items))
    ).scalar_one_or_none()
    if remito is None:
        return False, f"No existe el remito ID {remito_id}", None
    if remito.estado == "anulado":
        return False, f"El remito {remito.numero} ya está anulado", None

    # Marcar el remito como anulado
    remito.estado = "anulado"
    remito.fecha_anulacion = datetime.now(timezone.utc)
    remito.motivo_anulacion = (motivo or "").strip() or None

    # Generar la NC con los items del remito.
    # Import lazy para evitar import circular (notas_credito importa de remitos).
    from . import notas_credito as nc_svc

    items_para_nc = [
        ItemInput(
            descripcion=it.descripcion,
            cantidad=it.cantidad,
            precio_unitario=it.precio_unitario,
            descuento_porc=it.descuento_porc,
            sku=it.sku,
        )
        for it in remito.items
    ]

    # Necesitamos commit del remito anulado antes de crear la NC porque crear_nc
    # también hace commit. Lo persistimos primero.
    db.flush()

    nc = nc_svc.crear_nc(
        db,
        cliente_id=remito.cliente_id,
        items=items_para_nc,
        motivo="Anulación de remito",
        remito_origen_id=remito.id,
        descuento_general=remito.descuento_general,
        observaciones=(
            f"Generada automáticamente por anulación del remito Nº {remito.numero}. "
            + (f"Motivo: {motivo}." if motivo else "")
        ).strip(),
    )

    return (
        True,
        f"✓ Remito {remito.numero} anulado · NC {nc.numero} generada con stock reincorporado",
        nc.id,
    )


# =============================================================
# Consultas
# =============================================================

def get_remito(db: Session, remito_id: int) -> Optional[Remito]:
    return db.execute(
        select(Remito)
        .where(Remito.id == remito_id)
        .options(selectinload(Remito.items), selectinload(Remito.cliente))
    ).scalar_one_or_none()


def list_remitos(
    db: Session,
    *,
    search: str = "",
    cliente_id: Optional[int] = None,
    estado: str = "",
    page: int = 1,
) -> tuple[list[dict], int]:
    """
    Lista remitos con filtros y paginación.
    Devuelve (lista_de_dicts_con_cliente_e_items, total).

    Estrategia: select sobre Remito solo (no multi-entity) + eager load de
    items y cliente vía selectinload. El cliente_razon_social se accede vía
    relationship en el loop.
    """
    base_q = select(Remito)
    count_q = select(sql_func.count(Remito.id))
    needs_join = False

    extra_conds = []
    if search and search.strip():
        like = f"%{search.strip()}%"
        try:
            num_int = int(search.strip())
            extra_conds.append(or_(
                Remito.numero == num_int,
                Cliente.razon_social.ilike(like),
            ))
            needs_join = True
        except ValueError:
            extra_conds.append(Cliente.razon_social.ilike(like))
            needs_join = True
    if cliente_id is not None:
        extra_conds.append(Remito.cliente_id == cliente_id)
    if estado in ("emitido", "anulado"):
        extra_conds.append(Remito.estado == estado)

    if needs_join:
        base_q = base_q.join(Cliente, Cliente.id == Remito.cliente_id)
        count_q = count_q.join(Cliente, Cliente.id == Remito.cliente_id)

    for cond in extra_conds:
        base_q = base_q.where(cond)
        count_q = count_q.where(cond)

    total = int(db.execute(count_q).scalar() or 0)
    page = max(1, page)

    base_q = (
        base_q
        .options(selectinload(Remito.items), selectinload(Remito.cliente))
        .order_by(Remito.numero.desc())
        .limit(PAGE_SIZE)
        .offset((page - 1) * PAGE_SIZE)
    )

    rows: list[dict] = []
    for remito in db.execute(base_q).scalars().all():
        rows.append({
            "id": remito.id,
            "numero": remito.numero,
            "fecha": remito.fecha,
            "cliente_id": remito.cliente_id,
            "cliente_razon_social": remito.cliente.razon_social if remito.cliente else "",
            "subtotal": remito.subtotal,
            "descuento_general": remito.descuento_general,
            "total": remito.total,
            "estado": remito.estado,
            "items": [
                {
                    "sku": it.sku,
                    "descripcion": it.descripcion,
                    "cantidad": it.cantidad,
                    "precio_unitario": float(it.precio_unitario or 0),
                    "subtotal": float(it.subtotal or 0),
                    "es_linea_libre": it.es_linea_libre,
                }
                for it in remito.items
            ],
        })
    return rows, total
