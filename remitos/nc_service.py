"""
remitos/nc_service.py
=====================
Lógica de negocio para Notas de Crédito.

Dos casos soportados (según lo acordado con Federico):
  1. NC asociada a un remito existente: se copian items del remito (pueden ajustarse)
     y al emitirla se REINGRESA el stock automáticamente.
  2. NC independiente: sin remito previo (bonificaciones, ajustes puntuales). También
     reingresa stock si los items tienen producto_id.

Siempre se reingresa el stock (fue la decisión del usuario).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Optional

import db
from clientes import repo as clientes_repo
from logger import get_logger
from remitos import service as remitos_service

log = get_logger(__name__)


# ==========================================================
# Tipos / motivos estándar
# ==========================================================
MOTIVOS_NC = [
    "Devolución",
    "Bonificación",
    "Ajuste de precio",
    "Error de facturación",
    "Garantía",
    "Otro",
]


# ==========================================================
# Estructuras
# ==========================================================
@dataclass
class ItemNC:
    descripcion: str
    cantidad: int
    precio_unitario: float = 0.0
    descuento_porc: float = 0.0
    producto_id: Optional[int] = None
    sku: Optional[str] = None
    orden: int = 0
    es_linea_libre: bool = False

    @property
    def subtotal(self) -> float:
        bruto = self.cantidad * self.precio_unitario
        return round(bruto * (1 - self.descuento_porc / 100.0), 2)


@dataclass
class NotaCredito:
    id: int
    numero: int
    numero_formateado: str
    cliente_id: int
    cliente_razon_social: str
    remito_id: Optional[int]
    remito_numero: Optional[str]
    fecha: str
    motivo: str
    detalle_motivo: Optional[str]
    subtotal: float
    total: float
    reingreso_stock: bool
    estado: str
    items: list[dict] = field(default_factory=list)


class NotaCreditoError(Exception):
    """Error al crear/manipular una NC."""


# ==========================================================
# Crear NC
# ==========================================================
def crear_nota_credito(
    cliente_id: int,
    items: list[ItemNC],
    *,
    motivo: str,
    detalle_motivo: Optional[str] = None,
    remito_id: Optional[int] = None,
    fecha: Optional[str] = None,
    reingreso_stock: bool = True,
) -> NotaCredito:
    """
    Crea una NC, reingresa stock si corresponde y devuelve el objeto persistido.

    Args:
        cliente_id: cliente al que se emite la NC.
        items: lista de ItemNC (al menos 1).
        motivo: motivo corto ("Devolución", "Bonificación", etc.)
        detalle_motivo: texto libre extendido.
        remito_id: si se asocia a un remito previo (opcional).
        fecha: ISO 'YYYY-MM-DD' (default hoy).
        reingreso_stock: default True (según acordado).
    """
    if not items:
        raise NotaCreditoError("La NC necesita al menos un item.")
    if not motivo or not motivo.strip():
        raise NotaCreditoError("El motivo de la NC es obligatorio.")

    cliente = clientes_repo.obtener(cliente_id)
    if cliente is None:
        raise NotaCreditoError(f"Cliente id={cliente_id} no existe.")

    # Si trae remito_id, validar que exista y pertenezca al mismo cliente
    if remito_id is not None:
        remito = remitos_service.obtener_remito(remito_id)
        if remito is None:
            raise NotaCreditoError(f"Remito id={remito_id} no existe.")
        if remito.cliente_id != cliente_id:
            raise NotaCreditoError(
                f"El remito {remito.numero_formateado} no pertenece al cliente "
                f"'{cliente.razon_social}'."
            )

    fecha_nc = fecha or date.today().isoformat()
    subtotal = round(sum(it.subtotal for it in items), 2)
    total = subtotal  # por ahora sin descuento general en NC

    with db.conexion() as conn:
        # Número correlativo atómico
        numero = db.siguiente_numero("nota_credito", conn)
        numero_fmt = db.formatear_numero_documento("NC", numero)

        conn.execute(
            """
            INSERT INTO notas_credito (
                numero, numero_formateado, cliente_id, remito_id, fecha,
                motivo, detalle_motivo, subtotal, total, reingreso_stock, estado
            ) VALUES (?,?,?,?,?,?,?,?,?,?,'emitida')
            """,
            (
                numero, numero_fmt, cliente_id, remito_id, fecha_nc,
                motivo.strip(), (detalle_motivo or "").strip() or None,
                subtotal, total, int(bool(reingreso_stock)),
            ),
        )
        nc_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]

        # Items
        for idx, it in enumerate(items, start=1):
            if it.cantidad <= 0:
                raise NotaCreditoError(f"Cantidad inválida en '{it.descripcion}': {it.cantidad}")
            conn.execute(
                """
                INSERT INTO notas_credito_items (
                    nota_credito_id, orden, producto_id, sku, descripcion,
                    cantidad, precio_unitario, descuento_porc, subtotal, es_linea_libre
                ) VALUES (?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    nc_id, it.orden or idx,
                    it.producto_id if not it.es_linea_libre else None,
                    it.sku, it.descripcion,
                    it.cantidad, it.precio_unitario, it.descuento_porc, it.subtotal,
                    int(bool(it.es_linea_libre)),
                ),
            )

            # Reingreso de stock si corresponde
            if reingreso_stock and not it.es_linea_libre and it.producto_id is not None:
                prod = conn.execute(
                    "SELECT stock_actual FROM productos WHERE id = ?", (it.producto_id,)
                ).fetchone()
                if prod is None:
                    continue
                stock_prev = int(prod["stock_actual"] or 0)
                stock_nuevo = stock_prev + it.cantidad
                conn.execute(
                    "UPDATE productos SET stock_actual = ?, fecha_modificacion = datetime('now','localtime') WHERE id = ?",
                    (stock_nuevo, it.producto_id),
                )
                conn.execute(
                    """
                    INSERT INTO movimientos_stock (
                        producto_id, tipo, cantidad, stock_previo, stock_nuevo, origen, notas
                    ) VALUES (?, 'ingreso', ?, ?, ?, ?, ?)
                    """,
                    (
                        it.producto_id, it.cantidad, stock_prev, stock_nuevo,
                        f"Nota de crédito {numero_fmt}",
                        f"{motivo} — Cliente: {cliente.razon_social}",
                    ),
                )

        log.info(f"NC creada: {numero_fmt} cliente={cliente.razon_social} total=${total:,.2f}")

    return obtener_nc(nc_id)  # type: ignore[return-value]


# ==========================================================
# Consulta
# ==========================================================
def obtener_nc(nc_id: int) -> Optional[NotaCredito]:
    with db.conexion() as c:
        cab = c.execute(
            """
            SELECT nc.*,
                   cl.razon_social    AS cliente_razon_social,
                   r.numero_formateado AS remito_numero
            FROM notas_credito nc
            JOIN clientes cl ON cl.id = nc.cliente_id
            LEFT JOIN remitos r ON r.id = nc.remito_id
            WHERE nc.id = ?
            """,
            (nc_id,),
        ).fetchone()
        if cab is None:
            return None

        items = c.execute(
            "SELECT * FROM notas_credito_items WHERE nota_credito_id = ? ORDER BY orden",
            (nc_id,),
        ).fetchall()

        return NotaCredito(
            id=cab["id"],
            numero=cab["numero"],
            numero_formateado=cab["numero_formateado"],
            cliente_id=cab["cliente_id"],
            cliente_razon_social=cab["cliente_razon_social"],
            remito_id=cab["remito_id"],
            remito_numero=cab["remito_numero"],
            fecha=cab["fecha"],
            motivo=cab["motivo"],
            detalle_motivo=cab["detalle_motivo"],
            subtotal=float(cab["subtotal"] or 0),
            total=float(cab["total"] or 0),
            reingreso_stock=bool(cab["reingreso_stock"]),
            estado=cab["estado"],
            items=[dict(i) for i in items],
        )


def listar(
    *,
    cliente_id: Optional[int] = None,
    remito_id: Optional[int] = None,
    desde: Optional[str] = None,
    hasta: Optional[str] = None,
    limite: int = 100,
) -> list[dict]:
    with db.conexion() as c:
        sql = [
            """
            SELECT nc.id, nc.numero, nc.numero_formateado, nc.fecha,
                   nc.total, nc.motivo, nc.estado,
                   cl.razon_social       AS cliente,
                   r.numero_formateado   AS remito
            FROM notas_credito nc
            JOIN clientes cl ON cl.id = nc.cliente_id
            LEFT JOIN remitos r ON r.id = nc.remito_id
            WHERE 1=1
            """
        ]
        params: list = []
        if cliente_id is not None:
            sql.append("AND nc.cliente_id = ?")
            params.append(cliente_id)
        if remito_id is not None:
            sql.append("AND nc.remito_id = ?")
            params.append(remito_id)
        if desde:
            sql.append("AND nc.fecha >= ?")
            params.append(desde)
        if hasta:
            sql.append("AND nc.fecha <= ?")
            params.append(hasta)
        sql.append("ORDER BY nc.numero DESC LIMIT ?")
        params.append(limite)

        return [dict(r) for r in c.execute(" ".join(sql), params).fetchall()]


def guardar_pdf_path(nc_id: int, pdf_path: str) -> None:
    with db.conexion() as c:
        c.execute("UPDATE notas_credito SET pdf_path = ? WHERE id = ?", (pdf_path, nc_id))


# ==========================================================
# Helper: construir items a partir de un remito existente
# ==========================================================
def copiar_items_de_remito(remito_id: int) -> list[ItemNC]:
    """
    Devuelve los items del remito convertidos a ItemNC, listos para ajustarse
    (típicamente se eliminan líneas o se baja la cantidad antes de emitir la NC).
    """
    remito = remitos_service.obtener_remito(remito_id)
    if remito is None:
        raise NotaCreditoError(f"Remito id={remito_id} no existe.")

    items = []
    for it in remito.items:
        items.append(ItemNC(
            descripcion=it["descripcion"],
            cantidad=int(it["cantidad"]),
            precio_unitario=float(it["precio_unitario"] or 0),
            descuento_porc=float(it["descuento_porc"] or 0),
            producto_id=it["producto_id"],
            sku=it["sku"],
            orden=int(it["orden"] or 0),
            es_linea_libre=bool(it["es_linea_libre"]),
        ))
    return items
