"""
remitos/service.py
==================
Lógica de negocio para remitos.

Responsabilidades:
 - Crear un remito con validaciones (cliente existe, productos con stock, etc.)
 - Descontar stock automáticamente y registrar los movimientos
 - Asignar número correlativo único
 - Anular un remito (reingresa el stock)
 - Consultar remitos existentes

El PDF se genera en remitos/pdf.py usando el resultado de crear_remito().
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Optional

import db
from clientes import repo as clientes_repo
from logger import get_logger

log = get_logger(__name__)


# ==========================================================
# Estructuras de datos
# ==========================================================
@dataclass
class ItemRemito:
    """Representa una línea del remito antes de persistirse."""
    descripcion: str
    cantidad: int
    precio_unitario: float = 0.0
    descuento_porc: float = 0.0
    producto_id: Optional[int] = None  # None = línea libre (mano de obra, flete...)
    sku: Optional[str] = None
    orden: int = 0
    es_linea_libre: bool = False

    @property
    def subtotal(self) -> float:
        bruto = self.cantidad * self.precio_unitario
        return round(bruto * (1 - self.descuento_porc / 100.0), 2)


@dataclass
class Remito:
    id: int
    numero: int
    numero_formateado: str
    cliente_id: int
    cliente_razon_social: str
    fecha: str
    condicion_venta: Optional[str]
    forma_pago: Optional[str]
    subtotal: float
    descuento_general: float
    total: float
    observaciones: Optional[str]
    estado: str
    items: list[dict] = field(default_factory=list)


# ==========================================================
# Errores de dominio
# ==========================================================
class RemitoError(Exception):
    """Error genérico de creación/anulación de remito."""


class StockInsuficienteError(RemitoError):
    """Se intentó descontar más stock del disponible."""
    def __init__(self, sku: str, disponible: int, pedido: int):
        self.sku = sku
        self.disponible = disponible
        self.pedido = pedido
        super().__init__(
            f"Stock insuficiente para SKU {sku}: disponible {disponible}, pedido {pedido}"
        )


# ==========================================================
# Crear remito
# ==========================================================
def crear_remito(
    cliente_id: int,
    items: list[ItemRemito],
    *,
    fecha: Optional[str] = None,
    condicion_venta: Optional[str] = None,
    forma_pago: Optional[str] = None,
    descuento_general: float = 0.0,
    observaciones: Optional[str] = None,
    permitir_stock_negativo: bool = False,
) -> Remito:
    """
    Crea un remito, descuenta stock y devuelve el objeto Remito persistido.

    Todo sucede en UNA sola transacción: si algo falla, no se toca nada.

    Args:
        cliente_id: FK a clientes.id
        items: lista de ItemRemito (al menos 1)
        fecha: ISO 'YYYY-MM-DD' (default = hoy)
        condicion_venta: texto libre ("Contado", "Cuenta corriente", "Transferencia")
        forma_pago: texto libre
        descuento_general: descuento aplicado al total en $ (no %)
        observaciones: texto libre
        permitir_stock_negativo: si False (default), lanza StockInsuficienteError
            cuando no hay stock suficiente. Si True, deja pasar y queda stock negativo.
    """
    if not items:
        raise RemitoError("El remito necesita al menos un item.")

    cliente = clientes_repo.obtener(cliente_id)
    if cliente is None:
        raise RemitoError(f"Cliente id={cliente_id} no existe.")
    if not cliente.activo:
        raise RemitoError(f"Cliente '{cliente.razon_social}' está dado de baja.")

    fecha_remito = fecha or date.today().isoformat()

    # Calcular subtotales
    subtotal = round(sum(it.subtotal for it in items), 2)
    total = round(max(0.0, subtotal - (descuento_general or 0.0)), 2)

    with db.conexion() as conn:
        # 1) Validar stock y (si corresponde) descontarlo
        movimientos_a_registrar: list[tuple] = []

        for it in items:
            if it.cantidad <= 0:
                raise RemitoError(f"Cantidad inválida en item '{it.descripcion}': {it.cantidad}")

            if it.es_linea_libre or it.producto_id is None:
                continue

            row = conn.execute(
                "SELECT stock_actual, sku_master FROM productos WHERE id = ?",
                (it.producto_id,),
            ).fetchone()
            if row is None:
                raise RemitoError(f"Producto id={it.producto_id} no existe.")

            stock_actual = int(row["stock_actual"] or 0)
            if stock_actual < it.cantidad and not permitir_stock_negativo:
                raise StockInsuficienteError(
                    sku=row["sku_master"],
                    disponible=stock_actual,
                    pedido=it.cantidad,
                )

            stock_nuevo = stock_actual - it.cantidad
            movimientos_a_registrar.append((it.producto_id, stock_actual, stock_nuevo, it.cantidad))

        # 2) Asignar número correlativo (atómico)
        numero = db.siguiente_numero("remito", conn)
        numero_fmt = db.formatear_numero_documento("R", numero)

        # 3) Insertar cabecera
        conn.execute(
            """
            INSERT INTO remitos (
                numero, numero_formateado, cliente_id, fecha,
                condicion_venta, forma_pago,
                subtotal, descuento_general, total,
                observaciones, estado
            ) VALUES (?,?,?,?,?,?,?,?,?,?,'emitido')
            """,
            (
                numero, numero_fmt, cliente_id, fecha_remito,
                condicion_venta, forma_pago,
                subtotal, descuento_general, total,
                observaciones,
            ),
        )
        remito_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]

        # 4) Insertar items
        for idx, it in enumerate(items, start=1):
            conn.execute(
                """
                INSERT INTO remitos_items (
                    remito_id, orden, producto_id, sku, descripcion,
                    cantidad, precio_unitario, descuento_porc, subtotal, es_linea_libre
                ) VALUES (?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    remito_id, it.orden or idx,
                    it.producto_id if not it.es_linea_libre else None,
                    it.sku, it.descripcion,
                    it.cantidad, it.precio_unitario, it.descuento_porc, it.subtotal,
                    int(bool(it.es_linea_libre)),
                ),
            )

        # 5) Descontar stock real + registrar movimientos
        for producto_id, stock_prev, stock_nuevo, cantidad in movimientos_a_registrar:
            conn.execute(
                "UPDATE productos SET stock_actual = ?, fecha_modificacion = datetime('now','localtime') WHERE id = ?",
                (stock_nuevo, producto_id),
            )
            conn.execute(
                """
                INSERT INTO movimientos_stock (
                    producto_id, tipo, cantidad, stock_previo, stock_nuevo, origen, notas
                ) VALUES (?, 'egreso', ?, ?, ?, ?, ?)
                """,
                (
                    producto_id, -cantidad, stock_prev, stock_nuevo,
                    f"Remito {numero_fmt}",
                    f"Cliente: {cliente.razon_social}",
                ),
            )

        log.info(f"Remito creado: {numero_fmt} cliente={cliente.razon_social} total=${total:,.2f}")

    return obtener_remito(remito_id)  # type: ignore[return-value]


# ==========================================================
# Consulta
# ==========================================================
def obtener_remito(remito_id: int) -> Optional[Remito]:
    """Devuelve el remito completo con sus items, o None si no existe."""
    with db.conexion() as c:
        cab = c.execute(
            """
            SELECT r.*, cl.razon_social AS cliente_razon_social
            FROM remitos r
            JOIN clientes cl ON cl.id = r.cliente_id
            WHERE r.id = ?
            """,
            (remito_id,),
        ).fetchone()
        if cab is None:
            return None

        items = c.execute(
            """
            SELECT * FROM remitos_items
            WHERE remito_id = ?
            ORDER BY orden
            """,
            (remito_id,),
        ).fetchall()

        return Remito(
            id=cab["id"],
            numero=cab["numero"],
            numero_formateado=cab["numero_formateado"],
            cliente_id=cab["cliente_id"],
            cliente_razon_social=cab["cliente_razon_social"],
            fecha=cab["fecha"],
            condicion_venta=cab["condicion_venta"],
            forma_pago=cab["forma_pago"],
            subtotal=float(cab["subtotal"] or 0),
            descuento_general=float(cab["descuento_general"] or 0),
            total=float(cab["total"] or 0),
            observaciones=cab["observaciones"],
            estado=cab["estado"],
            items=[dict(i) for i in items],
        )


def obtener_por_numero(numero: int) -> Optional[Remito]:
    with db.conexion() as c:
        row = c.execute("SELECT id FROM remitos WHERE numero = ?", (numero,)).fetchone()
    return obtener_remito(row["id"]) if row else None


def listar(
    *,
    cliente_id: Optional[int] = None,
    desde: Optional[str] = None,
    hasta: Optional[str] = None,
    estado: Optional[str] = None,
    limite: int = 100,
) -> list[dict]:
    """Lista remitos con filtros opcionales."""
    with db.conexion() as c:
        sql = [
            """
            SELECT r.id, r.numero, r.numero_formateado, r.fecha,
                   r.subtotal, r.descuento_general, r.total, r.estado,
                   cl.razon_social AS cliente
            FROM remitos r
            JOIN clientes cl ON cl.id = r.cliente_id
            WHERE 1=1
            """
        ]
        params: list = []
        if cliente_id is not None:
            sql.append("AND r.cliente_id = ?")
            params.append(cliente_id)
        if desde:
            sql.append("AND r.fecha >= ?")
            params.append(desde)
        if hasta:
            sql.append("AND r.fecha <= ?")
            params.append(hasta)
        if estado:
            sql.append("AND r.estado = ?")
            params.append(estado)
        sql.append("ORDER BY r.numero DESC LIMIT ?")
        params.append(limite)

        rows = c.execute(" ".join(sql), params).fetchall()
        return [dict(r) for r in rows]


# ==========================================================
# Anular remito
# ==========================================================
def anular_remito(remito_id: int, motivo: str) -> None:
    """
    Marca un remito como anulado y reingresa el stock al inventario.
    No borra datos; deja trazabilidad completa.
    """
    if not motivo or not motivo.strip():
        raise RemitoError("Hay que especificar un motivo de anulación.")

    with db.conexion() as conn:
        cab = conn.execute(
            "SELECT id, numero_formateado, estado FROM remitos WHERE id = ?",
            (remito_id,),
        ).fetchone()
        if cab is None:
            raise RemitoError(f"Remito id={remito_id} no existe.")
        if cab["estado"] == "anulado":
            raise RemitoError(f"El remito {cab['numero_formateado']} ya está anulado.")

        items = conn.execute(
            """
            SELECT producto_id, cantidad, sku FROM remitos_items
            WHERE remito_id = ? AND producto_id IS NOT NULL AND es_linea_libre = 0
            """,
            (remito_id,),
        ).fetchall()

        for it in items:
            prod = conn.execute(
                "SELECT stock_actual FROM productos WHERE id = ?", (it["producto_id"],)
            ).fetchone()
            if prod is None:
                continue
            stock_prev = int(prod["stock_actual"] or 0)
            stock_nuevo = stock_prev + int(it["cantidad"])
            conn.execute(
                "UPDATE productos SET stock_actual = ?, fecha_modificacion = datetime('now','localtime') WHERE id = ?",
                (stock_nuevo, it["producto_id"]),
            )
            conn.execute(
                """
                INSERT INTO movimientos_stock (
                    producto_id, tipo, cantidad, stock_previo, stock_nuevo, origen, notas
                ) VALUES (?, 'ingreso', ?, ?, ?, ?, ?)
                """,
                (
                    it["producto_id"], int(it["cantidad"]), stock_prev, stock_nuevo,
                    f"Anulación remito {cab['numero_formateado']}",
                    motivo,
                ),
            )

        conn.execute(
            """
            UPDATE remitos
            SET estado = 'anulado',
                fecha_anulacion = datetime('now','localtime'),
                motivo_anulacion = ?
            WHERE id = ?
            """,
            (motivo.strip(), remito_id),
        )
        log.info(f"Remito anulado: {cab['numero_formateado']} motivo='{motivo}'")


def guardar_pdf_path(remito_id: int, pdf_path: str) -> None:
    """Registra la ruta del PDF generado (llamado desde remitos/pdf.py)."""
    with db.conexion() as c:
        c.execute("UPDATE remitos SET pdf_path = ? WHERE id = ?", (pdf_path, remito_id))
