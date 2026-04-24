"""
clientes/repo.py
================
CRUD y consultas de clientes.

Todo lo que toca la tabla `clientes` pasa por acá.
Las validaciones ligeras (CUIT, email) viven en este archivo para no duplicar lógica.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from typing import Optional

import db
from logger import get_logger

log = get_logger(__name__)


# ==========================================================
# Condición IVA (valores estándar Argentina)
# ==========================================================
CONDICIONES_IVA = [
    "Responsable Inscripto",
    "Monotributista",
    "Consumidor Final",
    "Exento",
    "No Responsable",
    "Sujeto No Categorizado",
]


# ==========================================================
# Representación de cliente
# ==========================================================
@dataclass
class Cliente:
    id: Optional[int] = None
    razon_social: str = ""
    nombre_comercial: Optional[str] = None
    cuit_dni: Optional[str] = None
    condicion_iva: Optional[str] = None
    direccion: Optional[str] = None
    localidad: Optional[str] = None
    provincia: Optional[str] = None
    codigo_postal: Optional[str] = None
    telefono: Optional[str] = None
    email: Optional[str] = None
    notas: Optional[str] = None
    activo: bool = True
    fecha_alta: Optional[str] = None
    fecha_modificacion: Optional[str] = None

    @classmethod
    def from_row(cls, row) -> "Cliente":
        return cls(
            id=row["id"],
            razon_social=row["razon_social"],
            nombre_comercial=row["nombre_comercial"],
            cuit_dni=row["cuit_dni"],
            condicion_iva=row["condicion_iva"],
            direccion=row["direccion"],
            localidad=row["localidad"],
            provincia=row["provincia"],
            codigo_postal=row["codigo_postal"],
            telefono=row["telefono"],
            email=row["email"],
            notas=row["notas"],
            activo=bool(row["activo"]),
            fecha_alta=row["fecha_alta"],
            fecha_modificacion=row["fecha_modificacion"],
        )

    def direccion_completa(self) -> str:
        """Arma 'Calle 15 4971, Berazategui, Buenos Aires CP 1884'."""
        partes = [self.direccion, self.localidad, self.provincia]
        cp = f"CP {self.codigo_postal}" if self.codigo_postal else ""
        base = ", ".join(p for p in partes if p)
        return f"{base} {cp}".strip(", ").strip()


# ==========================================================
# Validaciones
# ==========================================================
_REGEX_EMAIL = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def normalizar_cuit_dni(valor: str | None) -> str | None:
    """Quita guiones, puntos y espacios. '23-37354799-9' → '23373547999'."""
    if not valor:
        return None
    s = re.sub(r"[\s.\-]", "", str(valor))
    return s or None


def formatear_cuit(cuit: str | None) -> str:
    """'23373547999' → '23-37354799-9'. Si no tiene 11 dígitos, devuelve tal cual."""
    if not cuit:
        return ""
    s = normalizar_cuit_dni(cuit) or ""
    if len(s) == 11 and s.isdigit():
        return f"{s[:2]}-{s[2:10]}-{s[10]}"
    return s


def validar_cuit(cuit: str | None) -> bool:
    """
    Valida el dígito verificador del CUIT (algoritmo estándar AFIP).
    Acepta CUIT sin formato ('23373547999') o con guiones ('23-37354799-9').
    """
    s = normalizar_cuit_dni(cuit)
    if not s or len(s) != 11 or not s.isdigit():
        return False
    pesos = [5, 4, 3, 2, 7, 6, 5, 4, 3, 2]
    suma = sum(int(s[i]) * pesos[i] for i in range(10))
    resto = suma % 11
    dv = 11 - resto
    if dv == 11:
        dv = 0
    elif dv == 10:
        dv = 9
    return dv == int(s[10])


def validar_email(email: str | None) -> bool:
    if not email:
        return True  # vacío es válido (email es opcional)
    return bool(_REGEX_EMAIL.match(email.strip()))


# ==========================================================
# CRUD
# ==========================================================
def crear(cliente: Cliente) -> int:
    """
    Inserta un cliente nuevo y devuelve su id.
    Lanza ValueError si faltan datos obligatorios o si hay CUIT duplicado.
    """
    if not cliente.razon_social or not cliente.razon_social.strip():
        raise ValueError("La razón social es obligatoria.")

    cuit_limpio = normalizar_cuit_dni(cliente.cuit_dni)
    if cliente.email and not validar_email(cliente.email):
        raise ValueError(f"Email inválido: {cliente.email}")

    with db.conexion() as c:
        # Evitar duplicados por CUIT
        if cuit_limpio:
            existe = c.execute(
                "SELECT id FROM clientes WHERE cuit_dni = ? AND activo = 1",
                (cuit_limpio,),
            ).fetchone()
            if existe:
                raise ValueError(
                    f"Ya existe un cliente con CUIT/DNI {formatear_cuit(cuit_limpio)} (id={existe['id']})."
                )

        c.execute(
            """
            INSERT INTO clientes (
                razon_social, nombre_comercial, cuit_dni, condicion_iva,
                direccion, localidad, provincia, codigo_postal,
                telefono, email, notas, activo
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,1)
            """,
            (
                cliente.razon_social.strip(),
                (cliente.nombre_comercial or "").strip() or None,
                cuit_limpio,
                cliente.condicion_iva,
                (cliente.direccion or "").strip() or None,
                (cliente.localidad or "").strip() or None,
                (cliente.provincia or "").strip() or None,
                (cliente.codigo_postal or "").strip() or None,
                (cliente.telefono or "").strip() or None,
                (cliente.email or "").strip() or None,
                (cliente.notas or "").strip() or None,
            ),
        )
        nuevo_id = c.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
        log.info(f"Cliente creado: id={nuevo_id} razon_social='{cliente.razon_social}'")
        return int(nuevo_id)


def obtener(cliente_id: int) -> Optional[Cliente]:
    """Devuelve el cliente o None si no existe."""
    with db.conexion() as c:
        row = c.execute("SELECT * FROM clientes WHERE id = ?", (cliente_id,)).fetchone()
        return Cliente.from_row(row) if row else None


def actualizar(cliente: Cliente) -> None:
    """Actualiza un cliente existente (debe tener id)."""
    if cliente.id is None:
        raise ValueError("El cliente debe tener id para actualizarse.")
    if not cliente.razon_social or not cliente.razon_social.strip():
        raise ValueError("La razón social es obligatoria.")
    if cliente.email and not validar_email(cliente.email):
        raise ValueError(f"Email inválido: {cliente.email}")

    cuit_limpio = normalizar_cuit_dni(cliente.cuit_dni)

    with db.conexion() as c:
        # Chequear duplicado de CUIT en OTRO cliente
        if cuit_limpio:
            existe = c.execute(
                "SELECT id FROM clientes WHERE cuit_dni = ? AND activo = 1 AND id != ?",
                (cuit_limpio, cliente.id),
            ).fetchone()
            if existe:
                raise ValueError(
                    f"Ya existe otro cliente con ese CUIT/DNI (id={existe['id']})."
                )

        c.execute(
            """
            UPDATE clientes SET
                razon_social       = ?,
                nombre_comercial   = ?,
                cuit_dni           = ?,
                condicion_iva      = ?,
                direccion          = ?,
                localidad          = ?,
                provincia          = ?,
                codigo_postal      = ?,
                telefono           = ?,
                email              = ?,
                notas              = ?,
                fecha_modificacion = datetime('now','localtime')
            WHERE id = ?
            """,
            (
                cliente.razon_social.strip(),
                (cliente.nombre_comercial or "").strip() or None,
                cuit_limpio,
                cliente.condicion_iva,
                (cliente.direccion or "").strip() or None,
                (cliente.localidad or "").strip() or None,
                (cliente.provincia or "").strip() or None,
                (cliente.codigo_postal or "").strip() or None,
                (cliente.telefono or "").strip() or None,
                (cliente.email or "").strip() or None,
                (cliente.notas or "").strip() or None,
                cliente.id,
            ),
        )
        log.info(f"Cliente actualizado: id={cliente.id}")


def dar_de_baja(cliente_id: int) -> None:
    """Marca el cliente como inactivo (soft-delete). No borra registros."""
    with db.conexion() as c:
        c.execute("UPDATE clientes SET activo = 0, fecha_modificacion = datetime('now','localtime') WHERE id = ?",
                  (cliente_id,))
        log.info(f"Cliente dado de baja: id={cliente_id}")


def reactivar(cliente_id: int) -> None:
    """Vuelve a activar un cliente dado de baja."""
    with db.conexion() as c:
        c.execute("UPDATE clientes SET activo = 1, fecha_modificacion = datetime('now','localtime') WHERE id = ?",
                  (cliente_id,))


# ==========================================================
# Búsqueda
# ==========================================================
def buscar(
    texto: Optional[str] = None,
    *,
    solo_activos: bool = True,
    limite: int = 50,
) -> list[Cliente]:
    """
    Busca clientes por razón social, nombre comercial, CUIT/DNI, email o localidad.
    """
    with db.conexion() as c:
        sql = ["SELECT * FROM clientes WHERE 1=1"]
        params: list = []

        if solo_activos:
            sql.append("AND activo = 1")

        if texto:
            like = f"%{texto.strip()}%"
            sql.append(
                """AND (
                    razon_social     LIKE ? COLLATE NOCASE
                 OR nombre_comercial LIKE ? COLLATE NOCASE
                 OR cuit_dni         LIKE ?
                 OR email            LIKE ? COLLATE NOCASE
                 OR localidad        LIKE ? COLLATE NOCASE
                )"""
            )
            params += [like, like, like, like, like]

        sql.append("ORDER BY razon_social LIMIT ?")
        params.append(limite)

        rows = c.execute(" ".join(sql), params).fetchall()
        return [Cliente.from_row(r) for r in rows]


def buscar_por_cuit(cuit: str) -> Optional[Cliente]:
    """Búsqueda exacta por CUIT/DNI normalizado."""
    cuit_limpio = normalizar_cuit_dni(cuit)
    if not cuit_limpio:
        return None
    with db.conexion() as c:
        row = c.execute(
            "SELECT * FROM clientes WHERE cuit_dni = ? LIMIT 1", (cuit_limpio,)
        ).fetchone()
        return Cliente.from_row(row) if row else None


# ==========================================================
# Historial de compras
# ==========================================================
@dataclass
class ResumenHistorial:
    total_remitos: int = 0
    total_notas_credito: int = 0
    monto_total_remitos: float = 0.0
    monto_total_nc: float = 0.0
    monto_neto: float = 0.0          # remitos - NC
    ultima_compra: Optional[str] = None


@dataclass
class EntradaHistorial:
    tipo: str            # "remito" | "nota_credito"
    numero: str          # "R-0000001" o "NC-0000001"
    fecha: str
    total: float
    estado: str
    referencia: Optional[str] = None   # para NC: qué remito refiere


def resumen_historial(cliente_id: int) -> ResumenHistorial:
    """Devuelve totales agregados de remitos y NCs del cliente."""
    with db.conexion() as c:
        row_r = c.execute(
            """
            SELECT COUNT(*) AS n, COALESCE(SUM(total), 0) AS total
            FROM remitos
            WHERE cliente_id = ? AND estado = 'emitido'
            """,
            (cliente_id,),
        ).fetchone()
        row_nc = c.execute(
            """
            SELECT COUNT(*) AS n, COALESCE(SUM(total), 0) AS total
            FROM notas_credito
            WHERE cliente_id = ? AND estado = 'emitida'
            """,
            (cliente_id,),
        ).fetchone()
        row_ultima = c.execute(
            """
            SELECT MAX(fecha) AS f FROM (
                SELECT fecha FROM remitos        WHERE cliente_id = ?
                UNION ALL
                SELECT fecha FROM notas_credito  WHERE cliente_id = ?
            )
            """,
            (cliente_id, cliente_id),
        ).fetchone()

        total_r = float(row_r["total"] or 0)
        total_nc = float(row_nc["total"] or 0)
        return ResumenHistorial(
            total_remitos=int(row_r["n"] or 0),
            total_notas_credito=int(row_nc["n"] or 0),
            monto_total_remitos=total_r,
            monto_total_nc=total_nc,
            monto_neto=total_r - total_nc,
            ultima_compra=row_ultima["f"] if row_ultima else None,
        )


def historial(cliente_id: int, limite: int = 50) -> list[EntradaHistorial]:
    """
    Devuelve el historial mezclado de remitos + notas de crédito del cliente,
    ordenado por fecha descendente.
    """
    with db.conexion() as c:
        rows = c.execute(
            """
            SELECT 'remito' AS tipo, numero_formateado AS numero, fecha, total, estado, NULL AS referencia
            FROM remitos WHERE cliente_id = ?
            UNION ALL
            SELECT 'nota_credito' AS tipo, nc.numero_formateado AS numero, nc.fecha, nc.total, nc.estado,
                   r.numero_formateado AS referencia
            FROM notas_credito nc
            LEFT JOIN remitos r ON r.id = nc.remito_id
            WHERE nc.cliente_id = ?
            ORDER BY fecha DESC, numero DESC
            LIMIT ?
            """,
            (cliente_id, cliente_id, limite),
        ).fetchall()

        return [
            EntradaHistorial(
                tipo=r["tipo"],
                numero=r["numero"],
                fecha=r["fecha"],
                total=float(r["total"] or 0),
                estado=r["estado"],
                referencia=r["referencia"],
            )
            for r in rows
        ]


def listar_todos(solo_activos: bool = True) -> list[Cliente]:
    """Lista corta para dropdowns y selectores."""
    return buscar(texto=None, solo_activos=solo_activos, limite=10_000)
