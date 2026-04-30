"""
app/clientes.py
===============
Servicio del módulo Clientes:
  - Listado paginado con filtros (búsqueda, provincia, condición IVA, activo)
  - CRUD: crear, leer, actualizar (editar), archivar (soft delete)
  - Upload masivo de Excel/CSV con aliases para los headers del sistema viejo
  - Generador de template Excel

Schema compatible con el sistema local viejo: mismos campos que el dataclass
de `clientes/repo.py`. Esto permite migrar el `data/import/clientes.csv` o el
SQLite local sin rebuild de data.
"""

from __future__ import annotations

import io
import re
from dataclasses import dataclass, field
from typing import Any, Optional

import pandas as pd
from sqlalchemy import func as sql_func, or_, select
from sqlalchemy.orm import Session

from .catalogo import _is_blank, _norm_col, _parse_str
from .models import Cliente


PAGE_SIZE = 50

# Lista oficial de condiciones IVA en Argentina
CONDICIONES_IVA = [
    "Responsable Inscripto",
    "Monotributista",
    "Consumidor Final",
    "Exento",
    "No Responsable",
    "Sujeto No Categorizado",
]

# Las 24 jurisdicciones argentinas (23 provincias + CABA), orden alfabético.
# Mantenida como constante para evitar typos al cargar clientes.
PROVINCIAS_AR = [
    "Buenos Aires",
    "Catamarca",
    "Chaco",
    "Chubut",
    "Ciudad Autónoma de Buenos Aires",
    "Córdoba",
    "Corrientes",
    "Entre Ríos",
    "Formosa",
    "Jujuy",
    "La Pampa",
    "La Rioja",
    "Mendoza",
    "Misiones",
    "Neuquén",
    "Río Negro",
    "Salta",
    "San Juan",
    "San Luis",
    "Santa Cruz",
    "Santa Fe",
    "Santiago del Estero",
    "Tierra del Fuego",
    "Tucumán",
]


# =============================================================
# Helpers
# =============================================================

def _normalize_cuit(s: Any) -> Optional[str]:
    """'23-37354799-9' → '23373547999'. Quita guiones, puntos, espacios."""
    if _is_blank(s):
        return None
    raw = re.sub(r"[\s.\-]", "", str(s))
    return raw or None


def format_cuit_display(s: Optional[str]) -> str:
    """'23373547999' → '23-37354799-9' para mostrar en UI."""
    if not s:
        return ""
    digits = re.sub(r"\D", "", s)
    if len(digits) == 11:
        return f"{digits[:2]}-{digits[2:10]}-{digits[10]}"
    return s


def _parse_provincia_cp(value: Any) -> tuple[Optional[str], Optional[str]]:
    """
    Parsea celdas del estilo 'Buenos Aires' o 'Buenos Aires, CP 1878'
    y devuelve (provincia, codigo_postal).
    """
    if _is_blank(value):
        return None, None
    s = str(value).strip()
    cp_match = re.search(r"CP\s*(\d{4,8})", s, re.IGNORECASE)
    cp = cp_match.group(1) if cp_match else None
    # Saco la parte de CP del string para quedarme con la provincia
    provincia_str = re.sub(r",?\s*CP\s*\d+", "", s, flags=re.IGNORECASE).strip(", ").strip()
    if not provincia_str:
        return None, cp
    return provincia_str[:100], cp


# =============================================================
# Listado y filtros
# =============================================================

def list_clientes(
    db: Session,
    *,
    search: str = "",
    provincia: str = "",
    condicion_iva: str = "",
    incluir_archivados: bool = False,
    page: int = 1,
) -> tuple[list[Cliente], int]:
    """
    Lista clientes con filtros + paginación.
    Devuelve (clientes, total_matching).
    """
    base_q = select(Cliente)
    count_q = select(sql_func.count(Cliente.id))

    extra_conds = []
    if not incluir_archivados:
        extra_conds.append(Cliente.activo == True)  # noqa: E712

    if search and search.strip():
        like = f"%{search.strip()}%"
        # cuit/dni busca el dígito normalizado también
        like_digits = f"%{re.sub(r'[^0-9]', '', search)}%" if any(c.isdigit() for c in search) else like
        extra_conds.append(or_(
            Cliente.razon_social.ilike(like),
            Cliente.nombre_comercial.ilike(like),
            Cliente.email.ilike(like),
            Cliente.localidad.ilike(like),
            Cliente.cuit_dni.ilike(like_digits),
            Cliente.telefono.ilike(like),
        ))

    if provincia:
        extra_conds.append(Cliente.provincia == provincia)

    if condicion_iva:
        extra_conds.append(Cliente.condicion_iva == condicion_iva)

    for cond in extra_conds:
        base_q = base_q.where(cond)
        count_q = count_q.where(cond)

    total = int(db.execute(count_q).scalar() or 0)

    page = max(1, page)
    base_q = (
        base_q
        .order_by(Cliente.razon_social.asc())
        .limit(PAGE_SIZE)
        .offset((page - 1) * PAGE_SIZE)
    )
    clientes = list(db.execute(base_q).scalars().all())
    return clientes, total


def list_provincias(db: Session) -> list[str]:
    rows = db.execute(
        select(Cliente.provincia)
        .distinct()
        .where(Cliente.provincia.is_not(None))
        .order_by(Cliente.provincia)
    ).all()
    return [r[0] for r in rows if r[0]]


def list_condiciones_iva(db: Session) -> list[str]:
    rows = db.execute(
        select(Cliente.condicion_iva)
        .distinct()
        .where(Cliente.condicion_iva.is_not(None))
        .order_by(Cliente.condicion_iva)
    ).all()
    return [r[0] for r in rows if r[0]]


# =============================================================
# CRUD
# =============================================================

def get_cliente(db: Session, cliente_id: int) -> Optional[Cliente]:
    return db.execute(
        select(Cliente).where(Cliente.id == cliente_id)
    ).scalar_one_or_none()


def create_cliente(db: Session, **fields) -> tuple[Optional[Cliente], str]:
    """Crea un cliente nuevo. Devuelve (cliente, mensaje)."""
    razon_social = (fields.get("razon_social") or "").strip()
    if not razon_social:
        return None, "La razón social es obligatoria"

    # Normalizar cuit/dni (sin guiones)
    cuit = _normalize_cuit(fields.get("cuit_dni"))

    # Validar duplicado por CUIT si está cargado
    if cuit:
        existing = db.execute(
            select(Cliente).where(Cliente.cuit_dni == cuit)
        ).scalar_one_or_none()
        if existing:
            return None, f"Ya existe un cliente con CUIT/DNI {format_cuit_display(cuit)} (ID {existing.id})"

    cli = Cliente(
        razon_social=razon_social[:200],
        nombre_comercial=_parse_str(fields.get("nombre_comercial")),
        cuit_dni=cuit,
        condicion_iva=_parse_str(fields.get("condicion_iva")),
        direccion=_parse_str(fields.get("direccion")),
        localidad=_parse_str(fields.get("localidad")),
        provincia=_parse_str(fields.get("provincia")),
        codigo_postal=_parse_str(fields.get("codigo_postal")),
        telefono=_parse_str(fields.get("telefono")),
        email=_parse_str(fields.get("email")),
        notas=_parse_str(fields.get("notas")),
        activo=bool(fields.get("activo", True)),
    )
    db.add(cli)
    db.commit()
    db.refresh(cli)
    return cli, f"✓ Cliente '{cli.razon_social}' creado (ID {cli.id})"


def update_cliente(db: Session, cliente_id: int, **fields) -> tuple[bool, str]:
    """Actualiza un cliente existente. Devuelve (ok, mensaje)."""
    cli = get_cliente(db, cliente_id)
    if cli is None:
        return False, f"No existe el cliente ID {cliente_id}"

    razon_social = fields.get("razon_social")
    if razon_social is not None:
        razon_social = razon_social.strip()
        if not razon_social:
            return False, "La razón social no puede quedar vacía"
        cli.razon_social = razon_social[:200]

    if "nombre_comercial" in fields:
        cli.nombre_comercial = _parse_str(fields["nombre_comercial"])
    if "cuit_dni" in fields:
        new_cuit = _normalize_cuit(fields["cuit_dni"])
        # Validar duplicado en otro cliente
        if new_cuit and new_cuit != cli.cuit_dni:
            other = db.execute(
                select(Cliente).where(Cliente.cuit_dni == new_cuit, Cliente.id != cliente_id)
            ).scalar_one_or_none()
            if other:
                return False, f"Otro cliente ya tiene ese CUIT/DNI (ID {other.id})"
        cli.cuit_dni = new_cuit
    if "condicion_iva" in fields:
        cli.condicion_iva = _parse_str(fields["condicion_iva"])
    if "direccion" in fields:
        cli.direccion = _parse_str(fields["direccion"])
    if "localidad" in fields:
        cli.localidad = _parse_str(fields["localidad"])
    if "provincia" in fields:
        cli.provincia = _parse_str(fields["provincia"])
    if "codigo_postal" in fields:
        cli.codigo_postal = _parse_str(fields["codigo_postal"])
    if "telefono" in fields:
        cli.telefono = _parse_str(fields["telefono"])
    if "email" in fields:
        cli.email = _parse_str(fields["email"])
    if "notas" in fields:
        cli.notas = _parse_str(fields["notas"])
    if "activo" in fields:
        cli.activo = bool(fields["activo"])

    db.commit()
    return True, f"✓ Cliente '{cli.razon_social}' actualizado"


def archivar_cliente(db: Session, cliente_id: int) -> tuple[bool, str]:
    """Soft delete: pone activo=False. No borra de la DB para preservar histórico."""
    cli = get_cliente(db, cliente_id)
    if cli is None:
        return False, f"No existe el cliente ID {cliente_id}"
    cli.activo = False
    db.commit()
    return True, f"✓ Cliente '{cli.razon_social}' archivado"


def reactivar_cliente(db: Session, cliente_id: int) -> tuple[bool, str]:
    cli = get_cliente(db, cliente_id)
    if cli is None:
        return False, f"No existe el cliente ID {cliente_id}"
    cli.activo = True
    db.commit()
    return True, f"✓ Cliente '{cli.razon_social}' reactivado"


def eliminar_cliente(db: Session, cliente_id: int) -> tuple[bool, str]:
    """
    Borra DEFINITIVAMENTE un cliente de la DB. NO es reversible.
    A diferencia de archivar (soft delete que solo flippea activo=False),
    este DELETE saca el row de la tabla.

    Verifica post-commit que el row efectivamente se haya borrado, para
    detectar bugs silenciosos (ej: rollback implícito por algún listener).
    """
    cli = get_cliente(db, cliente_id)
    if cli is None:
        return False, f"No existe el cliente ID {cliente_id}"
    razon = cli.razon_social
    cid = cli.id

    try:
        db.delete(cli)
        db.commit()
    except Exception as e:
        db.rollback()
        return False, f"Error al eliminar: {type(e).__name__}: {e}"

    # Sanity check: ¿quedó realmente fuera?
    db.expire_all()  # invalidar cache de identity map
    still_there = db.execute(
        select(Cliente).where(Cliente.id == cid)
    ).scalar_one_or_none()
    if still_there is not None:
        return False, (
            f"DELETE no surtió efecto: el cliente '{razon}' (ID {cid}) "
            "sigue en la DB. Revisá los logs de Render."
        )

    return True, f"✓ Cliente '{razon}' eliminado definitivamente"


# =============================================================
# Excel upload — soporta headers del sistema viejo + headers nuevos
# =============================================================

# Mapping de headers normalizados (lower, sin tildes ni espacios) → campo del modelo
CLIENTE_COL_ALIASES: dict[str, str] = {
    # Razón social
    "razon_social": "razon_social",
    "razonsocial": "razon_social",
    "nombre": "razon_social",        # del CSV viejo
    "nombre_completo": "razon_social",
    "cliente": "razon_social",
    # Nombre comercial
    "nombre_comercial": "nombre_comercial",
    "fantasia": "nombre_comercial",
    # CUIT/DNI
    "cuit_dni": "cuit_dni",
    "cuit": "cuit_dni",
    "dni": "cuit_dni",
    "documento": "cuit_dni",
    # Condición IVA
    "condicion_iva": "condicion_iva",
    "condicioniva": "condicion_iva",
    "iva": "condicion_iva",
    # Dirección
    "direccion": "direccion",
    "domicilio": "direccion",
    # Localidad / ciudad
    "localidad": "localidad",
    "ciudad": "localidad",
    # Provincia (sola)
    "provincia": "provincia",
    # Provincia + CP combinado (CSV viejo: "Buenos Aires, CP 1878")
    "provincia_cp": "provincia_cp",
    "prov_cp": "provincia_cp",
    # Código postal
    "codigo_postal": "codigo_postal",
    "cp": "codigo_postal",
    # Teléfono
    "telefono": "telefono",
    "tel": "telefono",
    "celular": "telefono",
    # Email
    "email": "email",
    "mail": "email",
    "correo": "email",
    # Notas
    "notas": "notas",
    "observaciones": "notas",
    # Activo
    "activo": "activo",
}


@dataclass
class ClientesUploadResult:
    creados: int = 0
    actualizados: int = 0
    sin_cambios: int = 0
    errores: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return len(self.errores) == 0


def process_clientes_upload(db: Session, file_bytes: bytes) -> ClientesUploadResult:
    """
    Procesa Excel/CSV con clientes. Acepta los headers del CSV viejo
    (NOMBRE, DIRECCION, PROVINCIA_CP, TELEFONO, CUIT, MAIL) o el formato pleno.

    Estrategia de matcheo para upsert:
      - Si trae cuit_dni → busca por cuit_dni normalizado, update si existe
      - Si no trae cuit_dni pero trae email → busca por email
      - Si nada matchea → crea nuevo
    """
    result = ClientesUploadResult()

    try:
        sheets = pd.read_excel(io.BytesIO(file_bytes), sheet_name=None)
    except Exception as e:
        result.errores.append(f"No se pudo leer el archivo: {e}")
        return result

    # Tomar la primera hoja con razón social/nombre
    target_df = None
    for _name, df in sheets.items():
        df_copy = df.copy()
        df_copy.columns = [_norm_col(c) for c in df_copy.columns]
        cols = set(df_copy.columns)
        # Necesita al menos un campo de "nombre"
        if any(c in cols for c in ("razon_social", "nombre", "cliente", "nombre_completo")):
            target_df = df_copy
            break

    if target_df is None:
        result.errores.append(
            "Ninguna hoja tiene columna de nombre/razón social"
        )
        return result

    # Mapear columnas del Excel a campos del modelo
    field_to_col: dict[str, str] = {}
    for col in target_df.columns:
        if not col:
            continue
        target = CLIENTE_COL_ALIASES.get(col)
        if target:
            field_to_col[target] = col

    if "razon_social" not in field_to_col:
        result.errores.append("Falta columna de razón social/nombre")
        return result

    def _g(row, field_name):
        col = field_to_col.get(field_name)
        return row.get(col) if col else None

    for idx, row in target_df.iterrows():
        razon_social = _parse_str(_g(row, "razon_social"))
        if not razon_social:
            result.errores.append(f"Fila {idx + 2}: razón social vacía, saltada")
            continue

        cuit_norm = _normalize_cuit(_g(row, "cuit_dni"))
        email_str = _parse_str(_g(row, "email"))

        # Provincia + CP (formato combinado del CSV viejo)
        provincia = _parse_str(_g(row, "provincia"))
        cp = _parse_str(_g(row, "codigo_postal"))
        if "provincia_cp" in field_to_col:
            p, c = _parse_provincia_cp(_g(row, "provincia_cp"))
            provincia = provincia or p
            cp = cp or c

        # Buscar existente por CUIT, después por email
        existing = None
        if cuit_norm:
            existing = db.execute(
                select(Cliente).where(Cliente.cuit_dni == cuit_norm)
            ).scalar_one_or_none()
        if existing is None and email_str:
            existing = db.execute(
                select(Cliente).where(Cliente.email == email_str)
            ).scalar_one_or_none()

        # Datos comunes (los que vinieron en el Excel)
        data = {
            "razon_social": razon_social[:200],
        }
        if "nombre_comercial" in field_to_col:
            data["nombre_comercial"] = _parse_str(_g(row, "nombre_comercial"))
        if cuit_norm or "cuit_dni" in field_to_col:
            data["cuit_dni"] = cuit_norm
        if "condicion_iva" in field_to_col:
            data["condicion_iva"] = _parse_str(_g(row, "condicion_iva"))
        if "direccion" in field_to_col:
            data["direccion"] = _parse_str(_g(row, "direccion"))
        if "localidad" in field_to_col:
            data["localidad"] = _parse_str(_g(row, "localidad"))
        if provincia is not None:
            data["provincia"] = provincia
        if cp is not None:
            data["codigo_postal"] = cp
        if "telefono" in field_to_col:
            data["telefono"] = _parse_str(_g(row, "telefono"))
        if email_str is not None or "email" in field_to_col:
            data["email"] = email_str
        if "notas" in field_to_col:
            data["notas"] = _parse_str(_g(row, "notas"))

        if existing:
            # Actualizar solo campos que vinieron en el Excel
            cambio = False
            for k, v in data.items():
                if k == "razon_social" and not v:
                    continue
                if getattr(existing, k) != v:
                    setattr(existing, k, v)
                    cambio = True
            if cambio:
                result.actualizados += 1
            else:
                result.sin_cambios += 1
        else:
            cli = Cliente(activo=True, **data)
            db.add(cli)
            result.creados += 1

    db.commit()
    return result


def generate_clientes_template() -> bytes:
    """Excel template con 2 ejemplos."""
    output = io.BytesIO()
    df = pd.DataFrame([
        {
            "razon_social": "Pittavino Diego",
            "nombre_comercial": "",
            "cuit_dni": "20-12345678-9",
            "condicion_iva": "Consumidor Final",
            "telefono": "2215 751413",
            "email": "diegomartinpit@gmail.com",
            "direccion": "Calle 12 1234",
            "localidad": "La Plata",
            "provincia": "Buenos Aires",
            "codigo_postal": "1900",
            "notas": "",
            "activo": "SI",
        },
        {
            "razon_social": "Rectificaciones Ariel S.A.",
            "nombre_comercial": "Ariel Rectificaciones",
            "cuit_dni": "30-71234567-8",
            "condicion_iva": "Responsable Inscripto",
            "telefono": "11 4422-3333",
            "email": "ventas@arielrect.com.ar",
            "direccion": "Av. Mitre 4500",
            "localidad": "Avellaneda",
            "provincia": "Buenos Aires",
            "codigo_postal": "1870",
            "notas": "Cliente mayorista",
            "activo": "SI",
        },
    ])
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Clientes", index=False)
    return output.getvalue()
