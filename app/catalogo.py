"""
app/catalogo.py
===============
Servicio del módulo Catálogo:
  - Parsear Excel master (hojas Catalogo + Compatibilidades opcional)
  - Upsert en Postgres (productos + vehículos + compatibilidades)
  - Listar productos con búsqueda y paginación

Convenciones del Excel:
  * Hoja `Catalogo` — columnas conocidas (SKU, Titulo, Descripcion, Categoria,
    Marca, Precio_Costo, Precio_Final, Moneda, Stock, Activo) van a campos del
    modelo. Cualquier OTRA columna se guarda en `ficha_tecnica` (JSONB).
  * Hoja `Compatibilidades` — SKU + datos del vehículo. Para cada SKU se BORRAN
    las compatibilidades existentes y se reemplazan con las del Excel.
  * Vehículos se deduplican por la tupla completa (marca, modelo, motor, años).
"""

from __future__ import annotations

import io
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

import pandas as pd
from sqlalchemy import func as sql_func, or_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from .models import Producto, ProductoCompatibilidad, Vehiculo


# =============================================================
# Mapping de headers de Excel → campos del modelo
# =============================================================
# Las claves están normalizadas (lowercase, sin tildes ni espacios).
# Cualquier header de la hoja Catalogo que NO esté acá se guarda en
# `ficha_tecnica` con su nombre original (snake_case).

PRODUCTO_COL_ALIASES: dict[str, str] = {
    "sku": "sku",
    "codigo": "sku",
    "titulo": "titulo",
    "title": "titulo",
    "nombre": "titulo",
    "descripcion": "descripcion",
    "description": "descripcion",
    "categoria": "categoria",
    "category": "categoria",
    "rubro": "categoria",
    "marca": "marca",
    "brand": "marca",
    "precio_costo": "precio_costo",
    "costo": "precio_costo",
    "cost": "precio_costo",
    "precio_final": "precio_final",
    "precio": "precio_final",
    "price": "precio_final",
    "moneda": "moneda",
    "currency": "moneda",
    "stock": "stock_actual",
    "stock_actual": "stock_actual",
    "activo": "activo",
    "active": "activo",
}

COMPAT_COL_ALIASES: dict[str, str] = {
    "sku": "sku",
    "codigo": "sku",
    "marca_vehiculo": "marca",
    "marca": "marca",
    "modelo": "modelo",
    "model": "modelo",
    "combustible": "combustible",
    "fuel": "combustible",
    "cilindros": "cilindros",
    "cylinders": "cilindros",
    "valvulas": "valvulas",
    "valves": "valvulas",
    "cilindrada": "cilindrada_cc",
    "cilindrada_cc": "cilindrada_cc",
    "displacement": "cilindrada_cc",
    "anio_desde": "anio_desde",
    "ano_desde": "anio_desde",
    "year_from": "anio_desde",
    "anio_hasta": "anio_hasta",
    "ano_hasta": "anio_hasta",
    "year_to": "anio_hasta",
    "anio": "anio",  # un solo año → se aplica a desde y hasta
    "ano": "anio",
    "year": "anio",
    "notas": "notas",
    "notes": "notas",
}

PAGE_SIZE = 50


# =============================================================
# Helpers de parsing
# =============================================================

def _norm_col(s: Any) -> str:
    """Normaliza header de columna: lower, sin tildes, snake_case."""
    if s is None:
        return ""
    s = str(s).strip().lower()
    for a, b in {"á": "a", "é": "e", "í": "i", "ó": "o", "ú": "u", "ñ": "n"}.items():
        s = s.replace(a, b)
    return s.replace(" ", "_").replace("-", "_")


def _is_blank(v: Any) -> bool:
    if v is None:
        return True
    try:
        if pd.isna(v):
            return True
    except (TypeError, ValueError):
        pass
    return v == ""


def _parse_str(v: Any) -> Optional[str]:
    if _is_blank(v):
        return None
    return str(v).strip() or None


def _parse_int(v: Any) -> Optional[int]:
    if _is_blank(v):
        return None
    try:
        return int(float(v))
    except (ValueError, TypeError):
        return None


def _parse_decimal(v: Any) -> Optional[Decimal]:
    if _is_blank(v):
        return None
    try:
        return Decimal(str(v).strip().replace(",", "."))
    except (InvalidOperation, ValueError):
        return None


def _parse_bool(v: Any, default: bool = True) -> bool:
    if _is_blank(v):
        return default
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    return s not in ("0", "false", "no", "n", "off")


# =============================================================
# Resultado del upload (para mostrar al usuario)
# =============================================================

@dataclass
class UploadResult:
    productos_insertados: int = 0
    productos_actualizados: int = 0
    compats_creadas: int = 0
    vehiculos_creados: int = 0
    errores: list[str] = field(default_factory=list)

    @property
    def productos_total(self) -> int:
        return self.productos_insertados + self.productos_actualizados

    @property
    def ok(self) -> bool:
        return len(self.errores) == 0


# =============================================================
# Procesamiento: hoja Catalogo
# =============================================================

def _process_catalogo_sheet(db: Session, df: pd.DataFrame, result: UploadResult) -> None:
    if df.empty:
        return

    # Normalizar headers
    df = df.copy()
    df.columns = [_norm_col(c) for c in df.columns]

    # Mapear columnas conocidas a campos. Las extras van a ficha_tecnica.
    field_to_col: dict[str, str] = {}
    extra_cols: list[str] = []
    for col in df.columns:
        if not col:
            continue
        target = PRODUCTO_COL_ALIASES.get(col)
        if target:
            field_to_col[target] = col
        else:
            extra_cols.append(col)

    if "sku" not in field_to_col:
        result.errores.append("Hoja Catalogo: falta la columna SKU")
        return

    sku_col = field_to_col["sku"]

    # Construir filas para el upsert
    rows: list[dict] = []
    seen_skus: set[str] = set()
    for idx, row in df.iterrows():
        sku = _parse_str(row.get(sku_col))
        if not sku:
            result.errores.append(f"Catalogo fila {idx + 2}: SKU vacío, saltada")
            continue
        if sku in seen_skus:
            result.errores.append(f"Catalogo fila {idx + 2}: SKU duplicado en el Excel ({sku}), uso la última")
        seen_skus.add(sku)

        ficha: dict = {}
        for col in extra_cols:
            val = row.get(col)
            if _is_blank(val):
                continue
            # Mantener tipos numéricos donde se pueda; texto si no
            if isinstance(val, bool):
                ficha[col] = val
            elif isinstance(val, (int, float)):
                ficha[col] = val
            else:
                ficha[col] = str(val).strip()

        def _g(field: str) -> Any:
            col = field_to_col.get(field)
            return row.get(col) if col else None

        titulo = _parse_str(_g("titulo")) or sku
        rows.append({
            "sku": sku,
            "titulo": titulo,
            "descripcion": _parse_str(_g("descripcion")),
            "categoria": _parse_str(_g("categoria")),
            "marca": _parse_str(_g("marca")),
            "ficha_tecnica": ficha,
            "precio_costo": _parse_decimal(_g("precio_costo")),
            "precio_final": _parse_decimal(_g("precio_final")),
            "moneda": _parse_str(_g("moneda")) or "ARS",
            "stock_actual": _parse_int(_g("stock_actual")) or 0,
            "activo": _parse_bool(_g("activo")),
        })

    if not rows:
        return

    # Detectar cuáles ya existían (para distinguir insert vs update en el reporte)
    skus = [r["sku"] for r in rows]
    existing_skus = set(
        s for (s,) in db.execute(select(Producto.sku).where(Producto.sku.in_(skus))).all()
    )

    # UPSERT por chunks (Postgres limita a ~32K parámetros por statement)
    CHUNK = 500
    for i in range(0, len(rows), CHUNK):
        chunk = rows[i: i + CHUNK]
        stmt = pg_insert(Producto).values(chunk)
        stmt = stmt.on_conflict_do_update(
            index_elements=["sku"],
            set_={
                "titulo": stmt.excluded.titulo,
                "descripcion": stmt.excluded.descripcion,
                "categoria": stmt.excluded.categoria,
                "marca": stmt.excluded.marca,
                "ficha_tecnica": stmt.excluded.ficha_tecnica,
                "precio_costo": stmt.excluded.precio_costo,
                "precio_final": stmt.excluded.precio_final,
                "moneda": stmt.excluded.moneda,
                "stock_actual": stmt.excluded.stock_actual,
                "activo": stmt.excluded.activo,
                "updated_at": sql_func.now(),
            },
        )
        db.execute(stmt)

    db.commit()

    result.productos_insertados = len(rows) - len(existing_skus)
    result.productos_actualizados = len(existing_skus)


# =============================================================
# Procesamiento: hoja Compatibilidades
# =============================================================

def _get_or_create_vehiculo(
    db: Session,
    *,
    marca: str,
    modelo: str,
    combustible: Optional[str],
    cilindros: Optional[int],
    valvulas: Optional[int],
    cilindrada_cc: Optional[int],
    anio_desde: Optional[int],
    anio_hasta: Optional[int],
    cache: dict,
) -> tuple[Vehiculo, bool]:
    """Busca o crea un vehículo. Devuelve (vehiculo, was_created)."""
    key = (marca, modelo, combustible, cilindros, valvulas, cilindrada_cc, anio_desde, anio_hasta)
    if key in cache:
        return cache[key], False

    filters = [Vehiculo.marca == marca, Vehiculo.modelo == modelo]
    for col, val in [
        (Vehiculo.combustible, combustible),
        (Vehiculo.cilindros, cilindros),
        (Vehiculo.valvulas, valvulas),
        (Vehiculo.cilindrada_cc, cilindrada_cc),
        (Vehiculo.anio_desde, anio_desde),
        (Vehiculo.anio_hasta, anio_hasta),
    ]:
        filters.append(col.is_(None) if val is None else col == val)

    v = db.execute(select(Vehiculo).where(*filters)).scalar_one_or_none()
    if v is not None:
        cache[key] = v
        return v, False

    v = Vehiculo(
        marca=marca, modelo=modelo,
        combustible=combustible, cilindros=cilindros, valvulas=valvulas,
        cilindrada_cc=cilindrada_cc,
        anio_desde=anio_desde, anio_hasta=anio_hasta,
    )
    db.add(v)
    db.flush()  # para tener v.id
    cache[key] = v
    return v, True


def _process_compatibilidades_sheet(db: Session, df: pd.DataFrame, result: UploadResult) -> None:
    if df.empty:
        return

    df = df.copy()
    df.columns = [_norm_col(c) for c in df.columns]

    field_to_col: dict[str, str] = {}
    for col in df.columns:
        if not col:
            continue
        target = COMPAT_COL_ALIASES.get(col)
        if target:
            field_to_col[target] = col

    for required in ("sku", "marca", "modelo"):
        if required not in field_to_col:
            result.errores.append(
                f"Hoja Compatibilidades: falta columna '{required}' "
                "(necesita SKU, Marca_Vehiculo y Modelo como mínimo)"
            )
            return

    def _g(row: pd.Series, field: str) -> Any:
        col = field_to_col.get(field)
        return row.get(col) if col else None

    # Agrupar filas por SKU
    sku_to_rows: dict[str, list[dict]] = {}
    for idx, row in df.iterrows():
        sku = _parse_str(_g(row, "sku"))
        if not sku:
            continue
        marca = _parse_str(_g(row, "marca"))
        modelo = _parse_str(_g(row, "modelo"))
        if not marca or not modelo:
            result.errores.append(f"Compat fila {idx + 2}: marca o modelo vacío")
            continue

        anio_desde = _parse_int(_g(row, "anio_desde"))
        anio_hasta = _parse_int(_g(row, "anio_hasta"))
        if anio_desde is None and anio_hasta is None:
            anio = _parse_int(_g(row, "anio"))
            if anio is not None:
                anio_desde = anio_hasta = anio

        sku_to_rows.setdefault(sku, []).append({
            "marca": marca,
            "modelo": modelo,
            "combustible": _parse_str(_g(row, "combustible")),
            "cilindros": _parse_int(_g(row, "cilindros")),
            "valvulas": _parse_int(_g(row, "valvulas")),
            "cilindrada_cc": _parse_int(_g(row, "cilindrada_cc")),
            "anio_desde": anio_desde,
            "anio_hasta": anio_hasta,
            "notas": _parse_str(_g(row, "notas")),
        })

    if not sku_to_rows:
        return

    # Mapear SKUs → IDs (un solo query, evita N+1)
    skus = list(sku_to_rows.keys())
    sku_to_id = dict(
        db.execute(select(Producto.sku, Producto.id).where(Producto.sku.in_(skus))).all()
    )

    cache: dict = {}
    for sku, compats in sku_to_rows.items():
        producto_id = sku_to_id.get(sku)
        if producto_id is None:
            result.errores.append(f"Compat: SKU '{sku}' no existe en productos")
            continue

        # Borrar compats viejas para este producto (Excel es source of truth)
        db.query(ProductoCompatibilidad).filter(
            ProductoCompatibilidad.producto_id == producto_id
        ).delete(synchronize_session=False)

        for c in compats:
            v, created = _get_or_create_vehiculo(db, cache=cache, **{k: v for k, v in c.items() if k != "notas"})
            if created:
                result.vehiculos_creados += 1
            db.add(ProductoCompatibilidad(
                producto_id=producto_id,
                vehiculo_id=v.id,
                notas=c["notas"],
            ))
            result.compats_creadas += 1

    db.commit()


# =============================================================
# Orquestador
# =============================================================

def process_excel_upload(db: Session, file_bytes: bytes) -> UploadResult:
    """Lee el Excel, procesa Catalogo y Compatibilidades, devuelve un resumen."""
    result = UploadResult()

    try:
        sheets = pd.read_excel(io.BytesIO(file_bytes), sheet_name=None)
    except Exception as e:
        result.errores.append(f"No se pudo leer el Excel: {e}")
        return result

    # Buscar hojas (case-insensitive, sin tildes)
    sheet_lookup = {_norm_col(name): df for name, df in sheets.items()}
    cat_df = sheet_lookup.get("catalogo") or sheet_lookup.get("productos")
    compat_df = sheet_lookup.get("compatibilidades") or sheet_lookup.get("compat")

    if cat_df is None and compat_df is None:
        result.errores.append("El Excel no tiene hojas 'Catalogo' ni 'Compatibilidades'")
        return result

    if cat_df is not None:
        try:
            _process_catalogo_sheet(db, cat_df, result)
        except Exception as e:
            db.rollback()
            result.errores.append(f"Error procesando Catalogo: {type(e).__name__}: {e}")

    if compat_df is not None:
        try:
            _process_compatibilidades_sheet(db, compat_df, result)
        except Exception as e:
            db.rollback()
            result.errores.append(f"Error procesando Compatibilidades: {type(e).__name__}: {e}")

    return result


# =============================================================
# Listado y búsqueda (para el GET /catalogo)
# =============================================================

def list_productos(
    db: Session,
    search: str = "",
    page: int = 1,
) -> tuple[list[dict], int]:
    """
    Devuelve (productos, total). Cada producto incluye `compat_count`.
    `total` es el total de productos que matchean (para paginación).
    """
    # Subquery: cuántas compatibilidades tiene cada producto
    compat_count_sq = (
        select(
            ProductoCompatibilidad.producto_id.label("pid"),
            sql_func.count(ProductoCompatibilidad.id).label("n"),
        )
        .group_by(ProductoCompatibilidad.producto_id)
        .subquery()
    )

    base_q = (
        select(Producto, sql_func.coalesce(compat_count_sq.c.n, 0).label("compat_count"))
        .outerjoin(compat_count_sq, compat_count_sq.c.pid == Producto.id)
    )
    count_q = select(sql_func.count(Producto.id))

    if search and search.strip():
        like = f"%{search.strip()}%"
        cond = or_(
            Producto.sku.ilike(like),
            Producto.titulo.ilike(like),
            Producto.marca.ilike(like),
            Producto.categoria.ilike(like),
        )
        base_q = base_q.where(cond)
        count_q = count_q.where(cond)

    total = int(db.execute(count_q).scalar() or 0)

    page = max(1, page)
    base_q = (
        base_q
        .order_by(Producto.created_at.desc(), Producto.id.desc())
        .limit(PAGE_SIZE)
        .offset((page - 1) * PAGE_SIZE)
    )

    productos: list[dict] = []
    for prod, compat_count in db.execute(base_q).all():
        productos.append({
            "id": prod.id,
            "sku": prod.sku,
            "titulo": prod.titulo,
            "categoria": prod.categoria,
            "marca": prod.marca,
            "precio_final": prod.precio_final,
            "stock_actual": prod.stock_actual,
            "activo": prod.activo,
            "compat_count": int(compat_count or 0),
        })
    return productos, total


# =============================================================
# Generador de template Excel
# =============================================================

def generate_template() -> bytes:
    """Devuelve un Excel template con las 2 hojas y un par de filas de ejemplo."""
    output = io.BytesIO()

    catalogo_df = pd.DataFrame([
        {
            "SKU": "ARO-FORD-001",
            "Titulo": "Aro de pistón Ford Falcon 6cyl 1969-1985",
            "Descripcion": "Juego x6 aros de compresión, primera medida",
            "Categoria": "aro",
            "Marca": "ZEN",
            "Precio_Costo": 8500,
            "Precio_Final": 14900,
            "Moneda": "ARS",
            "Stock": 12,
            "Activo": "SI",
            "Diametro_mm": 75.0,
            "Espesor_mm": 1.5,
            "Material": "acero",
        },
        {
            "SKU": "STARTER-VW-002",
            "Titulo": "Burro de arranque VW 12V",
            "Descripcion": "9 dientes, rotación horaria",
            "Categoria": "starter",
            "Marca": "Bosch",
            "Precio_Costo": 32000,
            "Precio_Final": 58900,
            "Moneda": "ARS",
            "Stock": 4,
            "Activo": "SI",
            "Voltaje": 12,
            "Potencia_kW": 1.4,
            "Dientes": 9,
            "Rotacion": "horario",
        },
    ])

    compat_df = pd.DataFrame([
        {
            "SKU": "ARO-FORD-001",
            "Marca_Vehiculo": "Ford",
            "Modelo": "Falcon",
            "Combustible": "nafta",
            "Cilindros": 6,
            "Valvulas": 12,
            "Cilindrada_cc": 3000,
            "Anio_Desde": 1969,
            "Anio_Hasta": 1985,
            "Notas": "",
        },
        {
            "SKU": "STARTER-VW-002",
            "Marca_Vehiculo": "Volkswagen",
            "Modelo": "Gol",
            "Combustible": "nafta",
            "Cilindros": 4,
            "Valvulas": 8,
            "Cilindrada_cc": 1600,
            "Anio_Desde": 1995,
            "Anio_Hasta": 2008,
            "Notas": "",
        },
    ])

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        catalogo_df.to_excel(writer, sheet_name="Catalogo", index=False)
        compat_df.to_excel(writer, sheet_name="Compatibilidades", index=False)

    return output.getvalue()
