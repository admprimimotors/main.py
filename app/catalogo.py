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
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

import pandas as pd
from sqlalchemy import func as sql_func, or_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from .models import FotoProducto, Producto, ProductoCompatibilidad, Vehiculo


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
    # Vínculo con Mercado Libre (read-only por ahora, no sincroniza)
    "ml_item_id": "ml_item_id",
    "ml_id": "ml_item_id",
    "mlid": "ml_item_id",
    "item_id": "ml_item_id",
    "ml_permalink": "ml_permalink",
    "permalink": "ml_permalink",
    "ml_url": "ml_permalink",
    "ml_status": "ml_status",
    "estado_ml": "ml_status",
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


def _to_native(v: Any) -> Any:
    """
    Convierte valores numpy/pandas a tipos nativos de Python.
    Necesario porque numpy.int64 / numpy.float64 / pandas.Timestamp
    no son JSON-serializables y rompen al guardarse en JSONB.
    """
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    # numpy scalars (int64, float64, bool_) tienen .item() que devuelve nativo
    if hasattr(v, "item") and not isinstance(v, (str, bytes, list, dict, tuple)):
        try:
            v = v.item()
        except (ValueError, AttributeError, TypeError):
            pass
    # pandas Timestamp / datetime → ISO string (JSON-friendly)
    if hasattr(v, "isoformat") and not isinstance(v, str):
        try:
            return v.isoformat()
        except Exception:
            return str(v)
    return v


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
            val = _to_native(val)  # numpy.int64 / Timestamp → tipos nativos
            if val is None:
                continue
            if isinstance(val, str):
                val = val.strip()
                if not val:
                    continue
            ficha[col] = val

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
            "ml_item_id": _parse_str(_g("ml_item_id")),
            "ml_permalink": _parse_str(_g("ml_permalink")),
            "ml_status": _parse_str(_g("ml_status")),
        })

    if not rows:
        return

    # Detectar cuáles ya existían (para distinguir insert vs update en el reporte)
    skus = [r["sku"] for r in rows]
    existing_skus = set(
        s for (s,) in db.execute(select(Producto.sku).where(Producto.sku.in_(skus))).all()
    )

    # ON CONFLICT: solo actualizamos las columnas que vinieron en el Excel.
    # Si una columna NO está en el Excel, su valor existente en la DB se preserva.
    # Esto evita que un upload "parcial" pise datos (ej: subir solo Stock no
    # borra el ML_Item_ID existente).
    column_present = {
        "titulo": "titulo" in field_to_col,
        "descripcion": "descripcion" in field_to_col,
        "categoria": "categoria" in field_to_col,
        "marca": "marca" in field_to_col,
        "ficha_tecnica": len(extra_cols) > 0,
        "precio_costo": "precio_costo" in field_to_col,
        "precio_final": "precio_final" in field_to_col,
        "moneda": "moneda" in field_to_col,
        "stock_actual": "stock_actual" in field_to_col,
        "activo": "activo" in field_to_col,
        "ml_item_id": "ml_item_id" in field_to_col,
        "ml_permalink": "ml_permalink" in field_to_col,
        "ml_status": "ml_status" in field_to_col,
    }

    # UPSERT por chunks (Postgres limita a ~32K parámetros por statement)
    CHUNK = 500
    for i in range(0, len(rows), CHUNK):
        chunk = rows[i: i + CHUNK]
        stmt = pg_insert(Producto).values(chunk)
        set_clauses = {"updated_at": sql_func.now()}
        for col, present in column_present.items():
            if present:
                set_clauses[col] = getattr(stmt.excluded, col)
        stmt = stmt.on_conflict_do_update(
            index_elements=["sku"],
            set_=set_clauses,
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

    # Buscar hojas (case-insensitive, sin tildes).
    # OJO: no usar `or` con DataFrames — pandas no soporta evaluación truthy
    # ("ValueError: The truth value of a DataFrame is ambiguous").
    sheet_lookup = {_norm_col(name): df for name, df in sheets.items()}

    cat_df = sheet_lookup.get("catalogo")
    if cat_df is None:
        cat_df = sheet_lookup.get("productos")

    compat_df = sheet_lookup.get("compatibilidades")
    if compat_df is None:
        compat_df = sheet_lookup.get("compat")

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
            "ml_item_id": prod.ml_item_id,
            "ml_permalink": prod.ml_permalink,
        })
    return productos, total


# =============================================================
# Detalle de un producto (para GET /catalogo/{sku})
# =============================================================

def get_producto_detail(db: Session, sku: str) -> Optional[dict]:
    """
    Devuelve un dict con todos los datos del producto + compatibilidades
    + fotos. None si el SKU no existe.
    """
    prod = db.execute(
        select(Producto).where(Producto.sku == sku)
    ).scalar_one_or_none()
    if prod is None:
        return None

    # Compats con datos del vehículo (un solo query, ordenado por marca/modelo/año)
    compats_q = (
        select(ProductoCompatibilidad, Vehiculo)
        .join(Vehiculo, ProductoCompatibilidad.vehiculo_id == Vehiculo.id)
        .where(ProductoCompatibilidad.producto_id == prod.id)
        .order_by(
            Vehiculo.marca,
            Vehiculo.modelo,
            Vehiculo.anio_desde.nulls_last(),
        )
    )
    compats: list[dict] = []
    for pc, v in db.execute(compats_q).all():
        compats.append({
            "id": pc.id,
            "marca": v.marca,
            "modelo": v.modelo,
            "combustible": v.combustible,
            "cilindros": v.cilindros,
            "valvulas": v.valvulas,
            "cilindrada_cc": v.cilindrada_cc,
            "anio_desde": v.anio_desde,
            "anio_hasta": v.anio_hasta,
            "notas": pc.notas,
        })

    # Fotos (ya vienen ordenadas por `orden` por la relationship)
    fotos = [
        {"id": f.id, "url": f.url, "orden": f.orden}
        for f in prod.fotos
    ]

    return {
        "id": prod.id,
        "sku": prod.sku,
        "titulo": prod.titulo,
        "descripcion": prod.descripcion,
        "categoria": prod.categoria,
        "marca": prod.marca,
        "ficha_tecnica": prod.ficha_tecnica or {},
        "precio_costo": prod.precio_costo,
        "precio_final": prod.precio_final,
        "moneda": prod.moneda,
        "stock_actual": prod.stock_actual,
        "activo": prod.activo,
        "ml_item_id": prod.ml_item_id,
        "ml_permalink": prod.ml_permalink,
        "ml_status": prod.ml_status,
        "ml_stock": prod.ml_stock,
        "ml_precio": prod.ml_precio,
        "ml_last_synced_at": prod.ml_last_synced_at,
        "created_at": prod.created_at,
        "updated_at": prod.updated_at,
        "compatibilidades": compats,
        "fotos": fotos,
    }


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
            "ML_Item_ID": "MLAU3904630006",
            "ML_Permalink": "https://articulo.mercadolibre.com.ar/MLA-XXXXXX",
            "ML_Status": "active",
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
            "ML_Item_ID": "",
            "ML_Permalink": "",
            "ML_Status": "",
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


# =============================================================
# Fotos: upload / delete
# =============================================================

def add_foto(
    db: Session,
    sku: str,
    image_bytes: bytes,
    filename: str = "",
) -> tuple[bool, str]:
    """
    Optimiza la imagen, la sube a R2 y guarda el registro en la DB.
    Devuelve (ok, mensaje).
    """
    from . import storage  # import lazy para no requerir boto3 en tests

    if not storage.is_configured():
        return False, "Storage R2 no está configurado en las env vars"

    prod = db.execute(
        select(Producto).where(Producto.sku == sku)
    ).scalar_one_or_none()
    if prod is None:
        return False, f"Producto '{sku}' no existe"

    try:
        upload = storage.upload_photo(image_bytes, sku, filename)
    except Exception as e:
        return False, f"Error subiendo foto: {type(e).__name__}: {e}"

    # Orden = último + 1 (si no hay fotos, queda en 0)
    max_orden = db.execute(
        select(sql_func.coalesce(sql_func.max(FotoProducto.orden), -1))
        .where(FotoProducto.producto_id == prod.id)
    ).scalar()
    max_orden = -1 if max_orden is None else int(max_orden)

    db.add(FotoProducto(
        producto_id=prod.id,
        storage_key=upload["storage_key"],
        url=upload["url"],
        orden=max_orden + 1,
        bytes_size=upload["bytes_size"],
        width_px=upload["width_px"],
        height_px=upload["height_px"],
    ))
    db.commit()
    return True, "Foto subida correctamente"


def delete_foto(db: Session, foto_id: int) -> tuple[bool, str]:
    """
    Borra una foto de R2 (best-effort) y de la DB.
    Devuelve (ok, mensaje).
    """
    from . import storage

    foto = db.execute(
        select(FotoProducto).where(FotoProducto.id == foto_id)
    ).scalar_one_or_none()
    if foto is None:
        return False, "Foto no encontrada"

    # R2 borra es best-effort: si falla, igual borramos de la DB
    storage.delete_photo(foto.storage_key)

    db.delete(foto)
    db.commit()
    return True, "Foto eliminada"


# =============================================================
# Bulk linkeo SKU ↔ ML_Item_ID
# =============================================================

@dataclass
class MLLinkUploadResult:
    vinculados: int = 0
    sin_cambio: int = 0
    creados_placeholder: int = 0
    errores: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return len(self.errores) == 0


# Aliases específicos para el Excel de linkeo (más liberal que el master)
_ML_LINK_ALIASES = {
    "sku": "sku",
    "codigo": "sku",
    "ml_item_id": "ml_item_id",
    "item_id": "ml_item_id",
    "ml_id": "ml_item_id",
    "mlid": "ml_item_id",
    "ml_permalink": "ml_permalink",
    "permalink": "ml_permalink",
    "ml_url": "ml_permalink",
    "url": "ml_permalink",
}


def process_ml_link_upload(
    db: Session,
    file_bytes: bytes,
    crear_faltantes: bool = True,
) -> MLLinkUploadResult:
    """
    Procesa un Excel simple con SKU + ML_Item_ID (+ ML_Permalink opcional).
    Solo actualiza esos campos — no toca stock, precios, ficha, ni nada más.

    Si `crear_faltantes=True` (default), los SKUs que NO existen en el catálogo
    se crean como "placeholder": un Producto con titulo=SKU, stock=0, sin precios,
    pero CON el ml_item_id y ml_permalink seteados. Después el usuario completa
    los campos vacíos subiendo el Excel master normal — el upsert respeta los
    campos ya cargados (incluido el ML link).

    Si `crear_faltantes=False`, los SKUs faltantes se reportan como error y no
    se hace nada con ellos (modo estricto: validación contra el catálogo).
    """
    result = MLLinkUploadResult()

    try:
        sheets = pd.read_excel(io.BytesIO(file_bytes), sheet_name=None)
    except Exception as e:
        result.errores.append(f"No se pudo leer el Excel: {e}")
        return result

    # Buscar la primera hoja con columnas SKU + ML_Item_ID
    target_df = None
    for _name, df in sheets.items():
        df_copy = df.copy()
        df_copy.columns = [_norm_col(c) for c in df_copy.columns]
        cols = set(df_copy.columns)
        has_sku = any(c in cols for c in ("sku", "codigo"))
        has_ml = any(c in cols for c in ("ml_item_id", "item_id", "ml_id", "mlid"))
        if has_sku and has_ml:
            target_df = df_copy
            break

    if target_df is None:
        result.errores.append(
            "Ninguna hoja tiene columnas SKU y ML_Item_ID"
        )
        return result

    # Mapear columnas presentes a campos
    field_to_col: dict[str, str] = {}
    for col in target_df.columns:
        if not col:
            continue
        target = _ML_LINK_ALIASES.get(col)
        if target:
            field_to_col[target] = col

    sku_col = field_to_col["sku"]
    ml_id_col = field_to_col["ml_item_id"]
    ml_link_col = field_to_col.get("ml_permalink")

    # Recolectar updates
    updates: list[dict] = []
    for idx, row in target_df.iterrows():
        sku = _parse_str(row.get(sku_col))
        ml_item_id = _parse_str(row.get(ml_id_col))
        if not sku:
            continue
        if not ml_item_id:
            result.errores.append(f"Fila {idx + 2} (SKU {sku}): ML_Item_ID vacío")
            continue
        permalink = _parse_str(row.get(ml_link_col)) if ml_link_col else None
        updates.append({
            "sku": sku,
            "ml_item_id": ml_item_id,
            "ml_permalink": permalink,
        })

    if not updates:
        return result

    # Validar qué SKUs existen
    skus = [u["sku"] for u in updates]
    existing_map = dict(
        db.execute(
            select(Producto.sku, Producto.id).where(Producto.sku.in_(skus))
        ).all()
    )

    # Update / create SKU por SKU
    for u in updates:
        if u["sku"] not in existing_map:
            if crear_faltantes:
                # Crear placeholder con datos mínimos. Lo demás (título real,
                # precios, stock, descripción, ficha técnica, compatibilidades)
                # se completa después al subir el Excel master normal.
                placeholder = Producto(
                    sku=u["sku"],
                    titulo=u["sku"],     # placeholder: titulo == sku, fácil de identificar
                    ficha_tecnica={},
                    moneda="ARS",
                    stock_actual=0,
                    activo=True,
                    ml_item_id=u["ml_item_id"],
                    ml_permalink=u["ml_permalink"],
                )
                db.add(placeholder)
                result.creados_placeholder += 1
            else:
                result.errores.append(f"SKU '{u['sku']}' no existe en el catálogo")
            continue

        # SKU existe — actualizar solo si hay cambios
        prod = db.execute(
            select(Producto).where(Producto.sku == u["sku"])
        ).scalar_one_or_none()
        if prod is None:
            continue

        cambio = False
        if prod.ml_item_id != u["ml_item_id"]:
            prod.ml_item_id = u["ml_item_id"]
            cambio = True
        if u["ml_permalink"] is not None and prod.ml_permalink != u["ml_permalink"]:
            prod.ml_permalink = u["ml_permalink"]
            cambio = True

        if cambio:
            result.vinculados += 1
        else:
            result.sin_cambio += 1

    db.commit()
    return result


def generate_ml_link_template() -> bytes:
    """Excel template simple para el bulk linkeo — 3 columnas."""
    output = io.BytesIO()
    df = pd.DataFrame([
        {
            "SKU": "ZE0024",
            "ML_Item_ID": "MLAU3904630006",
            "ML_Permalink": "https://articulo.mercadolibre.com.ar/MLA-XXXXXX",
        },
        {
            "SKU": "ZEN1741",
            "ML_Item_ID": "MLAU1234567890",
            "ML_Permalink": "",
        },
    ])
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="ML_Links", index=False)
    return output.getvalue()


# =============================================================
# Sync desde ML (read-only)
# =============================================================

def sync_producto_from_ml(db: Session, sku: str) -> tuple[bool, str]:
    """
    Pulla datos frescos del item en ML y los guarda como snapshot
    (ml_stock, ml_precio, ml_status, ml_permalink, ml_last_synced_at).

    NO modifica stock_actual ni precio_final locales — solo registra
    "esto es lo que ML reporta hoy" para que se vea drift en el panel.

    Devuelve (ok, mensaje).
    """
    from . import ml_client

    if not ml_client.is_configured():
        return False, "ML no está configurado (faltan env vars ML_*)"

    prod = db.execute(
        select(Producto).where(Producto.sku == sku)
    ).scalar_one_or_none()
    if prod is None:
        return False, f"SKU '{sku}' no existe"
    if not prod.ml_item_id:
        return False, "Este SKU no tiene ML_Item_ID vinculado"

    try:
        item = ml_client.get_item(db, prod.ml_item_id)
    except ml_client.MLClientError as e:
        return False, f"Error consultando ML: {e}"

    # Extraer campos
    prod.ml_status = item.get("status")
    prod.ml_permalink = item.get("permalink") or prod.ml_permalink
    aq = item.get("available_quantity")
    if aq is not None:
        try:
            prod.ml_stock = int(aq)
        except (ValueError, TypeError):
            pass
    price = item.get("price")
    if price is not None:
        try:
            prod.ml_precio = Decimal(str(price))
        except Exception:
            pass
    prod.ml_last_synced_at = datetime.now(timezone.utc)

    db.commit()

    # Detectar drift y armar mensaje informativo
    drift_bits = []
    if prod.ml_stock is not None and prod.ml_stock != prod.stock_actual:
        drift_bits.append(f"stock DB={prod.stock_actual} ML={prod.ml_stock}")
    if (
        prod.ml_precio is not None
        and prod.precio_final is not None
        and prod.ml_precio != prod.precio_final
    ):
        drift_bits.append(
            f"precio DB=${prod.precio_final:,.0f} ML=${prod.ml_precio:,.0f}"
        )

    msg = f"✓ Sync OK · estado ML: {prod.ml_status or '?'}"
    if drift_bits:
        msg += " · ⚠ drift: " + ", ".join(drift_bits)

    return True, msg
