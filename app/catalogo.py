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
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

import pandas as pd
import requests
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
    # Costos variables del producto en ML (envío y % impuestos)
    "ml_envio_fijo": "ml_envio_fijo",
    "envio_fijo": "ml_envio_fijo",
    "envio": "ml_envio_fijo",
    "ml_impuestos_pct": "ml_impuestos_pct",
    "impuestos_pct": "ml_impuestos_pct",
    "impuestos": "ml_impuestos_pct",
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


# -----------------------------------------------------------------
# Helpers para procesar atributos / sale_terms de Mercado Libre
# -----------------------------------------------------------------

# Atributos ML que mapeamos a campos dedicados del producto (no a ficha_tecnica).
# Si en ML hay BRAND, se guarda en producto.marca (no en la ficha).
_ML_ATTR_TO_FIELD: dict[str, str] = {
    "BRAND": "marca",
}


def _norm_attr_key(name: str) -> str:
    """Normaliza un nombre de atributo a snake_case ASCII para usar como key del JSONB."""
    if not name:
        return ""
    s = str(name).strip().lower()
    for a, b in {"á": "a", "é": "e", "í": "i", "ó": "o", "ú": "u", "ñ": "n"}.items():
        s = s.replace(a, b)
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    return s


def _attr_value_str(attr: dict) -> str:
    """
    Extrae el valor legible de un atributo ML.
    ML puede entregar el valor en varios shapes:
      - value_name: string "Horario" o "30 cm"
      - value_struct: {number, unit} para medidas
      - value_id: id de la lista (fallback si no hay name)
    """
    val = attr.get("value_name")
    if val:
        return str(val).strip()
    struct = attr.get("value_struct") or {}
    num = struct.get("number")
    if num is not None:
        unit = (struct.get("unit") or "").strip()
        return f"{num} {unit}".strip()
    return str(attr.get("value_id") or "").strip()


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
            "ml_envio_fijo": _parse_decimal(_g("ml_envio_fijo")),
            "ml_impuestos_pct": _parse_decimal(_g("ml_impuestos_pct")),
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
        "ml_envio_fijo": "ml_envio_fijo" in field_to_col,
        "ml_impuestos_pct": "ml_impuestos_pct" in field_to_col,
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

def skus_oldest_matching(
    db: Session,
    *,
    search: str = "",
    vinculadas: str = "",
    categoria: str = "",
    marca: str = "",
    limit: int = 50,
    only_linked: bool = False,
) -> list[str]:
    """
    Devuelve SKUs que matchean los filtros, ordenados por ml_last_synced_at ASC
    NULLS FIRST (los nunca sincronizados primero, después los más viejos).

    Si `only_linked=True`, además filtra solo los que tienen ml_item_id.
    Útil para bulk hidratar (no tiene sentido hidratar lo que no está vinculado)
    y bulk push (idem).
    """
    q = select(Producto.sku)

    extra_conds = []
    if search and search.strip():
        like = f"%{search.strip()}%"
        extra_conds.append(or_(
            Producto.sku.ilike(like),
            Producto.titulo.ilike(like),
            Producto.marca.ilike(like),
            Producto.categoria.ilike(like),
        ))
    if vinculadas == "si":
        extra_conds.append(Producto.ml_item_id.is_not(None))
    elif vinculadas == "no":
        extra_conds.append(Producto.ml_item_id.is_(None))
    if categoria:
        extra_conds.append(Producto.categoria == categoria)
    if marca:
        extra_conds.append(Producto.marca == marca)
    if only_linked:
        extra_conds.append(Producto.ml_item_id.is_not(None))

    for cond in extra_conds:
        q = q.where(cond)

    q = q.order_by(Producto.ml_last_synced_at.asc().nulls_first()).limit(limit)
    return [s for (s,) in db.execute(q).all()]


def list_categorias(db: Session) -> list[str]:
    """Lista de categorías distintas, no-nulas, ordenadas alfabéticamente."""
    rows = db.execute(
        select(Producto.categoria)
        .distinct()
        .where(Producto.categoria.is_not(None))
        .order_by(Producto.categoria)
    ).all()
    return [r[0] for r in rows if r[0]]


def list_marcas(db: Session) -> list[str]:
    """Lista de marcas distintas, no-nulas, ordenadas alfabéticamente."""
    rows = db.execute(
        select(Producto.marca)
        .distinct()
        .where(Producto.marca.is_not(None))
        .order_by(Producto.marca)
    ).all()
    return [r[0] for r in rows if r[0]]


def list_productos(
    db: Session,
    search: str = "",
    page: int = 1,
    vinculadas: str = "",   # "si" → solo con ml_item_id, "no" → solo sin, "" → todas
    categoria: str = "",
    marca: str = "",
    rentabilidad: str = "", # "below" → solo bajo el ideal, "ok" → solo arriba, "" → todas
) -> tuple[list[dict], int]:
    """
    Devuelve (productos, total). Cada producto incluye `compat_count` y
    `below_ideal` (boolean: True si precio_final < precio_ideal_ML).

    `total` es el total de productos que matchean los filtros (para paginación).
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

    # Aplicar filtros — cada uno se aplica a base_q y count_q en paralelo
    extra_conds = []

    if search and search.strip():
        like = f"%{search.strip()}%"
        extra_conds.append(or_(
            Producto.sku.ilike(like),
            Producto.titulo.ilike(like),
            Producto.marca.ilike(like),
            Producto.categoria.ilike(like),
        ))

    if vinculadas == "si":
        extra_conds.append(Producto.ml_item_id.is_not(None))
    elif vinculadas == "no":
        extra_conds.append(Producto.ml_item_id.is_(None))

    if categoria:
        extra_conds.append(Producto.categoria == categoria)

    if marca:
        extra_conds.append(Producto.marca == marca)

    # Filtro por rentabilidad ML: precio_final vs precio_ideal calculado en SQL
    # Fórmula: precio_ideal = (costo*(1+obj/100) + COALESCE(envio, default_envio))
    #                       / (1 - (com + cuotas + COALESCE(imp, default_imp))/100)
    # con com = COALESCE(ml_comision_pct, default_com).
    if rentabilidad in ("below", "ok"):
        from . import precios as _precios
        cfg = _precios.get_ml_fees_config()
        margen_factor = (Decimal("1") + cfg["margen_objetivo_pct"] / Decimal("100"))
        envio_default = cfg["envio_default"]
        com_default = cfg["comision_pct"]
        cuotas_pct = cfg["cuotas_pct"]
        imp_default = cfg["impuestos_pct_default"]

        # Precio ideal calculado en SQL (NUMERIC arithmetic en Postgres es exacto)
        precio_ideal_sql = (
            (Producto.precio_costo * margen_factor
             + sql_func.coalesce(Producto.ml_envio_fijo, envio_default))
            / (Decimal("1") - (
                sql_func.coalesce(Producto.ml_comision_pct, com_default)
                + cuotas_pct
                + sql_func.coalesce(Producto.ml_impuestos_pct, imp_default)
            ) / Decimal("100"))
        )
        extra_conds.append(Producto.precio_costo.is_not(None))
        extra_conds.append(Producto.precio_final.is_not(None))
        extra_conds.append(Producto.precio_costo > 0)
        if rentabilidad == "below":
            extra_conds.append(Producto.precio_final < precio_ideal_sql)
        else:  # "ok"
            extra_conds.append(Producto.precio_final >= precio_ideal_sql)

    for cond in extra_conds:
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

    from . import precios as _precios

    productos: list[dict] = []
    for prod, compat_count in db.execute(base_q).all():
        # Computar rentabilidad para mostrar el indicador en la lista.
        # Solo si hay precio_costo y precio_final, sino no hay nada que comparar.
        below_ideal: Optional[bool] = None
        if prod.precio_costo is not None and prod.precio_final is not None and prod.precio_costo > 0:
            r = _precios.analyze_rentabilidad_ml(
                precio_costo=prod.precio_costo,
                precio_final=prod.precio_final,
                envio_fijo_producto=prod.ml_envio_fijo,
                impuestos_pct_producto=prod.ml_impuestos_pct,
                comision_pct_producto=prod.ml_comision_pct,
            )
            if r.precio_ideal is not None:
                below_ideal = bool(prod.precio_final < r.precio_ideal)
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
            "below_ideal": below_ideal,
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
        "ml_envio_fijo": prod.ml_envio_fijo,
        "ml_impuestos_pct": prod.ml_impuestos_pct,
        "ml_comision_pct": prod.ml_comision_pct,
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

    # Deduplicar por SKU (si el Excel tiene repeticiones, mantener la última).
    # Sin esto, dos rows con el mismo SKU intentarían un INSERT duplicado y
    # toda la transacción rollbearía por violación de unique constraint.
    seen_skus: dict[str, dict] = {}
    for u in updates:
        seen_skus[u["sku"]] = u
    updates = list(seen_skus.values())

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

def _q_placeholders_pendientes(db: Session):
    """
    Sub-query: productos con ML link pero al menos un dato por hidratar.
    Criterios de "pendiente":
      - titulo igual al sku (placeholder de linkeo)
      - precio_final NULL
      - categoria NULL
      - sin fotos en DB
    Cualquiera de las 4 lo marca como pendiente.
    """
    # Productos que SÍ tienen fotos
    con_fotos = select(FotoProducto.producto_id).distinct()

    return (
        select(Producto)
        .where(Producto.ml_item_id.is_not(None))
        .where(
            or_(
                Producto.titulo == Producto.sku,
                Producto.precio_final.is_(None),
                Producto.categoria.is_(None),
                ~Producto.id.in_(con_fotos),
            )
        )
    )


def count_placeholders_pendientes(db: Session) -> int:
    """Cuántos productos tienen al menos un dato por hidratar."""
    base = _q_placeholders_pendientes(db).subquery()
    return int(db.execute(
        select(sql_func.count()).select_from(base)
    ).scalar() or 0)


def hidratar_batch_placeholders(db: Session, limit: int = 5) -> dict:
    """
    Hidrata los próximos N placeholders pendientes (los más antiguos primero).
    Devuelve dict con conteos para que el frontend pueda hacer loop:
      - processed: cantidad hidratada en este batch
      - remaining: cuántos quedan pendientes después
      - done: True si no quedan más
      - errors: lista de errores (sku: msg)
      - skus_done: lista de SKUs procesados (para mostrar en UI)
    """
    q = (
        _q_placeholders_pendientes(db)
        .order_by(Producto.ml_last_synced_at.asc().nulls_first())
        .limit(limit)
    )
    skus = [p.sku for p in db.execute(q).scalars().all()]

    if not skus:
        return {
            "processed": 0,
            "remaining": 0,
            "done": True,
            "errors": [],
            "skus_done": [],
        }

    processed = 0
    errors: list[str] = []
    skus_done: list[str] = []
    for sku in skus:
        try:
            ok, msg = sync_producto_from_ml(db, sku, hidratar=True)
        except Exception as e:
            ok = False
            msg = f"{type(e).__name__}: {e}"
        if ok:
            processed += 1
            skus_done.append(sku)
        else:
            errors.append(f"{sku}: {msg}")

    remaining = count_placeholders_pendientes(db)
    return {
        "processed": processed,
        "remaining": remaining,
        "done": remaining == 0,
        "errors": errors,
        "skus_done": skus_done,
    }


def bulk_edit_skus(
    db: Session,
    skus: list[str],
    campo: str,
    valor: Any,
) -> tuple[int, list[str]]:
    """
    Aplica un mismo valor a un campo de N productos seleccionados.
    Campos permitidos: categoria, marca, moneda, activo.

    Devuelve (cantidad_aplicada, lista_de_errores).
    """
    ALLOWED = {"categoria", "marca", "moneda", "activo"}
    if campo not in ALLOWED:
        return 0, [f"Campo '{campo}' no permitido para bulk edit (permitidos: {', '.join(sorted(ALLOWED))})"]

    if not skus:
        return 0, ["No hay SKUs seleccionados"]

    # Normalizar valor según campo
    if campo == "activo":
        if isinstance(valor, bool):
            valor_final = valor
        else:
            valor_final = str(valor or "").strip().lower() in ("on", "true", "1", "yes", "si", "sí")
    elif campo == "moneda":
        v = str(valor or "").strip().upper()
        if v not in ("ARS", "USD"):
            return 0, [f"Moneda inválida: '{v}'. Usá ARS o USD."]
        valor_final = v
    else:
        # categoria / marca → string o None si vacío
        v = str(valor or "").strip()
        valor_final = v[:80] if v else None

    aplicados = 0
    errores: list[str] = []
    for sku in skus:
        prod = db.execute(
            select(Producto).where(Producto.sku == sku)
        ).scalar_one_or_none()
        if prod is None:
            errores.append(f"SKU '{sku}' no existe")
            continue
        setattr(prod, campo, valor_final)
        aplicados += 1

    db.commit()
    return aplicados, errores


def bulk_sync_oldest(db: Session, limit: int = 50) -> tuple[int, int, list[str]]:
    """
    Sincroniza los N productos vinculados con sync más antiguo (o nunca sync'd).
    Devuelve (ok, total_intentados, lista_de_errores).

    Estrategia: NULLS FIRST en ASC sobre ml_last_synced_at — primero los que
    nunca se sincronizaron, después los más viejos. Así cada click va atacando
    los más desactualizados.
    """
    from . import ml_client

    if not ml_client.is_configured():
        return 0, 0, ["ML no está configurado (faltan env vars ML_*)"]

    # Tomamos solo los SKUs (no el objeto entero, lo carga sync_producto_from_ml)
    q = (
        select(Producto.sku)
        .where(Producto.ml_item_id.is_not(None))
        .order_by(Producto.ml_last_synced_at.asc().nulls_first())
        .limit(limit)
    )
    skus = [s for (s,) in db.execute(q).all()]

    if not skus:
        return 0, 0, []

    ok = 0
    errors: list[str] = []
    for sku in skus:
        try:
            # Bulk sync = solo snapshot, sin descargar fotos ni tocar campos
            # locales. Si querés hidratar un placeholder, usá el botón individual.
            success, msg = sync_producto_from_ml(db, sku, hidratar=False)
        except Exception as e:
            success = False
            msg = f"{type(e).__name__}: {e}"
        if success:
            ok += 1
        else:
            errors.append(f"{sku}: {msg}")

    return ok, len(skus), errors


def update_producto_basic(
    db: Session,
    sku: str,
    *,
    titulo: Optional[str] = None,
    descripcion: Optional[str] = None,
    categoria: Optional[str] = None,
    marca: Optional[str] = None,
    precio_costo: Optional[Decimal] = None,
    precio_final: Optional[Decimal] = None,
    moneda: Optional[str] = None,
    activo: Optional[bool] = None,
    ml_envio_fijo: Optional[Decimal] = None,
    ml_impuestos_pct: Optional[Decimal] = None,
    update_envio: bool = False,
    update_impuestos: bool = False,
) -> tuple[bool, str, dict]:
    """
    Actualiza los campos básicos de un producto desde el form de edición.
    Devuelve (ok, mensaje, dict_de_cambios).

    `dict_de_cambios` es {field: (old, new)} — útil para que el caller decida
    si hace falta auto-pushear a ML (ej: si cambió el precio).
    """
    prod = db.execute(
        select(Producto).where(Producto.sku == sku)
    ).scalar_one_or_none()
    if prod is None:
        return False, f"SKU '{sku}' no existe", {}

    cambios: dict = {}

    def _set(field: str, new_val):
        old = getattr(prod, field)
        if old != new_val:
            cambios[field] = (old, new_val)
            setattr(prod, field, new_val)

    if titulo is not None:
        if not titulo.strip():
            return False, "El título no puede quedar vacío", {}
        _set("titulo", titulo.strip())
    if descripcion is not None:
        _set("descripcion", descripcion.strip() or None)
    if categoria is not None:
        _set("categoria", categoria.strip() or None)
    if marca is not None:
        _set("marca", marca.strip() or None)
    if precio_costo is not None:
        _set("precio_costo", precio_costo if precio_costo != "" else None)
    if precio_final is not None:
        _set("precio_final", precio_final if precio_final != "" else None)
    if moneda is not None:
        _set("moneda", (moneda.strip().upper() or "ARS")[:3])
    if activo is not None:
        _set("activo", bool(activo))
    # Para envío y impuestos usamos un flag explícito porque el "valor None"
    # significa "limpiar" (volver al default global), no "no tocar".
    if update_envio:
        _set("ml_envio_fijo", ml_envio_fijo)
    if update_impuestos:
        _set("ml_impuestos_pct", ml_impuestos_pct)

    if not cambios:
        return True, "Sin cambios", {}

    db.commit()
    return True, f"✓ {len(cambios)} campo{'' if len(cambios) == 1 else 's'} actualizado{'' if len(cambios) == 1 else 's'}", cambios


def _diff_attributes_for_push(
    raw_attributes: list,
    ficha_tecnica: dict,
) -> list[dict]:
    """
    Compara los valores actuales en ficha_tecnica con los raw_attributes
    originales de ML. Devuelve la lista de atributos que cambiaron, en formato
    {id, value_name} listo para mandar a `PUT /items/{id}`.

    Estrategia:
      - Por cada raw_attr, calculamos su key normalizada (igual que el sync hace)
      - Buscamos esa key en ficha_tecnica — si está y difiere del value_name
        original, es un cambio para pushear
      - Solo se incluyen atributos que ya existían en ML (no atributos custom
        que el usuario sumó a la ficha localmente — esos no tienen ID en ML)
    """
    if not raw_attributes:
        return []
    ficha = ficha_tecnica or {}
    cambios: list[dict] = []
    for raw in raw_attributes:
        attr_id = (raw.get("id") or "").strip()
        if not attr_id:
            continue
        attr_name = raw.get("name") or attr_id
        key = _norm_attr_key(attr_name) or _norm_attr_key(attr_id)
        if not key or key not in ficha:
            continue
        original_value = _attr_value_str(raw)
        current_value = ficha.get(key)
        if current_value is None:
            continue
        # Comparar como strings (ficha guarda strings normalmente)
        if str(current_value).strip() == str(original_value).strip():
            continue
        cambios.append({
            "id": attr_id,
            "value_name": str(current_value).strip(),
        })
    return cambios


def push_to_ml(
    db: Session,
    sku: str,
    *,
    push_stock: bool = True,
    push_price: bool = True,
    push_description: bool = False,
    push_attributes: bool = False,
) -> tuple[bool, str]:
    """
    Empuja stock / precio / descripción / atributos del DB local a la
    publicación de ML. Solo si write sync está habilitado.

    Para `push_attributes`: solo se mandan atributos cuyo valor en
    `ficha_tecnica` difiere del value_name original que ML reportó.
    Atributos nuevos (sin ID de ML) no se pushean — los ignora.

    Devuelve (ok, mensaje).
    """
    from . import ml_client

    if not ml_client.is_write_enabled():
        return False, (
            "Write sync ML deshabilitado. "
            "Para activar, seteá ML_SYNC_WRITE_ENABLED=true en Render."
        )

    prod = db.execute(
        select(Producto).where(Producto.sku == sku)
    ).scalar_one_or_none()
    if prod is None:
        return False, f"SKU '{sku}' no existe"
    if not prod.ml_item_id:
        return False, "Producto no vinculado a ML (sin ml_item_id)"

    # Calcular cambios de atributos antes (para saber si hay que pushear)
    attr_changes: list[dict] = []
    if push_attributes:
        attr_changes = _diff_attributes_for_push(
            prod.ml_raw_attributes or [], prod.ficha_tecnica or {}
        )

    # Decidir qué pushear según los flags y los datos disponibles
    actions = []
    if push_stock:
        actions.append("stock")
    if push_price and prod.precio_final is not None:
        actions.append("precio")
    if push_description and (prod.descripcion or "").strip():
        actions.append("descripcion")
    if push_attributes and attr_changes:
        actions.append("atributos")

    if not actions:
        return False, "Nada para pushear (sin cambios o flags desactivados)"

    msgs: list[str] = []
    errors: list[str] = []

    if "stock" in actions:
        try:
            ml_client.update_item_stock(db, prod.ml_item_id, prod.stock_actual)
            prod.ml_stock = prod.stock_actual
            msgs.append(f"stock={prod.stock_actual}")
        except ml_client.MLClientError as e:
            errors.append(f"stock falló: {e}")

    if "precio" in actions:
        try:
            ml_client.update_item_price(db, prod.ml_item_id, prod.precio_final)
            prod.ml_precio = prod.precio_final
            msgs.append(f"precio=${prod.precio_final:,.0f}")
        except ml_client.MLClientError as e:
            errors.append(f"precio falló: {e}")

    if "descripcion" in actions:
        try:
            ml_client.update_item_description(
                db, prod.ml_item_id, (prod.descripcion or "").strip()
            )
            msgs.append("descripción")
        except ml_client.MLClientError as e:
            errors.append(f"descripción falló: {e}")

    if "atributos" in actions:
        try:
            ml_client.update_item_attributes(db, prod.ml_item_id, attr_changes)
            msgs.append(f"{len(attr_changes)} atributos")
            # Refrescamos los raw_attributes con los nuevos values (parche optimista)
            updated_raw = list(prod.ml_raw_attributes or [])
            changes_by_id = {c["id"]: c["value_name"] for c in attr_changes}
            for r in updated_raw:
                if r.get("id") in changes_by_id:
                    r["value_name"] = changes_by_id[r["id"]]
                    r.pop("value_struct", None)  # ML resolverá
                    r.pop("value_id", None)
            prod.ml_raw_attributes = updated_raw
        except ml_client.MLClientError as e:
            errors.append(f"atributos fallaron: {e}")

    if msgs:
        prod.ml_last_synced_at = datetime.now(timezone.utc)
    db.commit()

    if not errors:
        return True, f"↑ ML: {', '.join(msgs)}"
    if msgs:
        return False, f"↑ ML parcial: {', '.join(msgs)} · {' · '.join(errors)}"
    return False, " · ".join(errors)


def _download_ml_photo(
    db: Session,
    producto_id: int,
    sku: str,
    url: str,
    orden: int,
) -> tuple[bool, str]:
    """
    Descarga una foto desde una URL pública (ej: ML CDN), la optimiza con
    Pillow y la sube a R2. Crea el registro FotoProducto.
    """
    from . import storage as storage_mod

    if not storage_mod.is_configured():
        return False, "R2 no configurado"

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
    except Exception as e:
        return False, f"Error descargando: {e}"

    filename = url.rsplit("/", 1)[-1] or "ml-foto.jpg"

    try:
        upload = storage_mod.upload_photo(response.content, sku, filename)
    except Exception as e:
        return False, f"Error subiendo a R2: {e}"

    db.add(FotoProducto(
        producto_id=producto_id,
        storage_key=upload["storage_key"],
        url=upload["url"],
        orden=orden,
        bytes_size=upload["bytes_size"],
        width_px=upload["width_px"],
        height_px=upload["height_px"],
    ))
    return True, ""


def sync_producto_from_ml(
    db: Session,
    sku: str,
    *,
    hidratar: bool = True,
) -> tuple[bool, str]:
    """
    Pulla datos frescos del item en ML.

    Siempre actualiza el SNAPSHOT (ml_stock, ml_precio, ml_status, ml_permalink,
    ml_last_synced_at) — útil para detectar drift entre DB y ML.

    Si `hidratar=True` (default para sync individual), además **completa
    campos vacíos del producto local** desde ML:
      - titulo: si está como placeholder (titulo == sku), usa el título de ML
      - precio_final: si está en None, usa el precio de ML
      - categoria: si está en None, usa el nombre de la categoría de ML
      - fotos: si no hay fotos cargadas en R2, descarga las de ML (max 10)

    Si `hidratar=False` (usado por bulk_sync_oldest), solo snapshot — sin tocar
    campos locales ni descargar fotos. Es ~50x más rápido.

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

    # ---- Snapshot (siempre) ----
    prod.ml_status = item.get("status")
    prod.ml_permalink = item.get("permalink") or prod.ml_permalink
    aq = item.get("available_quantity")
    if aq is not None:
        try:
            prod.ml_stock = int(aq)
        except (ValueError, TypeError):
            pass
    price = item.get("price")
    ml_price_decimal: Optional[Decimal] = None
    if price is not None:
        try:
            ml_price_decimal = Decimal(str(price))
            prod.ml_precio = ml_price_decimal
        except Exception:
            pass
    prod.ml_last_synced_at = datetime.now(timezone.utc)

    # ---- Hidratación (solo si hidratar=True) ----
    hidratado: list[str] = []

    if hidratar:
        # Título: si es placeholder (titulo == sku), reemplazar con el de ML
        if prod.titulo == sku and item.get("title"):
            prod.titulo = str(item["title"]).strip()[:500]
            hidratado.append("título")

        # Precio final: si está en None, usar precio de ML
        if prod.precio_final is None and ml_price_decimal is not None:
            prod.precio_final = ml_price_decimal
            hidratado.append("precio")

        # Categoría: si está en None, traer nombre de la categoría desde ML
        if not prod.categoria and item.get("category_id"):
            cat_info = ml_client.get_category(db, item["category_id"])
            cat_name = cat_info.get("name")
            if cat_name:
                prod.categoria = str(cat_name)[:80]
                hidratado.append("categoría")

        # Comisión ML real: usar /sites/MLA/listing_prices con la categoría +
        # listing_type_id del item para obtener el sale_fee_amount exacto.
        # Convertimos a % efectivo y guardamos. Sobrescribimos siempre (la
        # comisión cambia con cambios de tarifa de ML, queremos data fresca).
        if (
            item.get("category_id")
            and item.get("listing_type_id")
            and ml_price_decimal is not None
            and ml_price_decimal > 0
        ):
            lp = ml_client.get_listing_prices(
                db,
                price=float(ml_price_decimal),
                category_id=item["category_id"],
                listing_type_id=item["listing_type_id"],
            )
            sale_fee = lp.get("sale_fee_amount")
            if sale_fee is not None:
                try:
                    com_pct = (
                        Decimal(str(sale_fee)) / ml_price_decimal * Decimal("100")
                    ).quantize(Decimal("0.01"))
                    if prod.ml_comision_pct != com_pct:
                        prod.ml_comision_pct = com_pct
                        hidratado.append(f"comisión {com_pct}%")
                except Exception:
                    pass

        # Envío: si la publicación NO tiene free_shipping, el seller no paga
        # envío → ml_envio_fijo = 0. Si tiene free_shipping, ML no expone el
        # costo seller en una API limpia, lo dejamos como está (manual).
        shipping_info = item.get("shipping") or {}
        if shipping_info.get("free_shipping") is False:
            if prod.ml_envio_fijo != Decimal("0"):
                prod.ml_envio_fijo = Decimal("0")
                hidratado.append("envío $0 (paga el comprador)")

        # Descripción: endpoint separado en ML. Pull si la descripción local
        # está vacía — preservamos manual edits.
        if not (prod.descripcion or "").strip():
            desc_data = ml_client.get_item_description(db, prod.ml_item_id)
            desc_text = (desc_data.get("plain_text") or "").strip()
            if desc_text:
                # ML permite descripciones largas; cortamos a 50K para seguridad
                prod.descripcion = desc_text[:50000]
                hidratado.append("descripción")

        # Atributos crudos: guardamos el array completo (con value_id, value_struct
        # y demás) para poder PUSHEAR cambios después manteniendo los IDs de ML.
        raw_attrs = item.get("attributes") or []
        if raw_attrs:
            prod.ml_raw_attributes = raw_attrs

        # Atributos + sale_terms → ficha_tecnica (additive merge: solo agregamos
        # claves que NO existan ya, así no pisamos ediciones manuales).
        existing_ficha = dict(prod.ficha_tecnica or {})
        nuevos_attrs = 0

        for attr in (item.get("attributes") or []):
            attr_id = attr.get("id") or ""
            attr_name = attr.get("name") or attr_id
            value = _attr_value_str(attr)
            if not value:
                continue
            # Algunos atributos van a campos dedicados del producto
            target_field = _ML_ATTR_TO_FIELD.get(attr_id)
            if target_field == "marca" and not prod.marca:
                prod.marca = value[:80]
                hidratado.append("marca")
                continue
            # Resto va a la ficha técnica
            key = _norm_attr_key(attr_name) or _norm_attr_key(attr_id)
            if key and key not in existing_ficha:
                existing_ficha[key] = value
                nuevos_attrs += 1

        for term in (item.get("sale_terms") or []):
            term_id = term.get("id") or ""
            term_name = term.get("name") or term_id
            value = _attr_value_str(term)
            if not value:
                continue
            key = _norm_attr_key(term_name) or _norm_attr_key(term_id)
            if key and key not in existing_ficha:
                existing_ficha[key] = value
                nuevos_attrs += 1

        if nuevos_attrs > 0:
            # Asignación de dict nuevo (no mutación) para que SQLAlchemy
            # detecte el cambio en el campo JSONB.
            prod.ficha_tecnica = existing_ficha
            hidratado.append(
                f"{nuevos_attrs} atributo{'' if nuevos_attrs == 1 else 's'} en ficha"
            )

        # Compatibilidades vehiculares — endpoint separado.
        # Las creamos localmente si no están ya (matching por ml_compat_id).
        try:
            ml_compats = ml_client.get_item_compatibilities(db, prod.ml_item_id)
        except Exception:
            ml_compats = []

        if ml_compats:
            # Cargar set de ml_compat_ids ya conocidos para este producto
            existing_ml_ids = set(
                row[0] for row in db.execute(
                    select(ProductoCompatibilidad.ml_compat_id)
                    .where(
                        ProductoCompatibilidad.producto_id == prod.id,
                        ProductoCompatibilidad.ml_compat_id.is_not(None),
                    )
                ).all()
                if row[0]
            )
            cache_v: dict = {}
            nuevas_compats = 0
            for compat in ml_compats:
                ml_id = str(compat.get("id") or "")
                if not ml_id or ml_id in existing_ml_ids:
                    continue
                # Extraer atributos del compat
                cattrs = {a.get("id"): a for a in (compat.get("attributes") or [])}
                marca_a = cattrs.get("VEHICLE_BRAND") or cattrs.get("BRAND")
                modelo_a = cattrs.get("VEHICLE_MODEL") or cattrs.get("MODEL")
                year_a = cattrs.get("VEHICLE_YEAR") or cattrs.get("YEAR")
                if not marca_a or not modelo_a:
                    continue
                marca_v = (marca_a.get("value_name") or "").strip()
                modelo_v = (modelo_a.get("value_name") or "").strip()
                if not marca_v or not modelo_v:
                    continue
                # Año: puede venir como "1985" o como rango en value_struct
                anio_desde = anio_hasta = None
                if year_a:
                    yt = (year_a.get("value_name") or "").strip()
                    if "-" in yt:
                        parts = yt.split("-")
                        try:
                            anio_desde = int(parts[0].strip())
                            anio_hasta = int(parts[1].strip())
                        except (ValueError, IndexError):
                            pass
                    elif yt.isdigit():
                        anio_desde = anio_hasta = int(yt)
                v, _ = _get_or_create_vehiculo(
                    db,
                    marca=marca_v[:80],
                    modelo=modelo_v[:120],
                    combustible=None,
                    cilindros=None,
                    valvulas=None,
                    cilindrada_cc=None,
                    anio_desde=anio_desde,
                    anio_hasta=anio_hasta,
                    cache=cache_v,
                )
                # Si ya hay un link al mismo vehículo sin ml_compat_id, lo asociamos
                # con este ml_id (en lugar de crear duplicado).
                existing_link = db.execute(
                    select(ProductoCompatibilidad).where(
                        ProductoCompatibilidad.producto_id == prod.id,
                        ProductoCompatibilidad.vehiculo_id == v.id,
                    )
                ).scalar_one_or_none()
                if existing_link is not None:
                    if not existing_link.ml_compat_id:
                        existing_link.ml_compat_id = ml_id
                        nuevas_compats += 1
                    continue
                # Crear link nuevo
                db.add(ProductoCompatibilidad(
                    producto_id=prod.id,
                    vehiculo_id=v.id,
                    ml_compat_id=ml_id,
                ))
                nuevas_compats += 1
            if nuevas_compats > 0:
                hidratado.append(
                    f"{nuevas_compats} compatibilidad{'es' if nuevas_compats != 1 else ''}"
                )

        # Fotos: si no hay fotos en R2, descargar las de ML
        existing_photos = int(db.execute(
            select(sql_func.count(FotoProducto.id))
            .where(FotoProducto.producto_id == prod.id)
        ).scalar() or 0)

        if existing_photos == 0:
            from . import storage as storage_mod
            if storage_mod.is_configured():
                pictures = item.get("pictures") or []
                imported = 0
                for i, pic in enumerate(pictures[:10]):
                    pic_url = pic.get("secure_url") or pic.get("url")
                    if not pic_url:
                        continue
                    ok_pic, _err = _download_ml_photo(
                        db, prod.id, sku, pic_url, i
                    )
                    if ok_pic:
                        imported += 1
                if imported:
                    hidratado.append(f"{imported} foto{'s' if imported != 1 else ''}")

    db.commit()

    # ---- Mensaje ----
    msg_parts = [f"estado ML: {prod.ml_status or '?'}"]
    if hidratado:
        msg_parts.append(f"hidratado: {', '.join(hidratado)}")

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

    msg = f"✓ Sync OK · {' · '.join(msg_parts)}"
    if drift_bits:
        msg += " · ⚠ drift: " + ", ".join(drift_bits)

    return True, msg
