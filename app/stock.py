"""
app/stock.py
============
Servicio del módulo Stock:
  - Resumen de niveles de stock (totales, bajo stock, sin stock)
  - Listado de productos con stock bajo
  - Update individual de un SKU (set absoluto, +1, -1)
  - Bulk update via Excel simplificado (solo SKU + Stock_Actual)
  - Generador de template Excel para el upload masivo

A diferencia del Excel master (módulo Catálogo), este flujo SOLO toca
`stock_actual` — no afecta título, precios, ficha técnica ni compatibilidades.
Útil para "llegó mercadería, actualizo 50 SKUs" sin riesgo de pisar otros campos.
"""

from __future__ import annotations

import io
from dataclasses import dataclass, field

import pandas as pd
from sqlalchemy import func as sql_func, select, update
from sqlalchemy.orm import Session

from .catalogo import _norm_col, _parse_int, _parse_str
from .models import Producto


# Threshold default para "stock bajo" (excluye los que están en 0)
LOW_STOCK_THRESHOLD = 3


# =============================================================
# Resumen para el dashboard de Stock
# =============================================================

def get_summary(db: Session, low_threshold: int = LOW_STOCK_THRESHOLD) -> dict:
    """Métricas globales: totales, stock bajo, sin stock."""
    total_productos = db.execute(
        select(sql_func.count(Producto.id)).where(Producto.activo == True)  # noqa: E712
    ).scalar() or 0

    total_unidades = db.execute(
        select(sql_func.coalesce(sql_func.sum(Producto.stock_actual), 0))
        .where(Producto.activo == True)  # noqa: E712
    ).scalar() or 0

    low_stock = db.execute(
        select(sql_func.count(Producto.id)).where(
            Producto.activo == True,  # noqa: E712
            Producto.stock_actual < low_threshold,
            Producto.stock_actual > 0,
        )
    ).scalar() or 0

    sin_stock = db.execute(
        select(sql_func.count(Producto.id)).where(
            Producto.activo == True,  # noqa: E712
            Producto.stock_actual == 0,
        )
    ).scalar() or 0

    return {
        "total_productos": int(total_productos),
        "total_unidades": int(total_unidades),
        "low_stock": int(low_stock),
        "sin_stock": int(sin_stock),
        "low_threshold": low_threshold,
    }


# =============================================================
# Listado de productos con stock bajo
# =============================================================

def list_low_stock(
    db: Session,
    threshold: int = LOW_STOCK_THRESHOLD,
    limit: int = 200,
) -> list[dict]:
    """
    Productos activos con stock < threshold (incluye 0).
    Ordenados por stock ASC, después por título — los más críticos primero.
    """
    q = (
        select(Producto)
        .where(
            Producto.activo == True,  # noqa: E712
            Producto.stock_actual < threshold,
        )
        .order_by(Producto.stock_actual, Producto.titulo)
        .limit(limit)
    )
    productos: list[dict] = []
    for prod in db.execute(q).scalars().all():
        productos.append({
            "id": prod.id,
            "sku": prod.sku,
            "titulo": prod.titulo,
            "categoria": prod.categoria,
            "marca": prod.marca,
            "stock_actual": prod.stock_actual,
        })
    return productos


# =============================================================
# Update individual (set absoluto)
# =============================================================

def update_stock(db: Session, sku: str, new_stock: int) -> tuple[bool, str]:
    """Setea stock_actual a un valor absoluto. Devuelve (ok, mensaje)."""
    if new_stock < 0:
        return False, "El stock no puede ser negativo"

    result = db.execute(
        update(Producto)
        .where(Producto.sku == sku)
        .values(stock_actual=new_stock)
    )
    if result.rowcount == 0:
        return False, f"SKU '{sku}' no existe"

    db.commit()
    unidades = "unidad" if new_stock == 1 else "unidades"
    return True, f"Stock actualizado: {new_stock} {unidades}"


# =============================================================
# Bulk update via Excel
# =============================================================

@dataclass
class StockUploadResult:
    actualizados: int = 0
    errores: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return len(self.errores) == 0


def process_stock_upload(db: Session, file_bytes: bytes) -> StockUploadResult:
    """
    Procesa un Excel simplificado con SKU + Stock_Actual.
    Solo hace UPDATE del stock — los demás campos quedan intactos.
    """
    result = StockUploadResult()

    try:
        sheets = pd.read_excel(io.BytesIO(file_bytes), sheet_name=None)
    except Exception as e:
        result.errores.append(f"No se pudo leer el Excel: {e}")
        return result

    # Buscar la primera hoja que tenga columnas SKU + Stock
    target_df = None
    for _name, df in sheets.items():
        df_copy = df.copy()
        df_copy.columns = [_norm_col(c) for c in df_copy.columns]
        cols = set(df_copy.columns)
        has_sku = "sku" in cols or "codigo" in cols
        has_stock = "stock" in cols or "stock_actual" in cols
        if has_sku and has_stock:
            target_df = df_copy
            break

    if target_df is None:
        result.errores.append(
            "Ninguna hoja del Excel tiene columnas SKU y Stock"
        )
        return result

    sku_col = "sku" if "sku" in target_df.columns else "codigo"
    stock_col = "stock_actual" if "stock_actual" in target_df.columns else "stock"

    # Recolectar updates (SKU → stock)
    updates_map: dict[str, int] = {}
    for idx, row in target_df.iterrows():
        sku = _parse_str(row.get(sku_col))
        stock = _parse_int(row.get(stock_col))
        if not sku:
            continue
        if stock is None:
            result.errores.append(f"Fila {idx + 2} (SKU {sku}): stock vacío o inválido")
            continue
        if stock < 0:
            result.errores.append(f"Fila {idx + 2} (SKU {sku}): stock negativo no permitido")
            continue
        updates_map[sku] = stock

    if not updates_map:
        return result

    # Validar qué SKUs existen (un solo query, evita N+1)
    existing = set(
        s for (s,) in db.execute(
            select(Producto.sku).where(Producto.sku.in_(list(updates_map.keys())))
        ).all()
    )

    # UPDATE por SKU (uno por uno — para 50K filas habría que batchear,
    # pero para el flujo "llegó un lote" es razonable)
    for sku, stock in updates_map.items():
        if sku not in existing:
            result.errores.append(f"SKU '{sku}' no existe en el catálogo")
            continue
        db.execute(
            update(Producto).where(Producto.sku == sku).values(stock_actual=stock)
        )
        result.actualizados += 1

    db.commit()
    return result


# =============================================================
# Template Excel (solo SKU + Stock_Actual)
# =============================================================

def generate_stock_template() -> bytes:
    """Excel simple con una hoja 'Stock' y dos columnas."""
    output = io.BytesIO()
    df = pd.DataFrame([
        {"SKU": "ARO-FORD-001", "Stock_Actual": 12},
        {"SKU": "STARTER-VW-002", "Stock_Actual": 4},
    ])
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Stock", index=False)
    return output.getvalue()
