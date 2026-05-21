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

# Claves posibles en la ficha técnica que indican "unidades por envase / caja".
# La búsqueda es tolerante: se prueba en este orden y se usa la primera que
# matchee. Los headers del Excel del catálogo se normalizan vía _norm_col
# (snake_case, sin tildes), así que "Unidades por envase" llega como
# "unidades_por_envase".
_FICHA_KEYS_UNIDADES_ENVASE: tuple[str, ...] = (
    "unidades_por_envase",
    "unidades_x_envase",
    "unidades_por_caja",
    "unidades_x_caja",
    "u_por_envase",
    "u_x_envase",
)


def _get_unidades_por_envase(ficha: dict | None) -> int | None:
    """
    Lee la cantidad de unidades por envase desde la ficha técnica.
    Tolera varias variantes de nombre y valores numéricos como str/float/int.
    Devuelve None si no encuentra ningún campo válido, o si el valor no es
    un entero positivo.
    """
    if not ficha:
        return None
    for key in _FICHA_KEYS_UNIDADES_ENVASE:
        if key in ficha:
            raw = ficha[key]
            try:
                # Soporta "6", "6.0", 6, 6.0 — pero no "seis" ni texto basura
                upe = int(float(str(raw).strip().replace(",", ".")))
                if upe > 0:
                    return upe
            except (ValueError, TypeError, AttributeError):
                continue
    return None


@dataclass
class StockUploadConversion:
    """Detalle por SKU cuando se aplica modo distribuidor."""
    sku: str
    stock_distri: int
    unidades_por_envase: int
    stock_final: int  # Cajas resultantes (= stock_distri // unidades_por_envase)


@dataclass
class StockUploadResult:
    actualizados: int = 0
    # Modo distri:
    convertidos: int = 0           # SKUs cuyo stock se dividió por unidades/envase
    sin_caja_completa: list[StockUploadConversion] = field(default_factory=list)
    # Detalle de cada conversión aplicada (útil para auditar)
    conversiones: list[StockUploadConversion] = field(default_factory=list)
    errores: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return len(self.errores) == 0


def process_stock_upload(
    db: Session,
    file_bytes: bytes,
    *,
    convert_distri: bool = False,
) -> StockUploadResult:
    """
    Procesa un Excel simplificado con SKU + Stock_Actual.
    Solo hace UPDATE del stock — los demás campos quedan intactos.

    Si `convert_distri=True`, el valor de Stock_Actual se interpreta como
    "stock del distribuidor" (unidades sueltas) y se divide por las
    `unidades_por_envase` definidas en la ficha técnica del producto
    (floor / piso de la división). Pensado para SKUs que se venden por
    caja pero que el proveedor reporta por unidad (ej. Camisas de Motor).

    Reglas en modo distri:
      - Si el SKU no tiene `unidades_por_envase` en la ficha → ERROR.
      - Si stock_distri < unidades_por_envase → stock_final = 0
        (no llegamos a completar una caja). Se reporta como descartado.
      - Si stock_distri % unidades_por_envase > 0 → stock_final = floor.
        El excedente no se cuenta.
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

    # Recolectar updates (SKU → stock crudo del Excel)
    raw_inputs: dict[str, int] = {}
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
        raw_inputs[sku] = stock

    if not raw_inputs:
        return result

    # Traer todos los productos de una sola query: necesitamos ficha_tecnica
    # para el modo distri. Aunque no esté activado, conviene una sola lectura
    # para validar existencia.
    skus_list = list(raw_inputs.keys())
    productos_db = {
        p.sku: p for p in db.execute(
            select(Producto).where(Producto.sku.in_(skus_list))
        ).scalars().all()
    }

    # Decidir el stock final por SKU según el modo
    updates_map: dict[str, int] = {}
    for sku, stock_raw in raw_inputs.items():
        prod = productos_db.get(sku)
        if prod is None:
            result.errores.append(f"SKU '{sku}' no existe en el catálogo")
            continue

        if not convert_distri:
            # Modo clásico: el valor del Excel es el stock real
            updates_map[sku] = stock_raw
            continue

        # --- Modo distribuidor ---
        upe = _get_unidades_por_envase(prod.ficha_tecnica)
        if upe is None:
            result.errores.append(
                f"SKU '{sku}': no tiene 'unidades por envase' en la ficha técnica. "
                f"Cargá ese campo primero o subí el archivo sin el modo distribuidor."
            )
            continue

        stock_final = stock_raw // upe  # floor
        conv = StockUploadConversion(
            sku=sku,
            stock_distri=stock_raw,
            unidades_por_envase=upe,
            stock_final=stock_final,
        )
        result.conversiones.append(conv)
        result.convertidos += 1
        if stock_final == 0 and stock_raw > 0:
            # No alcanzó a completar una caja → se descarta, queda en 0
            result.sin_caja_completa.append(conv)
        updates_map[sku] = stock_final

    if not updates_map:
        return result

    # UPDATE por SKU (uno por uno — para 50K filas habría que batchear,
    # pero para el flujo "llegó un lote" es razonable)
    for sku, stock in updates_map.items():
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
