"""
app/precios.py
==============
Servicio del módulo Precios:
  - Cambios masivos por fórmula con preview (dry run)
  - Aplicación de cambios desde una lista pre-computada
  - Upload de Excel simplificado (SKU + Precio_Costo y/o Precio_Final)
  - Generador de template Excel

Las operaciones soportadas:
  - porc_inc / porc_dec  → aumento/descuento porcentual
  - fijo_inc / fijo_dec  → suma/resta fija en pesos
  - set                  → setear precio absoluto
  - mult                 → multiplicar por factor

Redondeo opcional a múltiplos de 100, 500 o 1000.

A diferencia del Excel master, este flujo solo toca `precio_costo` y/o
`precio_final` — no afecta título, ficha técnica, stock, ni vínculos ML.
"""

from __future__ import annotations

import io
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

import pandas as pd
from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from .catalogo import _is_blank, _norm_col, _parse_decimal, _parse_str
from .models import Producto


# =============================================================
# Constantes — operaciones, targets, redondeos
# =============================================================

OPERACIONES = {
    "porc_inc": "Aumento %",
    "porc_dec": "Descuento %",
    "fijo_inc": "Sumar $",
    "fijo_dec": "Restar $",
    "set":      "Setear $",
    "mult":     "Multiplicar ×",
}

TARGETS = {
    "costo": "Precio costo",
    "final": "Precio final",
    "ambos": "Ambos (mismo % o monto a cada uno)",
    "costo_keep_margen": "Costo (recalcular final manteniendo margen)",
}

REDONDEOS = {
    0:    "Sin redondear",
    100:  "A múltiplos de 100",
    500:  "A múltiplos de 500",
    1000: "A múltiplos de 1000",
}


# =============================================================
# Tipos de retorno
# =============================================================

@dataclass
class PrecioChange:
    sku: str
    titulo: str
    campo: str            # "precio_costo" o "precio_final"
    valor_actual: Decimal
    valor_nuevo: Decimal

    @property
    def delta(self) -> Decimal:
        return self.valor_nuevo - self.valor_actual

    @property
    def delta_pct(self) -> Decimal:
        if self.valor_actual == 0:
            return Decimal("0")
        return ((self.valor_nuevo - self.valor_actual) / self.valor_actual) * 100


@dataclass
class PreciosUploadResult:
    actualizados: int = 0
    errores: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return len(self.errores) == 0


# =============================================================
# Helpers de cálculo
# =============================================================

def _aplicar_operacion(
    precio: Decimal,
    operacion: str,
    valor: Decimal,
) -> Decimal:
    """Aplica la operación al precio actual. Devuelve el precio nuevo."""
    if operacion == "porc_inc":
        return precio * (Decimal("1") + valor / Decimal("100"))
    if operacion == "porc_dec":
        return precio * (Decimal("1") - valor / Decimal("100"))
    if operacion == "fijo_inc":
        return precio + valor
    if operacion == "fijo_dec":
        return precio - valor
    if operacion == "set":
        return valor
    if operacion == "mult":
        return precio * valor
    raise ValueError(f"Operación desconocida: {operacion}")


def _redondear(valor: Decimal, redondeo: int) -> Decimal:
    """Redondea al múltiplo de `redondeo` más cercano. Si redondeo=0, a 2 decimales."""
    if redondeo <= 0:
        return valor.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    base = Decimal(redondeo)
    # Dividir, redondear al entero, multiplicar de vuelta
    return (valor / base).quantize(Decimal("1"), rounding=ROUND_HALF_UP) * base


# =============================================================
# Cambios por fórmula — preview (dry run) + aplicación
# =============================================================

def compute_precio_changes(
    db: Session,
    *,
    operacion: str,
    valor: Decimal,
    target: str,
    redondeo: int = 0,
    # Filtros de scope (mismos que /catalogo)
    search: str = "",
    categoria: str = "",
    marca: str = "",
    vinculadas: str = "",
) -> list[PrecioChange]:
    """
    Computa los cambios sin aplicarlos (dry run para preview).
    Solo incluye productos `activos`.
    Excluye cambios que dejarían precio negativo o que no producen diff.
    """
    if operacion not in OPERACIONES:
        raise ValueError(f"Operación inválida: {operacion}")
    if target not in TARGETS:
        raise ValueError(f"Target inválido: {target}")

    q = select(Producto).where(Producto.activo == True)  # noqa: E712

    if search and search.strip():
        like = f"%{search.strip()}%"
        q = q.where(or_(
            Producto.sku.ilike(like),
            Producto.titulo.ilike(like),
            Producto.marca.ilike(like),
            Producto.categoria.ilike(like),
        ))
    if categoria:
        q = q.where(Producto.categoria == categoria)
    if marca:
        q = q.where(Producto.marca == marca)
    if vinculadas == "si":
        q = q.where(Producto.ml_item_id.is_not(None))
    elif vinculadas == "no":
        q = q.where(Producto.ml_item_id.is_(None))

    q = q.order_by(Producto.sku)

    productos = db.execute(q).scalars().all()

    changes: list[PrecioChange] = []

    # Modo especial: aplicar al costo y recalcular el final manteniendo el margen
    # implícito (precio_final / precio_costo).
    if target == "costo_keep_margen":
        for prod in productos:
            actual_costo = prod.precio_costo
            actual_final = prod.precio_final
            # Necesitamos ambos precios para inferir el margen
            if actual_costo is None or actual_final is None or actual_costo == 0:
                continue
            try:
                nuevo_costo = _aplicar_operacion(actual_costo, operacion, valor)
            except Exception:
                continue
            nuevo_costo = _redondear(nuevo_costo, redondeo)
            if nuevo_costo < 0 or nuevo_costo == actual_costo:
                continue

            # factor multiplicativo (preserva ratio costo→final)
            factor = actual_final / actual_costo
            nuevo_final = _redondear(nuevo_costo * factor, redondeo)
            if nuevo_final < 0:
                continue

            changes.append(PrecioChange(
                sku=prod.sku, titulo=prod.titulo, campo="precio_costo",
                valor_actual=actual_costo, valor_nuevo=nuevo_costo,
            ))
            if nuevo_final != actual_final:
                changes.append(PrecioChange(
                    sku=prod.sku, titulo=prod.titulo, campo="precio_final",
                    valor_actual=actual_final, valor_nuevo=nuevo_final,
                ))
        return changes

    # Modo simple: aplicar la misma operación a costo y/o final independientemente
    campos = []
    if target in ("costo", "ambos"):
        campos.append("precio_costo")
    if target in ("final", "ambos"):
        campos.append("precio_final")

    for prod in productos:
        for campo in campos:
            actual = getattr(prod, campo)
            if actual is None:
                continue
            try:
                nuevo = _aplicar_operacion(actual, operacion, valor)
            except Exception:
                continue
            nuevo = _redondear(nuevo, redondeo)
            if nuevo < 0:
                continue
            if nuevo == actual:
                continue
            changes.append(PrecioChange(
                sku=prod.sku,
                titulo=prod.titulo,
                campo=campo,
                valor_actual=actual,
                valor_nuevo=nuevo,
            ))

    return changes


def apply_precio_changes(db: Session, changes: list[PrecioChange]) -> int:
    """
    Aplica los cambios pre-computados. Devuelve cantidad de productos actualizados.
    Múltiples cambios para el mismo SKU (ej: costo y final) se mergean.
    """
    if not changes:
        return 0

    # Agrupar por SKU
    sku_updates: dict[str, dict] = {}
    for c in changes:
        sku_updates.setdefault(c.sku, {})[c.campo] = c.valor_nuevo

    aplicados = 0
    for sku, updates in sku_updates.items():
        prod = db.execute(
            select(Producto).where(Producto.sku == sku)
        ).scalar_one_or_none()
        if prod is None:
            continue
        for campo, val in updates.items():
            setattr(prod, campo, val)
        aplicados += 1

    db.commit()
    return aplicados


# =============================================================
# Upload Excel — solo precios
# =============================================================

def process_precios_upload(db: Session, file_bytes: bytes) -> PreciosUploadResult:
    """
    Procesa Excel con SKU + Precio_Costo y/o Precio_Final.
    Solo actualiza esos campos — no toca títulos, stock, ML, etc.
    """
    result = PreciosUploadResult()

    try:
        sheets = pd.read_excel(io.BytesIO(file_bytes), sheet_name=None)
    except Exception as e:
        result.errores.append(f"No se pudo leer el Excel: {e}")
        return result

    # Buscar la primera hoja con SKU + algún campo de precio
    target_df = None
    for _name, df in sheets.items():
        df_copy = df.copy()
        df_copy.columns = [_norm_col(c) for c in df_copy.columns]
        cols = set(df_copy.columns)
        has_sku = any(c in cols for c in ("sku", "codigo"))
        has_price = any(
            c in cols for c in ("precio_costo", "costo", "precio_final", "precio")
        )
        if has_sku and has_price:
            target_df = df_copy
            break

    if target_df is None:
        result.errores.append(
            "Ninguna hoja tiene columnas SKU y al menos un precio (Precio_Costo o Precio_Final)"
        )
        return result

    sku_col = "sku" if "sku" in target_df.columns else "codigo"

    # Normalizar columnas de precio (puede haber alias)
    costo_col = None
    for c in ("precio_costo", "costo"):
        if c in target_df.columns:
            costo_col = c
            break
    final_col = None
    for c in ("precio_final", "precio"):
        if c in target_df.columns:
            final_col = c
            break

    # Procesar filas
    for idx, row in target_df.iterrows():
        sku = _parse_str(row.get(sku_col))
        if not sku:
            continue

        prod = db.execute(
            select(Producto).where(Producto.sku == sku)
        ).scalar_one_or_none()
        if prod is None:
            result.errores.append(f"SKU '{sku}' no existe en el catálogo")
            continue

        cambio = False
        if costo_col:
            v = _parse_decimal(row.get(costo_col))
            if v is not None and v >= 0 and v != prod.precio_costo:
                prod.precio_costo = v
                cambio = True
        if final_col:
            v = _parse_decimal(row.get(final_col))
            if v is not None and v >= 0 and v != prod.precio_final:
                prod.precio_final = v
                cambio = True

        if cambio:
            result.actualizados += 1

    db.commit()
    return result


# =============================================================
# Template Excel
# =============================================================

def generate_precios_template() -> bytes:
    """Excel con SKU + Precio_Costo + Precio_Final."""
    output = io.BytesIO()
    df = pd.DataFrame([
        {"SKU": "ARO-FORD-001", "Precio_Costo": 8500, "Precio_Final": 14900},
        {"SKU": "STARTER-VW-002", "Precio_Costo": 32000, "Precio_Final": 58900},
    ])
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Precios", index=False)
    return output.getvalue()
