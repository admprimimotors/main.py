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
from typing import Optional

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
    # Margen implícito antes y después del cambio (computados a nivel SKU,
    # iguales en ambas filas cuando un mismo SKU tiene 2 cambios).
    # None si falta uno de los dos precios → no hay margen calculable.
    margen_actual: Optional[Decimal] = None
    margen_nuevo: Optional[Decimal] = None

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


@dataclass
class PrecioPreview:
    """Resultado del cálculo de cambios + diagnóstico del scope."""
    changes: list[PrecioChange] = field(default_factory=list)
    scope_total: int = 0           # cuántos productos matchean los filtros
    skipped_no_costo: int = 0      # productos en scope sin precio_costo
    skipped_no_final: int = 0      # productos en scope sin precio_final
    skipped_no_change: int = 0     # la fórmula no produce diff
    skipped_negative: int = 0      # el resultado sería negativo
    skipped_keep_margen_incompleto: int = 0  # falta uno de los precios para keep_margen


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


def _compute_margen(
    costo: Optional[Decimal],
    final: Optional[Decimal],
) -> Optional[Decimal]:
    """Margen implícito (final - costo) / costo * 100. None si falta data."""
    if costo is None or final is None or costo == 0:
        return None
    return ((final - costo) / costo * Decimal("100")).quantize(
        Decimal("0.1"), rounding=ROUND_HALF_UP
    )


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
    return_preview: bool = False,
) -> list[PrecioChange] | PrecioPreview:
    """
    Computa los cambios sin aplicarlos (dry run para preview).
    Solo incluye productos `activos`.
    Excluye cambios que dejarían precio negativo o que no producen diff.

    Si `return_preview=True`, devuelve un PrecioPreview con changes + diagnóstico
    del scope (cuántos productos hay, cuántos se saltearon y por qué).
    Si False (default), devuelve solo la lista de changes — útil para el apply.
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
    preview = PrecioPreview(scope_total=len(productos))

    cambia_costo = target in ("costo", "ambos", "costo_keep_margen")
    cambia_final = target in ("final", "ambos", "costo_keep_margen")

    for prod in productos:
        actual_costo = prod.precio_costo
        actual_final = prod.precio_final

        # Modo keep_margen: necesita ambos precios + costo > 0
        if target == "costo_keep_margen":
            if actual_costo is None or actual_final is None or actual_costo == 0:
                preview.skipped_keep_margen_incompleto += 1
                continue

        # ---- Calcular nuevo_costo ----
        nuevo_costo = actual_costo
        costo_skipped_reason = None
        if cambia_costo:
            if actual_costo is None:
                preview.skipped_no_costo += 1
                costo_skipped_reason = "no_costo"
            else:
                try:
                    nc = _redondear(_aplicar_operacion(actual_costo, operacion, valor), redondeo)
                    if nc < 0:
                        preview.skipped_negative += 1
                        costo_skipped_reason = "negative"
                    else:
                        nuevo_costo = nc
                except Exception:
                    costo_skipped_reason = "error"

        # ---- Calcular nuevo_final ----
        nuevo_final = actual_final
        final_skipped_reason = None
        if cambia_final:
            if target == "costo_keep_margen":
                # Recalcular final preservando el factor multiplicativo
                if nuevo_costo is not None and actual_costo and actual_final is not None:
                    factor = actual_final / actual_costo
                    nf = _redondear(nuevo_costo * factor, redondeo)
                    if nf < 0:
                        preview.skipped_negative += 1
                        final_skipped_reason = "negative"
                    else:
                        nuevo_final = nf
            else:
                if actual_final is None:
                    preview.skipped_no_final += 1
                    final_skipped_reason = "no_final"
                else:
                    try:
                        nf = _redondear(_aplicar_operacion(actual_final, operacion, valor), redondeo)
                        if nf < 0:
                            preview.skipped_negative += 1
                            final_skipped_reason = "negative"
                        else:
                            nuevo_final = nf
                    except Exception:
                        final_skipped_reason = "error"

        # ---- Margens (consistentes con el estado FINAL del producto) ----
        margen_actual = _compute_margen(actual_costo, actual_final)
        margen_nuevo = _compute_margen(nuevo_costo, nuevo_final)

        # ---- Crear filas para los cambios reales ----
        if cambia_costo and costo_skipped_reason is None and actual_costo is not None:
            if nuevo_costo == actual_costo:
                preview.skipped_no_change += 1
            else:
                preview.changes.append(PrecioChange(
                    sku=prod.sku, titulo=prod.titulo, campo="precio_costo",
                    valor_actual=actual_costo, valor_nuevo=nuevo_costo,
                    margen_actual=margen_actual, margen_nuevo=margen_nuevo,
                ))
        if cambia_final and final_skipped_reason is None and actual_final is not None:
            if nuevo_final == actual_final:
                preview.skipped_no_change += 1
            else:
                preview.changes.append(PrecioChange(
                    sku=prod.sku, titulo=prod.titulo, campo="precio_final",
                    valor_actual=actual_final, valor_nuevo=nuevo_final,
                    margen_actual=margen_actual, margen_nuevo=margen_nuevo,
                ))

    return preview if return_preview else preview.changes


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
