"""
inventory/importer.py
=====================
Importador de catálogos de proveedores a la base de datos local.

Soporta por ahora el formato ZEN:
  - Archivo de PRECIOS: CODART / DESCRIPCIO / MARCA / PRCOSTO
  - Archivo de STOCK:   CODIGO / RUBRO / DESCRIPCION / STK

El merge se hace por SKU núcleo normalizado.

Flujo:
  1. Leer ambos archivos con pandas.
  2. Normalizar SKU para tener una clave de merge.
  3. Combinar: si un producto está en ambos, toma precio de uno y stock/rubro del otro.
  4. Detectar categoría (desde rubro o fallback por descripción).
  5. Detectar marca de auto y marca de repuesto.
  6. Detectar (X PED) → por_pedido.
  7. Saltar productos reacondicionados (sufijo R) por decisión del usuario.
  8. Insertar/actualizar en la base dentro de una transacción.
  9. Registrar histórico de precios y movimiento de stock.
 10. Devolver un resumen detallado.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import pandas as pd

import db
from inventory import normalizer as norm
from logger import get_logger

log = get_logger(__name__)


# ==========================================================
# Resultado de la importación
# ==========================================================
@dataclass
class ResultadoImportacion:
    archivos: list[str] = field(default_factory=list)
    filas_leidas_precios: int = 0
    filas_leidas_stock: int = 0
    productos_matcheados: int = 0           # aparecen en ambos archivos
    productos_solo_precios: int = 0          # solo en archivo de precios
    productos_solo_stock: int = 0            # solo en archivo de stock
    productos_reacondicionados_saltados: int = 0
    productos_nuevos: int = 0                # insertados
    productos_actualizados: int = 0          # update (ya existían)
    productos_marcados_por_pedido: int = 0
    errores: list[str] = field(default_factory=list)

    def resumen_texto(self) -> str:
        lineas = [
            "Resumen de importación:",
            f"  Archivos procesados:              {len(self.archivos)}",
            f"  Filas leídas (precios):           {self.filas_leidas_precios}",
            f"  Filas leídas (stock):             {self.filas_leidas_stock}",
            f"  Productos en ambos archivos:      {self.productos_matcheados}",
            f"  Productos solo en precios:        {self.productos_solo_precios}",
            f"  Productos solo en stock:          {self.productos_solo_stock}",
            f"  Reacondicionados saltados:        {self.productos_reacondicionados_saltados}",
            f"  → NUEVOS insertados:              {self.productos_nuevos}",
            f"  → Actualizados:                   {self.productos_actualizados}",
            f"  → Marcados 'por pedido':          {self.productos_marcados_por_pedido}",
        ]
        if self.errores:
            lineas.append(f"  Errores: {len(self.errores)}")
            for e in self.errores[:5]:
                lineas.append(f"    - {e}")
        return "\n".join(lineas)


# ==========================================================
# Lectura de archivos
# ==========================================================
def _leer_precios_zen(archivo: Path) -> pd.DataFrame:
    """
    Lee el Excel 'LISTA PRECIOS ZEN'. La fila 0 son los encabezados pero vienen
    con nombres feos ('Lista de Precios', 'Unnamed: 1', etc.), así que saltamos
    esa fila y renombramos.
    """
    df = pd.read_excel(archivo, header=1)
    df.columns = ["CODART", "DESCRIPCION", "MARCA", "PRCOSTO"]
    df = df.dropna(subset=["CODART"]).copy()
    df["CODART"] = df["CODART"].astype(str).str.strip()
    df["DESCRIPCION"] = df["DESCRIPCION"].astype(str).str.strip()
    df["MARCA"] = df["MARCA"].astype(str).str.strip()
    return df


def _leer_stock_zen(archivo: Path) -> pd.DataFrame:
    """
    Lee el Excel 'LISTA ZEN STOCK SIN PRECIO'. Mismo problema de headers.
    """
    df = pd.read_excel(archivo, header=1)
    df.columns = ["CODIGO", "RUBRO", "DESCRIPCION", "STK"]
    df = df.dropna(subset=["CODIGO"]).copy()
    df["CODIGO"] = df["CODIGO"].astype(str).str.strip()
    df["DESCRIPCION"] = df["DESCRIPCION"].astype(str).str.strip()
    df["RUBRO"] = df["RUBRO"].fillna("").astype(str).str.strip()
    return df


# ==========================================================
# Merge de los dos archivos por SKU núcleo
# ==========================================================
def _construir_merge(
    df_precios: Optional[pd.DataFrame],
    df_stock: Optional[pd.DataFrame],
) -> pd.DataFrame:
    """
    Devuelve un único DataFrame con columnas estandarizadas:
      sku_master, sku_proveedor_precios, sku_proveedor_stock,
      descripcion, marca_proveedor, rubro_origen, costo, stock,
      es_reacondicionado, por_pedido
    """
    precios_rows = []
    if df_precios is not None:
        for _, r in df_precios.iterrows():
            codart = r["CODART"]
            nucleo = norm.normalizar_sku(codart)
            if not nucleo:
                continue
            precios_rows.append({
                "sku_master": nucleo,
                "sku_proveedor": codart,
                "descripcion": r["DESCRIPCION"],
                "marca_proveedor": r.get("MARCA") or "ZEN",
                "costo": norm.parsear_numero_ar(r.get("PRCOSTO")),
            })
    df_p = pd.DataFrame(precios_rows)

    stock_rows = []
    if df_stock is not None:
        for _, r in df_stock.iterrows():
            codigo = r["CODIGO"]
            reacond = norm.es_reacondicionado(codigo)
            nucleo = norm.normalizar_sku(codigo)
            if not nucleo:
                continue
            stock_rows.append({
                "sku_master": nucleo,
                "sku_proveedor_stock": codigo,
                "descripcion_stock": r["DESCRIPCION"],
                "rubro_origen": r.get("RUBRO") or "",
                "stock": norm.parsear_entero(r.get("STK")),
                "es_reacondicionado": reacond,
            })
    df_s = pd.DataFrame(stock_rows)

    if not df_p.empty and not df_s.empty:
        merged = df_p.merge(df_s, on="sku_master", how="outer")
    elif not df_p.empty:
        merged = df_p.copy()
        for col in ("sku_proveedor_stock", "descripcion_stock", "rubro_origen", "stock", "es_reacondicionado"):
            merged[col] = None
    elif not df_s.empty:
        merged = df_s.copy()
        for col in ("sku_proveedor", "descripcion", "marca_proveedor", "costo"):
            merged[col] = None
    else:
        return pd.DataFrame()

    # Unificar descripción y sku_proveedor:
    # - descripción preferida = la del archivo de precios (más completa con marcas)
    # - si no hay, usamos la del stock
    merged["descripcion_final"] = merged["descripcion"].fillna(merged.get("descripcion_stock"))
    merged["descripcion_final"] = merged["descripcion_final"].fillna("")

    # SKU proveedor preferido = el de precios (es el que usamos típicamente en ML)
    merged["sku_proveedor_final"] = merged["sku_proveedor"].fillna(merged.get("sku_proveedor_stock"))

    merged["es_reacondicionado"] = merged["es_reacondicionado"].astype("object").fillna(False).astype(bool)
    merged["por_pedido"] = merged["descripcion_final"].map(norm.es_por_pedido)
    merged["costo"] = merged["costo"].fillna(0.0)
    merged["stock"] = merged["stock"].fillna(0).astype(int)
    merged["marca_proveedor"] = merged["marca_proveedor"].fillna("ZEN")
    merged["rubro_origen"] = merged["rubro_origen"].fillna("")

    return merged


# ==========================================================
# Importación a la base de datos
# ==========================================================
def importar_zen(
    archivo_precios: Path | str | None,
    archivo_stock: Path | str | None,
    *,
    saltar_reacondicionados: bool = True,
    proveedor: str = "ZEN",
    dry_run: bool = False,
) -> ResultadoImportacion:
    """
    Importa un par de archivos ZEN (precios + stock) a la base.

    Args:
        archivo_precios: path al Excel con CODART/DESCRIPCIO/MARCA/PRCOSTO (puede ser None).
        archivo_stock:   path al Excel con CODIGO/RUBRO/DESCRIPCION/STK (puede ser None).
        saltar_reacondicionados: si True (default), no carga los SKU con sufijo R.
        proveedor: nombre del proveedor (se guarda en cada producto).
        dry_run: si True, no escribe en la base, solo muestra qué haría.

    Returns:
        ResultadoImportacion con todos los contadores.
    """
    resultado = ResultadoImportacion()

    # 1. Leer archivos
    df_precios = None
    if archivo_precios:
        p = Path(archivo_precios)
        resultado.archivos.append(p.name)
        df_precios = _leer_precios_zen(p)
        resultado.filas_leidas_precios = len(df_precios)
        log.info(f"  Precios leídos: {len(df_precios)}")

    df_stock = None
    if archivo_stock:
        p = Path(archivo_stock)
        resultado.archivos.append(p.name)
        df_stock = _leer_stock_zen(p)
        resultado.filas_leidas_stock = len(df_stock)
        log.info(f"  Stock leído:    {len(df_stock)}")

    # 2. Merge
    merged = _construir_merge(df_precios, df_stock)
    if merged.empty:
        resultado.errores.append("Ambos archivos están vacíos o no se pudieron leer.")
        return resultado

    # Contadores de cruce
    tiene_precios = merged["costo"] > 0 if "costo" in merged.columns else pd.Series([False] * len(merged))
    tiene_stock_info = merged["sku_proveedor_stock"].notna() if "sku_proveedor_stock" in merged.columns else pd.Series([False] * len(merged))
    tiene_precios_info = merged["sku_proveedor"].notna() if "sku_proveedor" in merged.columns else pd.Series([False] * len(merged))

    resultado.productos_matcheados = int((tiene_precios_info & tiene_stock_info).sum())
    resultado.productos_solo_precios = int((tiene_precios_info & ~tiene_stock_info).sum())
    resultado.productos_solo_stock = int((~tiene_precios_info & tiene_stock_info).sum())

    # 3. Cargar mapa de categorías
    categorias = {c["nombre"]: c["id"] for c in db.obtener_categorias(solo_activas=False)}
    coef_categoria = {c["id"]: c["coeficiente_default"] for c in db.obtener_categorias(solo_activas=False)}

    # 4. Recorrer y persistir
    if dry_run:
        log.info("  [DRY RUN] no se escribirá en la base")
    else:
        _persistir_productos(
            merged,
            categorias=categorias,
            coef_categoria=coef_categoria,
            saltar_reacondicionados=saltar_reacondicionados,
            proveedor=proveedor,
            resultado=resultado,
        )

    # 5. Registrar la importación
    if not dry_run:
        with db.conexion() as c:
            c.execute(
                """
                INSERT INTO importaciones (tipo, archivo, filas_leidas, filas_nuevas, filas_update, filas_error, resumen)
                VALUES (?,?,?,?,?,?,?)
                """,
                (
                    "zen_precios_stock",
                    " + ".join(resultado.archivos),
                    resultado.filas_leidas_precios + resultado.filas_leidas_stock,
                    resultado.productos_nuevos,
                    resultado.productos_actualizados,
                    len(resultado.errores),
                    resultado.resumen_texto(),
                ),
            )

    return resultado


def _persistir_productos(
    merged: pd.DataFrame,
    *,
    categorias: dict[str, int],
    coef_categoria: dict[int, float],
    saltar_reacondicionados: bool,
    proveedor: str,
    resultado: ResultadoImportacion,
) -> None:
    """Hace el INSERT/UPDATE de cada fila dentro de una transacción."""

    with db.conexion() as conn:
        for _, row in merged.iterrows():
            try:
                sku_master = str(row["sku_master"]).strip()
                if not sku_master:
                    continue

                # Saltar reacondicionados si corresponde
                if saltar_reacondicionados and bool(row["es_reacondicionado"]):
                    resultado.productos_reacondicionados_saltados += 1
                    continue

                descripcion = norm.limpiar_texto(row["descripcion_final"])
                por_pedido = bool(row["por_pedido"])
                rubro_origen = norm.limpiar_texto(row["rubro_origen"])

                # Categoría: primero rubro del proveedor, si no hay → fallback por descripción
                cat_nombre = norm.mapear_rubro_a_categoria(rubro_origen) if rubro_origen else None
                if (not cat_nombre) or cat_nombre == "Sin categoría":
                    cat_desde_desc = norm.detectar_categoria_por_descripcion(descripcion)
                    if cat_desde_desc:
                        cat_nombre = cat_desde_desc

                categoria_id = categorias.get(cat_nombre or "Sin categoría") or categorias.get("Sin categoría")

                # Marca de auto + marca de repuesto
                marcas_auto = norm.detectar_marcas_auto(descripcion)
                marca_auto_principal = marcas_auto[0] if marcas_auto else None
                marca_repuesto = norm.detectar_marca_repuesto(descripcion)
                # Si hay varias marcas de auto, las guardamos concatenadas (se muestran como "CHEVROLET, FORD")
                marca_auto_persistida = ", ".join(marcas_auto) if marcas_auto else None

                # Costo y precio
                costo = float(row["costo"] or 0)
                coef = coef_categoria.get(categoria_id, 1.50) if categoria_id else 1.50
                precio_venta = round(costo * coef, 2) if costo else 0.0

                stock_nuevo = int(row["stock"] or 0)

                sku_proveedor = row.get("sku_proveedor_final")
                sku_proveedor = norm.limpiar_texto(sku_proveedor) if sku_proveedor else None

                # ¿Existe ya?
                existente = conn.execute(
                    "SELECT id, costo, precio_venta, stock_actual FROM productos WHERE sku_master = ?",
                    (sku_master,),
                ).fetchone()

                if existente is None:
                    # INSERT
                    conn.execute(
                        """
                        INSERT INTO productos (
                            sku_master, sku_proveedor, sku_ml, descripcion,
                            marca_proveedor, proveedor, marca_auto, rubro_origen,
                            categoria_id, costo, precio_venta,
                            stock_actual, por_pedido, es_reacondicionado, activo,
                            fecha_modificacion
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,1, datetime('now','localtime'))
                        """,
                        (
                            sku_master,
                            sku_proveedor,
                            sku_proveedor,  # sku_ml arranca igual al del proveedor
                            descripcion,
                            (marca_repuesto or proveedor),
                            proveedor,
                            marca_auto_persistida,
                            rubro_origen or None,
                            categoria_id,
                            costo,
                            precio_venta,
                            stock_nuevo,
                            int(por_pedido),
                            0,  # Fase 1: no cargamos reacondicionados
                        ),
                    )
                    nuevo_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
                    resultado.productos_nuevos += 1
                    if por_pedido:
                        resultado.productos_marcados_por_pedido += 1

                    # Movimiento de stock inicial (si corresponde)
                    if stock_nuevo != 0:
                        conn.execute(
                            """
                            INSERT INTO movimientos_stock
                                (producto_id, tipo, cantidad, stock_previo, stock_nuevo, origen, notas)
                            VALUES (?, 'importacion', ?, 0, ?, 'Importación ZEN inicial', ?)
                            """,
                            (nuevo_id, stock_nuevo, stock_nuevo, f"Importación automática de {proveedor}"),
                        )

                    # Histórico de precios inicial
                    if costo > 0:
                        conn.execute(
                            """
                            INSERT INTO historico_precios
                                (producto_id, costo_anterior, costo_nuevo, precio_anterior, precio_nuevo, coef_usado, motivo)
                            VALUES (?, NULL, ?, NULL, ?, ?, 'Alta desde importación ZEN')
                            """,
                            (nuevo_id, costo, precio_venta, coef),
                        )
                else:
                    # UPDATE — solo actualizamos campos que cambiaron significativamente
                    prod_id = existente["id"]
                    costo_anterior = float(existente["costo"] or 0)
                    precio_anterior = float(existente["precio_venta"] or 0)
                    stock_anterior = int(existente["stock_actual"] or 0)

                    conn.execute(
                        """
                        UPDATE productos SET
                            sku_proveedor   = COALESCE(?, sku_proveedor),
                            sku_ml          = COALESCE(sku_ml, ?),
                            descripcion     = ?,
                            marca_proveedor = COALESCE(marca_proveedor, ?),
                            proveedor       = COALESCE(proveedor, ?),
                            marca_auto      = COALESCE(?, marca_auto),
                            rubro_origen    = COALESCE(?, rubro_origen),
                            categoria_id    = COALESCE(categoria_id, ?),
                            costo           = ?,
                            precio_venta    = ?,
                            stock_actual    = ?,
                            por_pedido      = ?,
                            fecha_modificacion = datetime('now','localtime')
                        WHERE id = ?
                        """,
                        (
                            sku_proveedor,
                            sku_proveedor,
                            descripcion,
                            (marca_repuesto or proveedor),
                            proveedor,
                            marca_auto_persistida,
                            rubro_origen or None,
                            categoria_id,
                            costo,
                            precio_venta,
                            stock_nuevo,
                            int(por_pedido),
                            prod_id,
                        ),
                    )
                    resultado.productos_actualizados += 1
                    if por_pedido:
                        resultado.productos_marcados_por_pedido += 1

                    # Movimiento de stock si cambió
                    if stock_nuevo != stock_anterior:
                        conn.execute(
                            """
                            INSERT INTO movimientos_stock
                                (producto_id, tipo, cantidad, stock_previo, stock_nuevo, origen, notas)
                            VALUES (?, 'ajuste', ?, ?, ?, 'Actualización importación ZEN', ?)
                            """,
                            (prod_id, stock_nuevo - stock_anterior, stock_anterior, stock_nuevo,
                             f"Sincronización con archivo {proveedor}"),
                        )

                    # Histórico si cambió precio/costo
                    if abs(costo - costo_anterior) > 0.01 or abs(precio_venta - precio_anterior) > 0.01:
                        conn.execute(
                            """
                            INSERT INTO historico_precios
                                (producto_id, costo_anterior, costo_nuevo, precio_anterior, precio_nuevo, coef_usado, motivo)
                            VALUES (?, ?, ?, ?, ?, ?, 'Actualización desde importación ZEN')
                            """,
                            (prod_id, costo_anterior, costo, precio_anterior, precio_venta, coef),
                        )

            except Exception as e:
                resultado.errores.append(f"SKU {row.get('sku_master')}: {e}")
                log.exception(f"Error procesando SKU {row.get('sku_master')}")
