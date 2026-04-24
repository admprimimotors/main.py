"""
inventory/search.py
===================
Funciones de búsqueda y consulta sobre el catálogo local.

Pensado para usarse desde la CLI (buscar_producto.py) y más adelante desde la UI web.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import db


@dataclass
class ResumenCatalogo:
    total_productos: int
    con_stock: int
    sin_stock: int
    por_pedido: int
    total_valor_costo: float
    total_valor_venta: float
    categorias: list[tuple[str, int]]  # (nombre, cantidad)
    marcas_auto: list[tuple[str, int]]  # top marcas


def resumen_catalogo() -> ResumenCatalogo:
    with db.conexion() as c:
        total = c.execute("SELECT COUNT(*) AS n FROM productos WHERE activo = 1").fetchone()["n"]
        con_stock = c.execute("SELECT COUNT(*) AS n FROM productos WHERE activo = 1 AND stock_actual > 0").fetchone()["n"]
        sin_stock = c.execute("SELECT COUNT(*) AS n FROM productos WHERE activo = 1 AND stock_actual <= 0").fetchone()["n"]
        por_pedido = c.execute("SELECT COUNT(*) AS n FROM productos WHERE activo = 1 AND por_pedido = 1").fetchone()["n"]

        valores = c.execute(
            """
            SELECT
                COALESCE(SUM(costo * stock_actual), 0)        AS valor_costo,
                COALESCE(SUM(precio_venta * stock_actual), 0) AS valor_venta
            FROM productos WHERE activo = 1 AND stock_actual > 0
            """
        ).fetchone()

        categorias = c.execute(
            """
            SELECT cat.nombre AS nombre, COUNT(p.id) AS n
            FROM productos p
            LEFT JOIN categorias cat ON cat.id = p.categoria_id
            WHERE p.activo = 1
            GROUP BY cat.nombre
            ORDER BY n DESC
            """
        ).fetchall()

        marcas = c.execute(
            """
            SELECT marca_auto, COUNT(*) AS n
            FROM productos
            WHERE activo = 1 AND marca_auto IS NOT NULL AND marca_auto != ''
            GROUP BY marca_auto
            ORDER BY n DESC
            LIMIT 15
            """
        ).fetchall()

    return ResumenCatalogo(
        total_productos=total,
        con_stock=con_stock,
        sin_stock=sin_stock,
        por_pedido=por_pedido,
        total_valor_costo=float(valores["valor_costo"] or 0),
        total_valor_venta=float(valores["valor_venta"] or 0),
        categorias=[(c["nombre"] or "Sin categoría", c["n"]) for c in categorias],
        marcas_auto=[(m["marca_auto"], m["n"]) for m in marcas],
    )


def buscar(
    texto: Optional[str] = None,
    *,
    solo_con_stock: bool = False,
    solo_sin_publicacion: bool = False,
    marca_auto: Optional[str] = None,
    categoria: Optional[str] = None,
    limite: int = 50,
) -> list[dict]:
    """
    Búsqueda libre del catálogo. Filtra por texto en SKU o descripción.
    """
    with db.conexion() as c:
        sql = [
            """
            SELECT p.id, p.sku_master, p.sku_proveedor, p.descripcion, p.marca_auto,
                   p.costo, p.precio_venta, p.stock_actual, p.por_pedido,
                   cat.nombre AS categoria,
                   ml.ml_item_id, ml.permalink, ml.status_ml
            FROM productos p
            LEFT JOIN categorias       cat ON cat.id = p.categoria_id
            LEFT JOIN publicaciones_ml ml  ON ml.producto_id = p.id
            WHERE p.activo = 1
            """
        ]
        params: list = []

        if texto:
            sql.append("AND (p.sku_master LIKE ? OR p.sku_proveedor LIKE ? OR p.descripcion LIKE ?)")
            like = f"%{texto.upper()}%"
            params += [like, like, like]

        if solo_con_stock:
            sql.append("AND p.stock_actual > 0")

        if solo_sin_publicacion:
            sql.append("AND ml.ml_item_id IS NULL")

        if marca_auto:
            sql.append("AND UPPER(p.marca_auto) LIKE ?")
            params.append(f"%{marca_auto.upper()}%")

        if categoria:
            sql.append("AND cat.nombre = ?")
            params.append(categoria)

        sql.append("ORDER BY p.sku_master LIMIT ?")
        params.append(limite)

        rows = c.execute(" ".join(sql), params).fetchall()
        return [dict(r) for r in rows]
