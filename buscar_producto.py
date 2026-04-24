"""
buscar_producto.py
==================
CLI simple para buscar productos en el catálogo local.

Uso:
    python buscar_producto.py
    python buscar_producto.py impulsor chev
    python buscar_producto.py 0013
    python buscar_producto.py --sin-publicacion
"""

from __future__ import annotations

import argparse
import sys

from inventory.search import buscar, resumen_catalogo


def _mostrar_resultado(r: dict) -> None:
    stock = r["stock_actual"]
    por_pedido = bool(r["por_pedido"])
    estado_stock = f"stock {stock}" if stock > 0 else ("POR PEDIDO" if por_pedido else "sin stock")
    en_ml = "✓ ML" if r.get("ml_item_id") else "  – "

    categoria = r.get("categoria") or "-"
    marca_auto = r.get("marca_auto") or "-"

    print(f"[{en_ml}] {r['sku_master']:<12} {r['sku_proveedor'] or '':<12} {categoria[:20]:<20} {marca_auto[:15]:<15}")
    print(f"         {r['descripcion'][:90]}")
    print(f"         costo ${r['costo']:>10,.2f}   venta ${r['precio_venta']:>10,.2f}   {estado_stock}")
    if r.get("permalink"):
        print(f"         🔗 {r['permalink']}")
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description="Buscar productos en el catálogo de Primi Motors.")
    parser.add_argument("texto", nargs="*", help="Texto a buscar (SKU, marca, descripción).")
    parser.add_argument("--con-stock",        action="store_true", help="Solo productos con stock > 0")
    parser.add_argument("--sin-publicacion",  action="store_true", help="Solo productos sin publicación en ML")
    parser.add_argument("--marca",            type=str,            help="Filtrar por marca de auto (FORD, CHEVROLET...)")
    parser.add_argument("--categoria",        type=str,            help="Filtrar por nombre de categoría exacto")
    parser.add_argument("--limite",           type=int, default=30, help="Cantidad máxima de resultados (default 30)")
    parser.add_argument("--resumen",          action="store_true", help="Mostrar resumen del catálogo y salir")
    args = parser.parse_args()

    if args.resumen:
        r = resumen_catalogo()
        print(f"Total productos:     {r.total_productos}")
        print(f"  con stock:         {r.con_stock}")
        print(f"  sin stock:         {r.sin_stock}")
        print(f"  por pedido:        {r.por_pedido}")
        print(f"Valor costo:         ${r.total_valor_costo:,.2f}")
        print(f"Valor venta:         ${r.total_valor_venta:,.2f}")
        print()
        print("Categorías:")
        for nombre, n in r.categorias:
            print(f"  {nombre:<40} {n}")
        return

    texto = " ".join(args.texto).strip() if args.texto else None

    resultados = buscar(
        texto=texto,
        solo_con_stock=args.con_stock,
        solo_sin_publicacion=args.sin_publicacion,
        marca_auto=args.marca,
        categoria=args.categoria,
        limite=args.limite,
    )

    if not resultados:
        print("No se encontraron productos con esos criterios.")
        sys.exit(0)

    print(f"Encontrados {len(resultados)} productos:")
    print()
    for r in resultados:
        _mostrar_resultado(r)


if __name__ == "__main__":
    main()
