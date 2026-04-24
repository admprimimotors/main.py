"""
nueva_publicacion.py
====================
CLI interactiva para **previsualizar** (dry-run) cómo se publicaría un producto
del catálogo en Mercado Libre.

NO publica nada. Sólo arma un draft, muestra el título, la descripción, la
categoría predicha, los atributos y el payload JSON que se mandaría a /items.

Uso:
    (venv) PS > python nueva_publicacion.py
    (venv) PS > python nueva_publicacion.py --sku 0013
    (venv) PS > python nueva_publicacion.py --producto-id 15
    (venv) PS > python nueva_publicacion.py --sku 0013 --offline      (sin llamar a ML)
    (venv) PS > python nueva_publicacion.py --sku 0013 --guardar      (además guarda el draft)
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Optional

import db
from logger import get_logger
from ml import publicaciones as pub

log = get_logger(__name__)


# ==========================================================
# Helpers
# ==========================================================
def _linea(char: str = "=", largo: int = 72) -> str:
    return char * largo


def _buscar_producto_por_sku(sku: str) -> Optional[dict]:
    """Busca por sku_master, sku_ml o sku_proveedor (case-insensitive)."""
    sku_norm = sku.strip().upper()
    with db.conexion() as c:
        row = c.execute(
            """
            SELECT * FROM productos
            WHERE UPPER(sku_master)   = ?
               OR UPPER(sku_ml)       = ?
               OR UPPER(sku_proveedor)= ?
            LIMIT 1
            """,
            (sku_norm, sku_norm, sku_norm),
        ).fetchone()
    return dict(row) if row else None


def _pedir_sku_interactivo() -> Optional[str]:
    try:
        sku = input("Ingresá el SKU del producto (o Enter para cancelar): ").strip()
    except (EOFError, KeyboardInterrupt):
        return None
    return sku or None


# ==========================================================
# Impresión del draft
# ==========================================================
def _imprimir_draft(draft: pub.DraftPublicacion, producto: dict) -> None:
    print(_linea())
    print("  DRAFT DE PUBLICACIÓN — PREVIEW (no se publica)")
    print(_linea())

    print(f"\n  Producto local")
    print(f"    ID             : {producto['id']}")
    print(f"    SKU master     : {producto.get('sku_master')}")
    print(f"    SKU ML         : {producto.get('sku_ml')}")
    print(f"    Descripción    : {producto.get('descripcion')}")
    print(f"    Marca repuesto : {producto.get('marca_proveedor')}")
    print(f"    Marca auto     : {producto.get('marca_auto')}")
    print(f"    Rubro          : {producto.get('rubro_origen')}")
    print(f"    Precio venta   : ${float(producto.get('precio_venta') or 0):,.2f}")
    print(f"    Stock actual   : {producto.get('stock_actual')}")

    print(f"\n  Publicación propuesta")
    print(f"    Título         : {draft.titulo}  ({len(draft.titulo)}/60 chars)")
    print(f"    Categoría ML   : {draft.category_id_ml or '(no predicha)'}")
    print(f"    Listing type   : {draft.listing_type_id}")
    print(f"    Condición      : {draft.condition_ml}")
    print(f"    Precio         : ${draft.precio:,.2f} {draft.currency}")
    print(f"    Stock publicado: {draft.stock}")
    print(f"    Envíos         : {draft.shipping_mode}")
    print(f"    Garantía       : {draft.warranty_type} — {draft.warranty_time}")
    print(f"    Estado         : {draft.estado}")
    if draft.mensaje_error:
        print(f"    ⚠ Aviso        : {draft.mensaje_error}")

    print(f"\n  Descripción:")
    for linea in draft.descripcion.splitlines():
        print(f"    {linea}")

    print(f"\n  Atributos ({len(draft.atributos)}):")
    if not draft.atributos:
        print("    (vacío)")
    else:
        for a in draft.atributos:
            print(f"    - {a.get('id'):<20} = {a.get('value_name')}")

    print(f"\n  Fotos: {len(draft.pictures)}  (Chunk B: extracción desde PDF ZEN + carpeta local SKU)")

    print(f"\n  Payload JSON que se mandaría a POST /items:")
    print(_linea("-"))
    print(json.dumps(draft.to_ml_payload(), ensure_ascii=False, indent=2))
    print(_linea("-"))


# ==========================================================
# Main
# ==========================================================
def main() -> int:
    parser = argparse.ArgumentParser(
        description="Previsualiza (dry-run) una publicación ML para un producto del catálogo."
    )
    parser.add_argument("--sku", type=str, help="SKU master / ML / proveedor")
    parser.add_argument("--producto-id", type=int, help="ID interno del producto")
    parser.add_argument(
        "--offline",
        action="store_true",
        help="No llama a la API de ML (no predice categoría ni lee atributos).",
    )
    parser.add_argument(
        "--guardar",
        action="store_true",
        help="Además de mostrar, guarda el draft en publicaciones_drafts.",
    )
    args = parser.parse_args()

    producto: Optional[dict] = None

    if args.producto_id is not None:
        with db.conexion() as c:
            row = c.execute("SELECT * FROM productos WHERE id = ?", (args.producto_id,)).fetchone()
        if row is None:
            print(f"✗ No existe un producto con id={args.producto_id}.")
            return 1
        producto = dict(row)
    else:
        sku = args.sku or _pedir_sku_interactivo()
        if not sku:
            print("  (cancelado)")
            return 0
        producto = _buscar_producto_por_sku(sku)
        if producto is None:
            print(f"✗ No encontré ningún producto con SKU '{sku}'.")
            return 1

    print(_linea())
    print(f"  Producto encontrado: {producto['descripcion']} (id={producto['id']})")
    print(_linea())

    if not args.offline:
        print("\n  Consultando Mercado Libre (predictor de categoría + atributos)…")

    try:
        draft = pub.construir_draft(producto["id"], predecir_cat=not args.offline)
    except pub.PublicacionError as e:
        print(f"✗ Error: {e}")
        return 1

    _imprimir_draft(draft, producto)

    if args.guardar:
        draft_id = pub.guardar_draft(draft)
        print(f"\n  ✓ Draft guardado con id={draft_id} en publicaciones_drafts.")
    else:
        print("\n  (no se guardó — agregá --guardar si querés persistirlo)")

    print()
    print(_linea())
    print("  Dry-run completo. Ninguna publicación fue creada en Mercado Libre.")
    print(_linea())
    return 0


if __name__ == "__main__":
    sys.exit(main())
