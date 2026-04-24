"""
inspeccionar_item.py
====================
Dump completo de un item de Mercado Libre (propio o ajeno) usando nuestra
auth. Útil para analizar publicaciones de referencia y ver:
  - qué atributos estructurados tienen
  - qué formato de título y descripción usan
  - cuántas fotos
  - con qué listing type, envío, garantía

Uso:
    (venv) PS > python inspeccionar_item.py MLA1746973751
    (venv) PS > python inspeccionar_item.py MLA1746973751 --json  > ref.json
    (venv) PS > python inspeccionar_item.py MLA1746973751 --desc
"""

from __future__ import annotations

import argparse
import json
import sys

from logger import get_logger
from ml import client as ml_client

log = get_logger(__name__)


def _hr(c: str = "=", n: int = 78) -> str:
    return c * n


def _imprimir(item: dict, descripcion: str | None = None) -> None:
    print(_hr())
    print(f"  ITEM {item.get('id')}  —  {item.get('status', '?')}")
    print(_hr())
    print(f"  Título        : {item.get('title')}")
    print(f"  Family name   : {item.get('family_name') or '(none)'}")
    print(f"  Categoría     : {item.get('category_id')}")
    print(f"  Condición     : {item.get('condition')}")
    print(f"  Listing type  : {item.get('listing_type_id')}")
    print(f"  Precio        : {item.get('currency_id')} {item.get('price')}")
    print(f"  Stock         : {item.get('available_quantity')}  (vendidos: {item.get('sold_quantity', 0)})")
    ship = item.get("shipping") or {}
    print(f"  Shipping mode : {ship.get('mode')}  free={ship.get('free_shipping')}  logistic={ship.get('logistic_type')}")
    print(f"  Permalink     : {item.get('permalink')}")
    # Health / calidad
    health = item.get("health")
    if health is not None:
        print(f"  Health score  : {health}")
    # Warranty
    for t in (item.get("sale_terms") or []):
        print(f"  Sale term     : {t.get('id')} = {t.get('value_name')}")

    # Pictures
    pics = item.get("pictures") or []
    print(f"\n  Pictures ({len(pics)}):")
    for i, p in enumerate(pics, 1):
        print(f"    {i}. id={p.get('id')}  size={p.get('size')}  max={p.get('max_size')}  url={p.get('secure_url') or p.get('url')}")

    # Attributes estructurados
    atts = item.get("attributes") or []
    print(f"\n  Attributes ({len(atts)}):")
    for a in atts:
        aid = a.get("id")
        name = a.get("name")
        vn = a.get("value_name")
        vid = a.get("value_id")
        vs = a.get("value_struct")
        extra = ""
        if vid:
            extra = f"  (value_id={vid})"
        if vs:
            extra += f"  struct={vs}"
        print(f"    {aid:<34} {name!r:40} = {vn!r}{extra}")

    # Variations / combinations (por si el item los tiene)
    var = item.get("variations") or []
    if var:
        print(f"\n  Variations: {len(var)}")

    # Tags
    tags = item.get("tags") or []
    if tags:
        print(f"\n  Tags: {tags}")

    # Descripción
    if descripcion is not None:
        print()
        print(_hr())
        print("  DESCRIPCIÓN")
        print(_hr())
        print(descripcion)

    print(_hr())


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Dump de un item ML (propio o ajeno). Útil como referencia."
    )
    ap.add_argument("item_id", help="Ej: MLA1746973751")
    ap.add_argument("--json", action="store_true", help="Imprime el JSON crudo del item.")
    ap.add_argument("--desc", action="store_true", help="También fetch /items/{id}/description.")
    args = ap.parse_args()

    item_id = args.item_id.strip().upper()
    if not item_id.startswith("MLA"):
        item_id = "MLA" + item_id.lstrip("#")

    try:
        item = ml_client.get(f"/items/{item_id}")
    except Exception as e:
        print(f"✗ Error al obtener {item_id}: {e}", file=sys.stderr)
        return 2

    if not isinstance(item, dict):
        print(f"✗ Respuesta inesperada: {item!r}", file=sys.stderr)
        return 2

    descripcion_texto: str | None = None
    if args.desc:
        try:
            d = ml_client.get(f"/items/{item_id}/description")
            if isinstance(d, dict):
                descripcion_texto = d.get("plain_text") or d.get("text") or ""
        except Exception as e:
            log.warning(f"No se pudo leer la descripción: {e}")

    if args.json:
        out = {"item": item}
        if descripcion_texto is not None:
            out["description_plain_text"] = descripcion_texto
        print(json.dumps(out, ensure_ascii=False, indent=2))
    else:
        _imprimir(item, descripcion=descripcion_texto)

    return 0


if __name__ == "__main__":
    sys.exit(main())
