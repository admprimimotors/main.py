"""
pausar_items.py
===============
CLI para pausar (o cerrar) ítems ya publicados en Mercado Libre usando su
MLA ID. Útil para:
  - Recuperar un ítem "huérfano" creado por un intento de publicación que
    falló del lado local (DB), pero que sí quedó vivo en ML.
  - Pausar una tanda manualmente si un PUT no se completó en su momento.

Uso:
    (venv) PS > python pausar_items.py MLA1756752843
    (venv) PS > python pausar_items.py MLA1756752843 MLA9999999999
    (venv) PS > python pausar_items.py MLA1756752843 --status closed
    (venv) PS > python pausar_items.py MLA1756752843 --dry-run
"""

from __future__ import annotations

import argparse
import sys

from logger import get_logger
from ml import client as ml_client

log = get_logger(__name__)


def _hr(c: str = "=", n: int = 72) -> str:
    return c * n


def pausar_item(item_id: str, nuevo_status: str = "paused") -> dict:
    """
    Cambia el status de un item en ML con PUT /items/{id}. Devuelve el dict
    de respuesta o lanza excepción. No captura errores HTTP.
    """
    return ml_client.put(f"/items/{item_id}", json_body={"status": nuevo_status})


def consultar_item(item_id: str) -> dict:
    """GET /items/{id} — devuelve el estado actual."""
    return ml_client.get(f"/items/{item_id}")


def main() -> int:
    p = argparse.ArgumentParser(
        description="Cambia el status de uno o varios items ML (paused/active/closed)."
    )
    p.add_argument("items", nargs="+", help="MLA IDs a modificar (ej: MLA1756752843)")
    p.add_argument("--status", choices=("paused", "active", "closed"),
                   default="paused",
                   help="Nuevo status (default: paused).")
    p.add_argument("--dry-run", action="store_true",
                   help="Sólo consulta el estado actual, no modifica nada.")
    args = p.parse_args()

    print(_hr())
    print(f"  {'CONSULTA' if args.dry_run else 'CAMBIO DE STATUS'} — {len(args.items)} item(s) → '{args.status}'")
    print(_hr())

    ok = 0
    err = 0
    for item_id in args.items:
        item_id = item_id.strip().upper()
        if not item_id:
            continue
        try:
            estado_previo = consultar_item(item_id)
            if not isinstance(estado_previo, dict):
                print(f"  ✗ {item_id}: respuesta inesperada.")
                err += 1
                continue
            st = estado_previo.get("status")
            title = estado_previo.get("title") or estado_previo.get("family_name") or "?"
            perm = estado_previo.get("permalink") or ""
            print(f"\n  {item_id}")
            print(f"    título  : {title}")
            print(f"    status  : {st}")
            if perm:
                print(f"    URL     : {perm}")

            if args.dry_run:
                print(f"    [dry-run] pediría: status → {args.status}")
                ok += 1
                continue

            if st == args.status:
                print(f"    = ya está en '{args.status}', no se toca.")
                ok += 1
                continue

            resp = pausar_item(item_id, args.status)
            nuevo_st = resp.get("status") if isinstance(resp, dict) else None
            print(f"    ✓ status actualizado: {st} → {nuevo_st or args.status}")
            ok += 1
        except Exception as e:
            print(f"  ✗ {item_id}: {e}")
            err += 1

    print()
    print(_hr())
    print(f"  RESUMEN: OK={ok}  errores={err}")
    print(_hr())
    return 0 if err == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
