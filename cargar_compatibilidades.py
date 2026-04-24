"""
cargar_compatibilidades.py
==========================
CLI para cargar filas en la tabla `compatibilidades_vehiculares` desde un CSV.

Formato del CSV (UTF-8, separador coma, primera fila encabezado):

    sku_master,marca,modelo,anio_desde,anio_hasta,motor,notas

Columnas:
  - sku_master    (obligatorio): debe existir en la tabla productos
  - marca         (obligatorio): CHEVROLET, FORD, OPEL, DODGE, VW, MWM, etc.
  - modelo        (opcional)  : Apache, C-20, F-100, Opala, etc.
  - anio_desde    (opcional)  : entero (ej 1971). Vacío → NULL.
  - anio_hasta    (opcional)  : entero (ej 2001). Vacío → NULL.
  - motor         (opcional)  : "4 cilindros", "6.2 diesel", "OM 352", etc.
  - notas         (opcional)  : texto libre.

Uso:
  python cargar_compatibilidades.py data/compatibilidades_seed.csv
  python cargar_compatibilidades.py archivo.csv --reemplazar   # borra compat existentes del SKU antes de insertar
  python cargar_compatibilidades.py archivo.csv --dry-run      # muestra sin escribir

Por qué idempotente: si corrés sin --reemplazar dos veces, vas a duplicar filas.
Usá --reemplazar cuando estés cargando el estado definitivo de un SKU.
"""

from __future__ import annotations

import argparse
import csv
import sys
from typing import Optional

import db
from logger import get_logger

log = get_logger(__name__)


def _int_o_none(v) -> Optional[int]:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    try:
        return int(s)
    except ValueError:
        return None


def _resolver_producto_id(sku: str) -> Optional[int]:
    s = (sku or "").strip()
    if not s:
        return None
    with db.conexion() as c:
        variantes = [s.upper()]
        if s.isdigit() and len(s) < 4:
            variantes.append(s.zfill(4).upper())
        for v in variantes:
            r = c.execute(
                """SELECT id FROM productos
                    WHERE UPPER(sku_master)=? OR UPPER(sku_ml)=? OR UPPER(sku_proveedor)=?
                    LIMIT 1""",
                (v, v, v),
            ).fetchone()
            if r:
                return int(r["id"])
    return None


def cargar(path: str, *, reemplazar: bool = False, dry_run: bool = False) -> int:
    """
    Lee el CSV y carga filas en compatibilidades_vehiculares.
    Devuelve la cantidad de filas insertadas (0 en dry_run).
    """
    try:
        f = open(path, newline="", encoding="utf-8")
    except OSError as e:
        print(f"✗ No pude abrir {path}: {e}", file=sys.stderr)
        return -1

    with f:
        reader = csv.DictReader(f)
        filas = list(reader)

    if not filas:
        print(f"⚠ CSV vacío: {path}")
        return 0

    requeridas = {"sku_master", "marca"}
    faltan = requeridas - set(reader.fieldnames or [])
    if faltan:
        print(f"✗ Faltan columnas en el CSV: {sorted(faltan)}", file=sys.stderr)
        return -1

    # Agrupar por sku para facilitar el --reemplazar
    por_sku: dict[str, list[dict]] = {}
    for row in filas:
        sku = (row.get("sku_master") or "").strip()
        if not sku:
            continue
        por_sku.setdefault(sku, []).append(row)

    insertadas = 0
    skus_sin_producto: list[str] = []
    resumen_por_sku: list[tuple[str, int, int]] = []  # (sku, borradas, insertadas)

    with db.conexion() as c:
        for sku, rows in sorted(por_sku.items()):
            pid = _resolver_producto_id(sku)
            if pid is None:
                skus_sin_producto.append(sku)
                continue

            borradas = 0
            if reemplazar and not dry_run:
                cur = c.execute(
                    "DELETE FROM compatibilidades_vehiculares WHERE producto_id = ?",
                    (pid,),
                )
                borradas = cur.rowcount

            insertadas_sku = 0
            for row in rows:
                marca = (row.get("marca") or "").strip().upper()
                if not marca:
                    continue
                modelo = (row.get("modelo") or "").strip() or None
                ad = _int_o_none(row.get("anio_desde"))
                ah = _int_o_none(row.get("anio_hasta"))
                motor = (row.get("motor") or "").strip() or None
                notas = (row.get("notas") or "").strip() or None

                if dry_run:
                    insertadas_sku += 1
                    continue

                c.execute(
                    """
                    INSERT INTO compatibilidades_vehiculares
                        (producto_id, marca, modelo, anio_desde, anio_hasta, motor, notas)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (pid, marca, modelo, ad, ah, motor, notas),
                )
                insertadas_sku += 1

            insertadas += insertadas_sku
            resumen_por_sku.append((sku, borradas, insertadas_sku))

    # ===== Reporte =====
    print("=" * 70)
    print(f"  CARGA DE COMPATIBILIDADES {'(DRY RUN)' if dry_run else ''}")
    print(f"  Archivo: {path}")
    print(f"  Modo   : {'REEMPLAZAR existentes' if reemplazar else 'APPEND (se suman)'}")
    print("=" * 70)
    print(f"  {'SKU':<10} {'BORRADAS':>10} {'INSERTADAS':>12}")
    print("-" * 70)
    for sku, b, i in resumen_por_sku:
        print(f"  {sku:<10} {b:>10} {i:>12}")
    print("-" * 70)
    print(f"  TOTAL filas insertadas: {insertadas}")

    if skus_sin_producto:
        print()
        print(f"  ⚠ SKUs sin producto en DB ({len(skus_sin_producto)}):")
        for s in skus_sin_producto[:20]:
            print(f"    - {s}")

    return insertadas


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Cargar compatibilidades vehiculares desde un CSV."
    )
    ap.add_argument("csv", help="Ruta al archivo CSV.")
    ap.add_argument(
        "--reemplazar", action="store_true",
        help="Borrar las compatibilidades existentes del SKU antes de insertar (idempotente)."
    )
    ap.add_argument("--dry-run", action="store_true", help="Simular sin escribir en DB.")
    args = ap.parse_args()

    try:
        db.inicializar_db()
    except Exception as e:
        print(f"  ⚠ No pude inicializar DB: {e}", file=sys.stderr)

    insertadas = cargar(args.csv, reemplazar=args.reemplazar, dry_run=args.dry_run)
    return 0 if insertadas >= 0 else 1


if __name__ == "__main__":
    sys.exit(main())
