"""
publicar_10.py
==============
CLI para publicar en Mercado Libre los primeros N productos del catálogo que
todavía NO estén publicados (sku_ml vacío).

Reglas de negocio (primera tanda de prueba):
  - N = 10 por default (ajustable con --limite).
  - status = "paused" en ML → el item se crea pausado y lo activás vos
    desde el panel.
  - Sólo se publican productos con:
      * precio_venta > 0
      * stock_actual > 0
      * foto ZEN existente en data/fotos/<sku>/zen.png
      * categoría predicha por ML
  - Muestra la lista de candidatos + pide confirmación antes de publicar.

Uso:
    (venv) PS > python publicar_10.py --listar          # sólo mostrar los 10
    (venv) PS > python publicar_10.py --dry-run         # armar drafts sin llamar a ML
    (venv) PS > python publicar_10.py                   # publicar (pide confirmación)
    (venv) PS > python publicar_10.py --limite 3        # publicar sólo 3
    (venv) PS > python publicar_10.py --status active   # publicar activas (NO recomendado)
    (venv) PS > python publicar_10.py --si              # sin preguntar (batch script)
"""

from __future__ import annotations

import argparse
import sys
from typing import Optional

import db
from logger import get_logger
from ml import publicar as pubr

log = get_logger(__name__)


def _hr(c: str = "=", n: int = 78) -> str:
    return c * n


def _imprimir_candidatos(candidatos: list[dict]) -> None:
    print(_hr())
    print(f"  CANDIDATOS ({len(candidatos)}) — productos sin sku_ml, con precio y foto ZEN")
    print(_hr())
    print(f"  {'#':<3} {'SKU':<10} {'Precio':>12} {'Stock':>6} {'Fichas':>6}  Descripción")
    print(_hr("-"))
    for i, p in enumerate(candidatos, 1):
        precio = float(p.get("precio_venta") or 0)
        stock = int(p.get("stock_actual") or 0)
        n_f = int(p.get("n_fichas") or 0)
        desc = (p.get("descripcion") or "")[:46]
        sku = p.get("sku_master") or ""
        print(f"  {i:<3} {sku:<10} ${precio:>10,.2f} {stock:>6} {n_f:>6}  {desc}")
    print(_hr())


def _confirmar(msg: str) -> bool:
    try:
        r = input(f"{msg} (s/N): ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        return False
    return r in ("s", "si", "sí", "y", "yes")


def main() -> int:
    p = argparse.ArgumentParser(
        description="Publica en ML los primeros N productos sin sku_ml. "
                    "Por default los deja en 'paused' para revisión manual."
    )
    p.add_argument("--sku", type=str, default=None,
                   help="Lista de SKUs explícitos separados por coma. "
                        "Si se pasa, ignora el selector automático y publica "
                        "exactamente estos. Ej: --sku 0013,0020,0024")
    p.add_argument("--limite", type=int, default=10,
                   help="Cantidad máxima a publicar (default 10). "
                        "Ignorado si usás --sku.")
    p.add_argument("--status", choices=("paused", "active"),
                   default=pubr.PUBLICAR_STATUS_DEFAULT,
                   help="Estado en ML (default: paused).")
    p.add_argument("--listar", action="store_true",
                   help="Sólo muestra los candidatos, no publica nada.")
    p.add_argument("--dry-run", action="store_true",
                   help="Arma los drafts y valida, sin llamar a ML.")
    p.add_argument("--si", action="store_true",
                   help="No pide confirmación interactiva.")
    p.add_argument("--permitir-sin-foto", action="store_true",
                   help="Publica productos aun si no tienen foto ZEN (no recomendado).")
    p.add_argument("--diagnostico", action="store_true",
                   help="Muestra por qué no hay candidatos (conteos por filtro) y sale.")
    p.add_argument("--sugerir", action="store_true",
                   help="Sugiere los N mejores candidatos listos para publicar "
                        "(con precio>0, stock>0, foto ZEN y ficha técnica).")
    args = p.parse_args()

    # Asegurar schema actualizado (migraciones incrementales de columnas)
    # — idempotente, no reescribe datos si las tablas ya están al día.
    try:
        db.inicializar_db()
    except Exception as e:
        print(f"  ⚠ No se pudo asegurar el schema de la DB: {e}")
        return 1

    # ---- Sugerir candidatos ideales ----
    if args.sugerir:
        with __import__("db").conexion() as c:
            rows = c.execute("""
                SELECT p.sku_master, p.descripcion, p.marca_proveedor,
                       p.precio_venta, p.stock_actual,
                       (SELECT COUNT(*) FROM fichas_tecnicas ft WHERE ft.producto_id = p.id) AS n_fichas
                  FROM productos p
                 WHERE p.precio_venta > 0
                   AND p.stock_actual > 0
                 ORDER BY n_fichas DESC, p.sku_master
                 LIMIT 200
            """).fetchall()
        import os
        candidatos_ok = []
        for r in rows:
            sku = r["sku_master"]
            if (pubr.FOTOS_DIR_DEFAULT / sku / "zen.png").exists():
                candidatos_ok.append(dict(r))
            if len(candidatos_ok) >= args.limite:
                break
        if not candidatos_ok:
            print("✗ No encontré candidatos listos (precio>0 + stock>0 + foto ZEN + ficha).")
            return 1
        print(_hr())
        print(f"  SUGERENCIA — top {len(candidatos_ok)} candidatos listos para publicar")
        print(_hr())
        print(f"  {'SKU':<10} {'Precio':>12} {'Stock':>6} {'Fichas':>6}  Descripción")
        print(_hr("-"))
        for c in candidatos_ok:
            desc = (c.get("descripcion") or "")[:46]
            print(f"  {c['sku_master']:<10} ${c['precio_venta']:>10,.2f} "
                  f"{c['stock_actual']:>6} {c['n_fichas']:>6}  {desc}")
        sugerencia = ",".join(c["sku_master"] for c in candidatos_ok)
        print()
        print(f"  Para publicarlos como paused:")
        print(f'    python publicar_10.py --sku "{sugerencia}" --dry-run')
        print(f'    python publicar_10.py --sku "{sugerencia}"')
        print(_hr())
        return 0

    # ---- Diagnóstico: ver por qué no hay candidatos ----
    if args.diagnostico:
        diag = pubr.diagnostico_candidatos()
        print(_hr())
        print("  DIAGNÓSTICO — por qué el selector devuelve (o no) candidatos")
        print(_hr())
        print(f"  Total productos en DB                       : {diag['total_productos']:>6}")
        print(f"  Con sku_ml vacío (sin publicar todavía)     : {diag['sku_ml_vacio']:>6}")
        print(f"    + precio_venta > 0                        : {diag['con_precio']:>6}")
        print(f"    + stock_actual > 0                        : {diag['con_stock']:>6}")
        print(f"    + foto ZEN en data/fotos/<sku>/zen.png    : {diag['con_foto']:>6}  ← final")
        print(f"  Productos con foto ZEN (total, sin filtros) : {diag['productos_con_foto_en_total']:>6}")
        print()
        if diag["muestra_con_sku_ml"]:
            print(f"  Muestra de productos YA publicados (sku_ml lleno):")
            for r in diag["muestra_con_sku_ml"]:
                print(f"    - {r['sku_master']:<10} → sku_ml='{r['sku_ml']}'")
        if diag["muestra_sin_precio"]:
            print(f"\n  Muestra de productos sin precio (precio_venta <= 0):")
            for r in diag["muestra_sin_precio"]:
                print(f"    - {r['sku_master']:<10} → precio={r['precio_venta']}")
        if diag["muestra_sin_stock"]:
            print(f"\n  Muestra de productos sin stock (stock_actual <= 0):")
            for r in diag["muestra_sin_stock"]:
                print(f"    - {r['sku_master']:<10} → stock={r['stock_actual']}")
        print(_hr())
        return 0

    skip_sin_foto = not args.permitir_sin_foto

    # 1) Seleccionar candidatos
    if args.sku:
        skus_list = [s.strip() for s in args.sku.split(",") if s.strip()]
        # Si vinieron truncados por PowerShell (sin comillas) avisamos al usuario
        truncados = [s for s in skus_list if s.isdigit() and len(s) < 4]
        if truncados:
            print(f"  ⚠ Detecté SKUs sin ceros a la izquierda ({truncados}). "
                  "PowerShell se los comió — para evitarlo pasá --sku entre comillas:")
            print(f"    python publicar_10.py --sku \"0013,0020,0024\" --dry-run")
            print(f"  Voy a intentar matchear agregando padding (13 → 0013).")
        print(f"  Modo --sku: publicando exactamente estos {len(skus_list)} SKUs → {skus_list}")
        candidatos = pubr.seleccionar_por_skus(skus_list)
        # Para reportar faltantes, consideramos encontrado si cualquier variante matchea
        def _match(sku_pedido: str, p: dict) -> bool:
            variantes = {sku_pedido.upper()}
            if sku_pedido.isdigit() and len(sku_pedido) < 4:
                variantes.add(sku_pedido.zfill(4).upper())
            for campo in ("sku_master", "sku_ml", "sku_proveedor"):
                if (p.get(campo) or "").upper() in variantes:
                    return True
            return False
        faltantes = [s for s in skus_list if not any(_match(s, p) for p in candidatos)]
        if faltantes:
            print(f"  ⚠ SKUs NO encontrados en DB (se saltearán): {faltantes}")
    else:
        candidatos = pubr.seleccionar_no_publicados(limite=args.limite)

    if not candidatos:
        print("✗ No hay productos candidatos. Desglose de filtros:")
        diag = pubr.diagnostico_candidatos()
        print(f"    Total productos                 : {diag['total_productos']}")
        print(f"    Con sku_ml vacío                : {diag['sku_ml_vacio']}")
        print(f"    + precio > 0                    : {diag['con_precio']}")
        print(f"    + stock > 0                     : {diag['con_stock']}")
        print(f"    + foto ZEN presente             : {diag['con_foto']}  ← bloqueo final")
        print()
        print("  Para ver muestras de qué queda afuera y por qué, corré:")
        print("    python publicar_10.py --diagnostico")
        print()
        print("  O bien elegí a mano los SKUs que querés publicar:")
        print("    python publicar_10.py --sku 0013,0020,0024,0025,0028 --dry-run")
        return 1

    _imprimir_candidatos(candidatos)

    # 2) Modo listar: salir acá
    if args.listar:
        print(f"\n  Modo --listar: no se publica nada.")
        return 0

    # 3) Confirmación
    accion = "PUBLICAR en ML" if not args.dry_run else "armar drafts (dry-run, sin ML)"
    print(f"\n  Se va a {accion} con status='{args.status}' "
          f"({len(candidatos)} productos).")
    if not args.si and not args.dry_run:
        if not _confirmar("  ¿Confirmás?"):
            print("  (cancelado)")
            return 0

    # 4) Publicar
    print()
    print(_hr())
    print(f"  PUBLICANDO — status={args.status} dry_run={args.dry_run}")
    print(_hr())

    resultados: list[pubr.ResultadoPublicacion] = []
    for i, prod in enumerate(candidatos, 1):
        sku = prod.get("sku_master") or ""
        print(f"\n  [{i}/{len(candidatos)}] SKU {sku} — {(prod.get('descripcion') or '')[:50]}")
        r = pubr.publicar_producto(
            producto_id=prod["id"],
            status=args.status,
            skip_sin_foto=skip_sin_foto,
            dry_run=args.dry_run,
        )
        resultados.append(r)
        if r.ok:
            if args.dry_run:
                print(f"    ✓ {r.mensaje}")
            else:
                print(f"    ✓ {r.mensaje}")
                print(f"      ml_item_id : {r.ml_item_id}")
                print(f"      permalink  : {r.ml_permalink}")
                print(f"      status ML  : {r.ml_status}")
        elif r.skipped:
            print(f"    ⏭  {r.mensaje}")
        else:
            print(f"    ✗ {r.mensaje}")

    # 5) Resumen
    print()
    print(_hr())
    print("  RESUMEN")
    print(_hr())
    ok = sum(1 for r in resultados if r.ok)
    skipped = sum(1 for r in resultados if r.skipped)
    err = sum(1 for r in resultados if not r.ok and not r.skipped)
    print(f"  Publicados OK     : {ok}")
    print(f"  Salteados         : {skipped}")
    print(f"  Con error         : {err}")
    if err:
        print("\n  Errores:")
        for r in resultados:
            if not r.ok and not r.skipped:
                print(f"    - SKU {r.sku}: {r.mensaje}")
    if ok and not args.dry_run:
        print("\n  Items publicados (paused en ML — activalos desde tu panel):")
        for r in resultados:
            if r.ok:
                print(f"    • {r.sku} → {r.ml_item_id}  {r.ml_permalink or ''}")

    print(_hr())
    if args.dry_run:
        print("  Dry-run: no se publicó nada en ML.")
    print(_hr())

    return 0 if err == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
