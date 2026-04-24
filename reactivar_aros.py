"""
reactivar_aros.py
=================
CLI para reactivar publicaciones ML y actualizar stock en batch, usando el
SELLER_SKU (columna CODIGO del Excel) como llave.

Uso esperado:
  1) Reporte en seco (no toca ML):
        python reactivar_aros.py data/aros_stock.csv --dry-run
     Te muestra cuántos va a reactivar, cuántos no encuentra, y escribe un
     CSV con el detalle.
  2) Ejecución real (pide confirmación):
        python reactivar_aros.py data/aros_stock.csv
     Pide "s" para confirmar antes de hacer los PUT.
  3) Ejecución automática (sin preguntar — típica para scripts):
        python reactivar_aros.py data/aros_stock.csv --si

Input CSV:
  - Debe tener columnas `CODIGO` y `STOCK` (case-insensitive).
  - Si tenés un .xls/.xlsx, convertilo primero (File → Save As → CSV UTF-8),
    o corré el script con --excel que intenta abrirlo directo.

Qué hace por cada fila:
  1. Si STOCK < 1 → SKIP (no tocamos los que están en 0: fueron la decisión
     de negocio "solo reactivar los que tengan unidades para vender").
  2. Busca el ml_item_id con GET /users/me/items/search?seller_sku=<CODIGO>.
     Puede devolver:
       - 1 item  → OK, lo actualizamos
       - N items → WARNING, actualizamos TODOS (si hay duplicados reales
         en tu cuenta, conviene consolidarlos después a mano)
       - 0 items → no-encontrado (lo reportamos y seguimos)
  3. PUT /items/{ml_item_id} con {"available_quantity": N, "status": "active"}.
     ML acepta ambos campos en la misma llamada. Idempotente: si ya estaba
     activo con stock OK, el PUT es no-op.

Rate limiting:
  ML permite ~60 req/min holgados por token. Con sleep de 0.3s entre
  llamadas estamos muy por debajo. Para 474 items: ~5 minutos.

Resumen final:
  - Escribe reactivar_aros_REPORTE_<timestamp>.csv con una fila por CODIGO:
    codigo, stock, ml_item_id, accion, resultado, mensaje.
"""

from __future__ import annotations

import argparse
import csv
import datetime
import sys
import time
from pathlib import Path
from typing import Optional

from logger import get_logger
from ml import client as ml_client

log = get_logger(__name__)

SLEEP_ENTRE_CALLS = 0.3   # segundos — cortesía con la API

# Cache del user_id numérico — ML rechaza /users/me/items/search con 400
# "UserID is mandatory and must be integer". Hay que usar el numérico.
_USER_ID: Optional[int] = None


def _obtener_user_id() -> int:
    """Cachea el user_id numérico llamando a /users/me una sola vez."""
    global _USER_ID
    if _USER_ID is not None:
        return _USER_ID
    r = ml_client.get("/users/me") or {}
    uid = r.get("id")
    if not isinstance(uid, int):
        raise RuntimeError(f"No pude obtener user_id de /users/me: {r!r}")
    _USER_ID = uid
    log.info(f"user_id ML: {uid} (nickname={r.get('nickname')})")
    return uid


# ==========================================================
# Lectura de input (CSV preferido, xls/xlsx como fallback)
# ==========================================================
def _leer_input(path: Path, excel_mode: bool) -> list[dict]:
    """
    Devuelve una lista de dicts con claves 'codigo' y 'stock' (int).
    Consolida duplicados tomando el MAX del stock.
    """
    filas: list[tuple[str, int]] = []

    if excel_mode or path.suffix.lower() in (".xls", ".xlsx"):
        try:
            import pandas as pd  # lazy import
        except ImportError:
            print("✗ Falta pandas/openpyxl. Instalá: pip install pandas openpyxl xlrd",
                  file=sys.stderr)
            sys.exit(2)
        df = pd.read_excel(path)
        df.columns = [str(c).strip().upper() for c in df.columns]
        if "CODIGO" not in df.columns or "STOCK" not in df.columns:
            print(f"✗ El archivo debe tener columnas CODIGO y STOCK. Encontré: {list(df.columns)}",
                  file=sys.stderr)
            sys.exit(2)
        for _, r in df.iterrows():
            cod = str(r["CODIGO"]).strip()
            if not cod or cod.lower() == "nan":
                continue
            try:
                stk = int(float(r["STOCK"]))
            except (TypeError, ValueError):
                stk = 0
            filas.append((cod, stk))
    else:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            # Mapeo case-insensitive de columnas
            cols = {c.strip().upper(): c for c in (reader.fieldnames or [])}
            if "CODIGO" not in cols or "STOCK" not in cols:
                print(f"✗ El CSV debe tener columnas CODIGO y STOCK. Encontré: {reader.fieldnames}",
                      file=sys.stderr)
                sys.exit(2)
            for row in reader:
                cod = (row.get(cols["CODIGO"]) or "").strip()
                if not cod:
                    continue
                try:
                    stk = int(float(row.get(cols["STOCK"]) or 0))
                except ValueError:
                    stk = 0
                filas.append((cod, stk))

    # Consolidar duplicados con MAX
    por_cod: dict[str, int] = {}
    for cod, stk in filas:
        por_cod[cod] = max(por_cod.get(cod, 0), stk)
    return [{"codigo": c, "stock": s} for c, s in por_cod.items()]


# ==========================================================
# Resolver CODIGO → ml_item_id(s)
# ==========================================================
def _buscar_item_ids(seller_sku: str) -> list[str]:
    """
    GET /users/{user_id}/items/search?seller_sku=<seller_sku>
    Devuelve lista de ml_item_ids matcheados. Puede ser 0, 1 o varios.

    IMPORTANTE: ML devuelve 400 si se usa /users/me/... en este endpoint —
    hay que pasar el user_id numérico. Por eso lo cacheamos al inicio.
    """
    try:
        uid = _obtener_user_id()
        resp = ml_client.get(f"/users/{uid}/items/search",
                             params={"seller_sku": seller_sku, "limit": 50})
    except Exception as e:
        log.warning(f"search seller_sku={seller_sku} falló: {e}")
        return []
    if not isinstance(resp, dict):
        return []
    return list(resp.get("results") or [])


# ==========================================================
# PUT item con stock + activar
# ==========================================================
def _actualizar_item(ml_item_id: str, stock: int) -> tuple[bool, str]:
    """
    PUT /items/{id} con available_quantity + status=active.
    Devuelve (ok, mensaje).
    """
    body = {"available_quantity": int(stock), "status": "active"}
    try:
        resp = ml_client.put(f"/items/{ml_item_id}", json_body=body)
    except Exception as e:
        msg = str(e)[:200]
        return False, msg
    # Si ML devuelve el item actualizado sin error, OK
    if isinstance(resp, dict) and resp.get("id"):
        actual_status = resp.get("status") or "?"
        actual_qty = resp.get("available_quantity", "?")
        return True, f"status={actual_status} qty={actual_qty}"
    return True, "OK (sin eco)"


# ==========================================================
# Loop principal
# ==========================================================
def procesar(
    rows: list[dict],
    *,
    dry_run: bool,
    limite: Optional[int] = None,
) -> tuple[list[dict], dict]:
    """
    Procesa las filas y devuelve (reporte, resumen).
    reporte = lista de dicts por CODIGO con lo que pasó.
    """
    reporte: list[dict] = []
    resumen = {
        "total": 0, "reactivados": 0, "stock_0_skip": 0,
        "no_encontrados": 0, "errores": 0, "items_tocados": 0,
        "codigos_con_multiples_items": 0,
    }

    procesar_filas = [r for r in rows if r["stock"] >= 1]
    skip_filas = [r for r in rows if r["stock"] < 1]

    resumen["stock_0_skip"] = len(skip_filas)
    resumen["total"] = len(rows)

    # Los que se saltean entran al reporte como info
    for r in skip_filas:
        reporte.append({
            "codigo": r["codigo"], "stock": r["stock"], "ml_item_id": "",
            "accion": "skip", "resultado": "stock<1 — no se toca",
            "mensaje": "",
        })

    if limite:
        procesar_filas = procesar_filas[:limite]

    for i, r in enumerate(procesar_filas, 1):
        codigo = r["codigo"]
        stock = r["stock"]
        if i % 25 == 0 or i == len(procesar_filas):
            print(f"  [{i}/{len(procesar_filas)}] procesando {codigo} (stock={stock})...")

        if dry_run:
            # Igual llamamos al search para dar una estimación real
            ids = _buscar_item_ids(codigo)
            if len(ids) == 0:
                resumen["no_encontrados"] += 1
                reporte.append({
                    "codigo": codigo, "stock": stock, "ml_item_id": "",
                    "accion": "dry-run", "resultado": "no-encontrado",
                    "mensaje": "No existe publicación con ese SELLER_SKU",
                })
            else:
                resumen["reactivados"] += 1
                resumen["items_tocados"] += len(ids)
                if len(ids) > 1:
                    resumen["codigos_con_multiples_items"] += 1
                reporte.append({
                    "codigo": codigo, "stock": stock,
                    "ml_item_id": ",".join(ids),
                    "accion": "dry-run",
                    "resultado": f"se actualizarían {len(ids)} item(s)",
                    "mensaje": "",
                })
            time.sleep(SLEEP_ENTRE_CALLS)
            continue

        # ---- Ejecución real ----
        ids = _buscar_item_ids(codigo)
        time.sleep(SLEEP_ENTRE_CALLS)

        if len(ids) == 0:
            resumen["no_encontrados"] += 1
            reporte.append({
                "codigo": codigo, "stock": stock, "ml_item_id": "",
                "accion": "search", "resultado": "no-encontrado",
                "mensaje": "No hay publicación con ese SELLER_SKU",
            })
            continue

        if len(ids) > 1:
            resumen["codigos_con_multiples_items"] += 1
            log.warning(f"{codigo} matchea {len(ids)} items — actualizando todos")

        hubo_error = False
        mensajes: list[str] = []
        for mid in ids:
            ok, msg = _actualizar_item(mid, stock)
            mensajes.append(f"{mid}:{msg}")
            if not ok:
                hubo_error = True
            time.sleep(SLEEP_ENTRE_CALLS)

        resumen["items_tocados"] += len(ids)
        if hubo_error:
            resumen["errores"] += 1
            reporte.append({
                "codigo": codigo, "stock": stock,
                "ml_item_id": ",".join(ids),
                "accion": "PUT", "resultado": "ERROR parcial o total",
                "mensaje": " | ".join(mensajes),
            })
        else:
            resumen["reactivados"] += 1
            reporte.append({
                "codigo": codigo, "stock": stock,
                "ml_item_id": ",".join(ids),
                "accion": "PUT", "resultado": "reactivado OK",
                "mensaje": " | ".join(mensajes),
            })

    return reporte, resumen


# ==========================================================
# IO de reporte
# ==========================================================
def _escribir_reporte(reporte: list[dict], base_dir: Path) -> Path:
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    out = base_dir / f"reactivar_aros_REPORTE_{ts}.csv"
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["codigo", "stock", "ml_item_id", "accion", "resultado", "mensaje"],
        )
        w.writeheader()
        for r in reporte:
            w.writerow(r)
    return out


def _imprimir_resumen(resumen: dict, path_reporte: Path) -> None:
    print()
    print("=" * 70)
    print("  RESUMEN")
    print("=" * 70)
    print(f"  Códigos únicos en input         : {resumen['total']}")
    print(f"  Skip (stock < 1)                : {resumen['stock_0_skip']}")
    print(f"  Reactivados OK                  : {resumen['reactivados']}")
    print(f"  No encontrados en ML            : {resumen['no_encontrados']}")
    print(f"  Errores                         : {resumen['errores']}")
    print(f"  Items ML tocados (total)        : {resumen['items_tocados']}")
    if resumen['codigos_con_multiples_items']:
        print(f"  ⚠ Códigos con >1 publicación   : {resumen['codigos_con_multiples_items']}")
    print(f"  Reporte detallado               : {path_reporte}")
    print("=" * 70)


# ==========================================================
# Diagnóstico: para UN código, mostrar qué devuelve ML
# ==========================================================
def _diagnosticar_codigo(codigo: str) -> None:
    """
    Ayuda a entender por qué un código no matchea:
      1. Prueba seller_sku search → debería encontrar si el SELLER_SKU está cargado.
      2. Prueba full-text search (q=) → encuentra si el código aparece en título.
      3. Para cada match, trae el detalle del item y muestra su SELLER_SKU real.
    """
    print("=" * 70)
    print(f"  DIAGNÓSTICO para CODIGO = {codigo!r}")
    print("=" * 70)

    try:
        uid = _obtener_user_id()
    except Exception as e:
        print(f"✗ No pude obtener user_id: {e}")
        return

    print(f"\n[1] GET /users/{uid}/items/search?seller_sku=...")
    try:
        r = ml_client.get(f"/users/{uid}/items/search",
                          params={"seller_sku": codigo, "limit": 50})
        ids = list((r or {}).get("results") or [])
        print(f"    → {len(ids)} matches por SELLER_SKU exacto")
        for mid in ids[:5]:
            print(f"      - {mid}")
    except Exception as e:
        print(f"    ✗ error: {e}")

    print(f"\n[2] GET /users/{uid}/items/search?q=<codigo>  (full-text)")
    try:
        r = ml_client.get(f"/users/{uid}/items/search",
                          params={"q": codigo, "limit": 5})
        ids = list((r or {}).get("results") or [])
        print(f"    → {len(ids)} matches por texto libre")
        for mid in ids:
            print(f"      - {mid}")
            # Traer el detalle para ver qué SELLER_SKU tiene realmente
            try:
                it = ml_client.get(f"/items/{mid}",
                                   params={"attributes": "id,title,status,available_quantity,seller_custom_field,attributes"})
                titulo = (it or {}).get("title", "?")
                status = (it or {}).get("status", "?")
                qty = (it or {}).get("available_quantity", "?")
                seller_sku_real = (it or {}).get("seller_custom_field") or ""
                # BONUS: buscar SELLER_SKU en atributos también
                seller_sku_attr = ""
                for a in (it or {}).get("attributes") or []:
                    if a.get("id") in ("SELLER_SKU", "PART_NUMBER"):
                        seller_sku_attr = f"{a['id']}={a.get('value_name')!r}"
                        break
                print(f"        titulo           : {titulo[:60]}")
                print(f"        status           : {status}   qty: {qty}")
                print(f"        seller_custom_field: {seller_sku_real!r}")
                if seller_sku_attr:
                    print(f"        atributo         : {seller_sku_attr}")
            except Exception as e:
                print(f"        ✗ no pude leer detalle: {e}")
    except Exception as e:
        print(f"    ✗ error: {e}")

    print("\n[3] GET /users/me  (sanity check del token)")
    try:
        r = ml_client.get("/users/me")
        print(f"    user_id: {(r or {}).get('id')}   nickname: {(r or {}).get('nickname')}")
    except Exception as e:
        print(f"    ✗ error: {e}")
    print("=" * 70)


# ==========================================================
# CLI
# ==========================================================
def main() -> int:
    ap = argparse.ArgumentParser(
        description="Reactiva publicaciones ML y actualiza stock en batch "
                    "usando el SELLER_SKU (columna CODIGO) como llave."
    )
    ap.add_argument("input", nargs="?",
                    help="CSV (preferido) o .xls/.xlsx con columnas CODIGO, STOCK. "
                         "No requerido si usás --diagnostico.")
    ap.add_argument("--diagnostico", metavar="CODIGO",
                    help="Diagnostica por qué un CODIGO específico no matchea. "
                         "No requiere el CSV.")
    ap.add_argument("--dry-run", action="store_true",
                    help="No hace PUT. Llama al search para estimar, pero no toca items.")
    ap.add_argument("--si", action="store_true",
                    help="No pide confirmación (útil en scripts).")
    ap.add_argument("--excel", action="store_true",
                    help="Forzá lectura como Excel (pandas/openpyxl).")
    ap.add_argument("--limite", type=int, default=None,
                    help="Procesar solo los primeros N códigos con stock≥1 (prueba).")
    args = ap.parse_args()

    # Modo diagnóstico: para un solo código, mostrar qué devuelve ML
    if args.diagnostico:
        _diagnosticar_codigo(args.diagnostico.strip())
        return 0

    if not args.input:
        ap.error("falta el archivo de input (o usá --diagnostico CODIGO)")

    path = Path(args.input).expanduser().resolve()
    if not path.exists():
        print(f"✗ No existe: {path}", file=sys.stderr)
        return 2

    rows = _leer_input(path, excel_mode=args.excel)
    con_stock = sum(1 for r in rows if r["stock"] >= 1)
    sin_stock = sum(1 for r in rows if r["stock"] < 1)
    print("=" * 70)
    print(f"  INPUT: {path.name}")
    print(f"  Códigos únicos  : {len(rows)}")
    print(f"  Con stock ≥ 1   : {con_stock}  ← a reactivar")
    print(f"  Con stock = 0   : {sin_stock}  ← skip")
    if args.limite:
        print(f"  --limite        : solo los primeros {args.limite}")
    print("=" * 70)

    if not args.dry_run and not args.si:
        resp = input("  Continuar y tocar ML? (s/N): ").strip().lower()
        if resp != "s":
            print("Cancelado.")
            return 0

    modo = "DRY-RUN (sin tocar ML)" if args.dry_run else "EJECUCIÓN REAL"
    print(f"  Modo: {modo}")
    print()

    reporte, resumen = procesar(rows, dry_run=args.dry_run, limite=args.limite)
    ruta_rep = _escribir_reporte(reporte, path.parent)
    _imprimir_resumen(resumen, ruta_rep)
    return 0


if __name__ == "__main__":
    sys.exit(main())
