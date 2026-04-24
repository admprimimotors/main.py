"""
diagnostico_atributos.py
========================
Herramienta de diagnóstico para saber, antes de publicar, qué tan completa va
a quedar la ficha técnica estructurada de un producto en Mercado Libre.

Lo que muestra:
  - Título SEO que se va a usar.
  - Categoría ML predicha.
  - Lista de atributos REQUIRED / CATALOG_REQUIRED / recomendados de esa
    categoría, con:
        ✓  respondido (y con qué valor)
        ✗  faltante  → te dice exactamente qué cargar en la ficha técnica
  - Score de calidad final (0-100). El target es ≥70.

Uso:
    (venv) PS > python diagnostico_atributos.py 0013
    (venv) PS > python diagnostico_atributos.py 0013 --json     # output JSON
    (venv) PS > python diagnostico_atributos.py 0013 --full     # incluir TODOS los atributos de la categoría

Tip: cuando un atributo required sale ✗, abrí fichas_tecnicas de ese producto
y agregale una fila con la clave en lenguaje humano (ej. "Dientes", "Voltaje",
"Diámetro") — el mapeo alias → ML está en ml/atributos.py::ALIAS_A_ATTR_ML.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Optional

import db
from logger import get_logger
from ml import atributos as mlattrs
from ml import publicaciones as pub

log = get_logger(__name__)


def _hr(c: str = "=", n: int = 78) -> str:
    return c * n


def _cargar_producto(sku_o_id: str) -> Optional[dict]:
    """
    Resuelve un arg del usuario (SKU texto o id numérico) a un producto.

    IMPORTANTE — orden de resolución:
      1. Se prueba SIEMPRE como SKU primero (con padding a 4 dígitos si
         corresponde). Razón: los SKUs de Primi Motors son "0013", "0020",
         etc. — si interpretáramos numéricamente, "0013" se convertiría en
         id=13 y traería el producto equivocado.
      2. Recién si no matchea como SKU, caemos a id numérico. Y sólo si el
         string NO empieza con "0" (para no confundir un SKU como "0013"
         con id=13).
    """
    s = (sku_o_id or "").strip()
    if not s:
        return None
    with db.conexion() as c:
        # 1) Como SKU (master/ml/proveedor). Con padding de ceros por si
        #    PowerShell lo truncó (ej. "13" → probamos "0013" también).
        variantes = [s.upper()]
        if s.isdigit() and len(s) < 4:
            variantes.append(s.zfill(4).upper())
        for v in variantes:
            r = c.execute(
                """SELECT * FROM productos
                     WHERE UPPER(sku_master)=? OR UPPER(sku_ml)=? OR UPPER(sku_proveedor)=?
                     LIMIT 1""",
                (v, v, v),
            ).fetchone()
            if r:
                return dict(r)
        # 2) Recién ahora como id numérico, y sólo si NO empieza con "0"
        #    (los SKUs con ceros a la izquierda nunca deberían colisionar
        #    con IDs numéricos).
        if s.isdigit() and not s.startswith("0"):
            r = c.execute("SELECT * FROM productos WHERE id = ?", (int(s),)).fetchone()
            if r:
                return dict(r)
    return None


def _leer_fichas(producto_id: int) -> list[dict]:
    try:
        with db.conexion() as c:
            rows = c.execute(
                """SELECT clave, valor, unidad, orden FROM fichas_tecnicas
                    WHERE producto_id=? ORDER BY orden, clave""",
                (producto_id,),
            ).fetchall()
        return [dict(r) for r in rows]
    except Exception as e:
        log.warning(f"fichas_tecnicas ilegible: {e}")
        return []


def _leer_compats(producto_id: int) -> list[dict]:
    with db.conexion() as c:
        rows = c.execute(
            """SELECT marca, modelo, anio_desde, anio_hasta, motor, notas
                 FROM compatibilidades_vehiculares
                WHERE producto_id=? ORDER BY marca, modelo, anio_desde""",
            (producto_id,),
        ).fetchall()
    return [dict(r) for r in rows]


def _tag_label(tags: dict) -> str:
    if tags.get("required"):
        return "REQ"
    if tags.get("catalog_required"):
        return "CAT"
    if tags.get("conditional_required"):
        return "CND"
    if tags.get("hidden"):
        return "HID"
    return "rec"


def _diagnosticar(producto: dict, *, show_all: bool = False) -> dict:
    """
    Corre la predicción + fetch de atributos + mapper y devuelve un reporte.
    No escribe nada.
    """
    fichas = _leer_fichas(producto["id"])
    compats = _leer_compats(producto["id"])

    titulo = pub.construir_titulo(producto, compats)
    category_id = pub.predecir_categoria_ml(titulo)
    cat_atts: list[dict] = []
    atributos: list[dict] = []
    score: Optional[dict] = None

    if category_id:
        cat_atts = mlattrs.obtener_atributos_categoria(category_id)
        # IMPORTANTE: pasamos los MISMOS overrides que usa publicaciones.py
        # en el flujo real (vehicle_type + part_number "ZE0097" + BRAND=ZEN).
        # Si no, el diagnóstico muestra cosas distintas a lo que va al
        # POST /items (ej. BRAND=Delco Remy en vez de ZEN).
        atributos = mlattrs.construir_atributos(
            producto=producto,
            fichas=fichas,
            compatibilidades=compats,
            cat_atts=cat_atts,
            vehicle_type=pub.deducir_vehicle_type(producto, compats),
            part_number_override=pub.construir_part_number(producto),
            brand_override=pub.BUSINESS_BRAND,
        )
        score = mlattrs.evaluar_completitud(cat_atts, atributos)

    # Armar filas comparativas: cada atributo de la categoría + si lo completamos
    completados = {a["id"]: a for a in atributos}
    filas = []
    for a in cat_atts:
        aid = a.get("id")
        nombre = a.get("name") or aid
        tags = a.get("tags") or {}
        vt = a.get("value_type") or "?"
        tag_lab = _tag_label(tags)
        if aid in completados:
            c = completados[aid]
            vname = c.get("value_name") or c.get("value_id") or "?"
            filas.append({
                "ok": True, "id": aid, "nombre": nombre, "tag": tag_lab,
                "tipo": vt, "valor": vname,
            })
        else:
            filas.append({
                "ok": False, "id": aid, "nombre": nombre, "tag": tag_lab,
                "tipo": vt, "valor": None,
            })

    return {
        "producto": {
            "id": producto["id"],
            "sku_master": producto.get("sku_master"),
            "descripcion": producto.get("descripcion"),
            "marca_proveedor": producto.get("marca_proveedor"),
        },
        "titulo": titulo,
        "category_id": category_id,
        "fichas_cargadas": len(fichas),
        "compatibilidades_cargadas": len(compats),
        "atributos_respondidos": len(atributos),
        "atributos_total_categoria": len(cat_atts),
        "score": score,
        "filas": filas if show_all else [f for f in filas if f["tag"] in ("REQ", "CAT", "CND") or f["ok"]],
    }


def _imprimir(reporte: dict) -> None:
    p = reporte["producto"]
    print(_hr())
    print(f"  DIAGNÓSTICO DE ATRIBUTOS — producto #{p['id']} SKU {p['sku_master']}")
    print(_hr())
    desc = (p.get("descripcion") or "").strip()
    if desc:
        print(f"  Descripción   : {desc}")
    if p.get("marca_proveedor"):
        print(f"  Marca prov.   : {p['marca_proveedor']}")
    print(f"  Título SEO    : {reporte['titulo']}")
    print(f"  Categoría ML  : {reporte['category_id'] or '—  (no se pudo predecir)'}")
    print(f"  Fichas técn.  : {reporte['fichas_cargadas']}   Compat.: {reporte['compatibilidades_cargadas']}")
    print(_hr("-"))

    s = reporte.get("score")
    if s:
        print(f"  SCORE GLOBAL  : {s['score']}%   "
              f"(req {s['required_completados']}/{s['required_total']}, "
              f"recom {s['recomm_completados']}/{s['recomm_total']})")
        if s["score"] >= 70:
            print(f"  🎯 Llega al umbral profesional (≥70%).")
        else:
            print(f"  ⚠  Debajo del 70% — cargá datos en la ficha técnica para subirlo.")
    print(_hr("-"))

    if reporte["filas"]:
        print(f"  {'ST':<2} {'TAG':<4} {'ATTR_ID':<28} {'TIPO':<14} VALOR")
        print(_hr("-"))
        for f in reporte["filas"]:
            st = "✓" if f["ok"] else "✗"
            val = f["valor"] if f["ok"] else "— (FALTA)"
            print(f"  {st:<2} {f['tag']:<4} {f['id']:<28} {f['tipo']:<14} {val}")

    if s and s.get("faltantes_required"):
        print()
        print("  REQUERIDOS faltantes — cargalos en fichas_tecnicas del producto:")
        for x in s["faltantes_required"]:
            print(f"    - {x}")
    if s and s.get("faltantes_recomm") and len(s["faltantes_recomm"]) <= 20:
        print()
        print("  Recomendados (suman al score) faltantes:")
        for x in s["faltantes_recomm"]:
            print(f"    - {x}")
    print(_hr())


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Diagnostica el score de atributos ML de un producto antes de publicar."
    )
    ap.add_argument("sku", help="SKU master / SKU ML / SKU proveedor / id numérico.")
    ap.add_argument("--json", action="store_true", help="Imprime el reporte en JSON.")
    ap.add_argument("--full", action="store_true",
                    help="Incluir TODOS los atributos de la categoría (no solo required + respondidos).")
    args = ap.parse_args()

    try:
        db.inicializar_db()
    except Exception as e:
        print(f"  ⚠ No pude inicializar DB: {e}", file=sys.stderr)

    prod = _cargar_producto(args.sku)
    if not prod:
        print(f"✗ No encontré un producto con '{args.sku}'.", file=sys.stderr)
        return 2

    rep = _diagnosticar(prod, show_all=args.full)
    if args.json:
        print(json.dumps(rep, ensure_ascii=False, indent=2))
    else:
        _imprimir(rep)
    return 0


if __name__ == "__main__":
    sys.exit(main())
