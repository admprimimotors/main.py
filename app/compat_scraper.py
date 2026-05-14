"""
app/compat_scraper.py
=====================
Scraper de compatibilidades vehiculares: encuentra los vehículos compatibles
para cada producto buscando publicaciones similares de COMPETIDORES en ML y
aglomerando sus compats por frecuencia.

Estrategia:
  1. Para un producto P con SKU+título+part_number, buscar en ML items
     similares (mismo title o part_number).
  2. Para los top N matches, traer sus compatibilidades vía
     GET /items/{id}/compatibilities.
  3. Aglomerar por vehicle_id (el ID del vehículo en el catálogo de ML).
  4. Quedarnos con los que tienen ≥ min_votes votos, ordenados por frecuencia.
  5. POST /items/{P.ml_item_id}/compatibilities con esa lista.
  6. Guardar local en producto_compatibilidades + vehiculos (creando si no existe).

Procesamiento en batch para no exceder timeouts de Render. Cada llamada a
ML tiene un sleep configurable.
"""

from __future__ import annotations

import os
import time
from typing import Optional

from sqlalchemy import select, func as sql_func
from sqlalchemy.orm import Session, selectinload

from . import ml_client
from .models import Producto, ProductoCompatibilidad, Vehiculo


# Sleep entre llamadas a ML (segundos). ML rate-limita a ~10 req/s, somos
# conservadores con 0.25s para tener margen.
SLEEP_BETWEEN_CALLS = float(os.environ.get("COMPAT_SCRAPER_SLEEP", "0.25"))

# Búsqueda en ML: cuántos top items competidores miramos por SKU
SEARCH_TOP_N = int(os.environ.get("COMPAT_SCRAPER_SEARCH_TOP", "10"))

# Filtros aglomeración
DEFAULT_MIN_VOTES = int(os.environ.get("COMPAT_SCRAPER_MIN_VOTES", "2"))
DEFAULT_MAX_COMPATS = int(os.environ.get("COMPAT_SCRAPER_MAX_COMPATS", "10"))


def find_similar_ml_items(
    db: Session,
    producto: Producto,
    *,
    limit: int = SEARCH_TOP_N,
) -> list[str]:
    """
    Busca en ML items similares al producto, devuelve sus item_ids.
    Estrategia: buscamos por part_number (si está en la ficha o en el título),
    y por título tal cual.
    """
    queries: list[str] = []
    # Si la ficha tiene un campo "numero_de_pieza" lo usamos primero — es el
    # match más preciso.
    ficha = producto.ficha_tecnica or {}
    for key in ("numero_de_pieza", "part_number", "oem", "codigo_oem", "sku_fabricante"):
        v = ficha.get(key)
        if v and str(v).strip():
            queries.append(str(v).strip())
    # También intentamos con el título
    if producto.titulo and producto.titulo.strip():
        queries.append(producto.titulo.strip())

    seen_ids: set[str] = set()
    matches: list[str] = []
    for q in queries:
        try:
            items = ml_client.search_items(db, q, limit=limit)
        except Exception:
            items = []
        for item in items:
            iid = item.get("id")
            if iid and iid not in seen_ids:
                # Excluir el item del propio producto si aparece en la búsqueda
                if iid == producto.ml_item_id:
                    continue
                seen_ids.add(iid)
                matches.append(iid)
                if len(matches) >= limit:
                    return matches
        time.sleep(SLEEP_BETWEEN_CALLS)
    return matches


def fetch_compatibilities_aggregated(
    db: Session,
    item_ids: list[str],
) -> list[dict]:
    """
    Para cada item_id de ML, trae sus compatibilidades y aglomera por
    vehicle_id (el ID del vehículo dentro del catálogo de ML). Devuelve
    una lista ordenada por votos descendentes:

      [{"vehicle_id": "MLA-XYZ", "votes": 8, "raw": <last raw compat dict>}, ...]

    Esa "raw compat dict" tiene los attributes (BRAND, MODEL, YEAR, etc.)
    que usamos para crear o matchear el Vehiculo local.
    """
    if not item_ids:
        return []
    votes: dict[str, dict] = {}
    for item_id in item_ids:
        try:
            compats = ml_client.get_item_compatibilities(db, item_id)
        except Exception:
            compats = []
        time.sleep(SLEEP_BETWEEN_CALLS)
        for c in compats:
            vid = c.get("id")
            if not vid:
                continue
            if vid not in votes:
                votes[vid] = {"vehicle_id": vid, "votes": 0, "raw": c}
            votes[vid]["votes"] += 1
            # Guardamos el último raw — los attributes deberían ser idénticos
            # entre publicaciones para el mismo vehicle_id.
            votes[vid]["raw"] = c
    return sorted(votes.values(), key=lambda x: -x["votes"])


def _extract_vehiculo_data(raw: dict) -> dict:
    """
    Extrae los campos de Vehiculo desde el dict raw que devuelve la API de ML.
    `raw.attributes` es una lista de {id, value_name, value_id}.
    Mapeamos los IDs ML a las columnas locales del modelo Vehiculo.
    """
    attrs = {a.get("id"): a.get("value_name") for a in (raw.get("attributes") or [])}
    return {
        "marca": (attrs.get("VEHICLE_BRAND") or attrs.get("BRAND") or "").strip() or None,
        "modelo": (attrs.get("VEHICLE_MODEL") or attrs.get("MODEL") or "").strip() or None,
        "anio": attrs.get("VEHICLE_YEAR") or attrs.get("YEAR"),
        "combustible": (attrs.get("VEHICLE_FUEL_TYPE") or "").strip().lower() or None,
        "cilindrada_cc": _try_int(attrs.get("VEHICLE_ENGINE_DISPLACEMENT")),
    }


def _try_int(v) -> Optional[int]:
    if v is None:
        return None
    try:
        # ML a veces devuelve "1.6 L" o "1600 cc"
        s = str(v).strip().lower().replace("cc", "").replace("l", "").strip()
        return int(float(s))
    except (ValueError, TypeError):
        return None


def _get_or_create_vehiculo(
    db: Session,
    *,
    marca: Optional[str],
    modelo: Optional[str],
    anio: Optional[int],
    combustible: Optional[str],
    cilindrada_cc: Optional[int],
    cache: dict,
) -> Optional[Vehiculo]:
    """Busca o crea un Vehiculo. None si faltan datos esenciales (marca o modelo)."""
    if not marca or not modelo:
        return None
    key = (marca.lower(), modelo.lower(), anio, combustible, cilindrada_cc)
    if key in cache:
        return cache[key]

    filters = [
        sql_func.lower(Vehiculo.marca) == marca.lower(),
        sql_func.lower(Vehiculo.modelo) == modelo.lower(),
    ]
    if anio is not None:
        filters.append(Vehiculo.anio_desde == anio)
        filters.append(Vehiculo.anio_hasta == anio)
    if combustible:
        filters.append(Vehiculo.combustible == combustible)
    if cilindrada_cc is not None:
        filters.append(Vehiculo.cilindrada_cc == cilindrada_cc)
    v = db.execute(select(Vehiculo).where(*filters)).scalar_one_or_none()
    if v is not None:
        cache[key] = v
        return v
    v = Vehiculo(
        marca=marca,
        modelo=modelo,
        anio_desde=anio,
        anio_hasta=anio,
        combustible=combustible,
        cilindrada_cc=cilindrada_cc,
    )
    db.add(v)
    db.flush()
    cache[key] = v
    return v


def apply_compats_to_producto(
    db: Session,
    producto: Producto,
    aggregated: list[dict],
    *,
    min_votes: int = DEFAULT_MIN_VOTES,
    max_compats: int = DEFAULT_MAX_COMPATS,
    vehiculo_cache: Optional[dict] = None,
) -> tuple[int, list[str]]:
    """
    Aplica las compats aglomeradas al producto: crea Vehiculos locales si
    no existen + crea ProductoCompatibilidad rows (skipea duplicados).

    Devuelve (n_added_local, vehicle_ids_para_pushear_a_ml).

    El segundo elemento es la lista de IDs de ML listos para POST a
    /items/{ml_item_id}/compatibilities.
    """
    if vehiculo_cache is None:
        vehiculo_cache = {}
    selected = [c for c in aggregated if c["votes"] >= min_votes][:max_compats]
    n_added = 0
    ml_vehicle_ids: list[str] = []

    # SKUs locales que ya tiene este producto (por vehicle_id ML)
    existing_compats = db.execute(
        select(ProductoCompatibilidad).where(
            ProductoCompatibilidad.producto_id == producto.id
        )
    ).scalars().all()
    existing_ml_ids = {c.ml_compat_id for c in existing_compats if c.ml_compat_id}
    existing_vehicle_ids = {c.vehiculo_id for c in existing_compats}

    for c in selected:
        ml_vehicle_ids.append(c["vehicle_id"])
        if c["vehicle_id"] in existing_ml_ids:
            continue
        vdata = _extract_vehiculo_data(c["raw"])
        vehic = _get_or_create_vehiculo(
            db,
            marca=vdata["marca"],
            modelo=vdata["modelo"],
            anio=vdata["anio"],
            combustible=vdata["combustible"],
            cilindrada_cc=vdata["cilindrada_cc"],
            cache=vehiculo_cache,
        )
        if vehic is None:
            continue
        if vehic.id in existing_vehicle_ids:
            continue
        db.add(ProductoCompatibilidad(
            producto_id=producto.id,
            vehiculo_id=vehic.id,
            ml_compat_id=c["vehicle_id"],
            notas=f"Auto-scraped ({c['votes']} votos)",
        ))
        existing_vehicle_ids.add(vehic.id)
        n_added += 1

    return n_added, ml_vehicle_ids


def process_one_producto(
    db: Session,
    producto: Producto,
    *,
    push_to_ml: bool = True,
    min_votes: int = DEFAULT_MIN_VOTES,
    max_compats: int = DEFAULT_MAX_COMPATS,
    vehiculo_cache: Optional[dict] = None,
) -> dict:
    """
    Procesa un producto: scraper → aplicar local → (opcional) push a ML.
    Devuelve dict con resultado:
      {sku, n_local_added, n_pushed_ml, ok, error?}
    """
    result = {
        "sku": producto.sku,
        "ml_item_id": producto.ml_item_id,
        "n_local_added": 0,
        "n_pushed_ml": 0,
        "ok": False,
        "error": None,
    }
    if not producto.ml_item_id:
        result["error"] = "Sin ml_item_id"
        return result

    try:
        similar = find_similar_ml_items(db, producto)
        if not similar:
            result["error"] = "No encontré items similares en ML"
            return result

        aggregated = fetch_compatibilities_aggregated(db, similar)
        if not aggregated:
            result["error"] = "Items similares sin compats cargadas"
            return result

        n_added, ml_ids = apply_compats_to_producto(
            db, producto, aggregated,
            min_votes=min_votes, max_compats=max_compats,
            vehiculo_cache=vehiculo_cache,
        )
        result["n_local_added"] = n_added

        if not ml_ids:
            result["error"] = "Ninguna compat alcanzó el min de votos"
            return result

        # Push a ML
        if push_to_ml and ml_client.is_write_enabled():
            try:
                ml_client.add_item_compatibilities(db, producto.ml_item_id, ml_ids)
                result["n_pushed_ml"] = len(ml_ids)
            except Exception as e:
                result["error"] = f"Push ML falló: {type(e).__name__}: {str(e)[:120]}"
                # No revertimos lo local — el usuario puede pushear manual después.
                return result

        result["ok"] = True
    except Exception as e:
        result["error"] = f"{type(e).__name__}: {str(e)[:200]}"
        try:
            db.rollback()
        except Exception:
            pass
    return result


def list_eligible_skus(db: Session, limit: int = 50) -> list[Producto]:
    """
    Productos activos vinculados a ML que NO tienen compatibilidades locales.
    Ordenados por created_at desc (los más nuevos primero).
    """
    # Subquery: productos con al menos 1 compat
    con_compat = (
        select(ProductoCompatibilidad.producto_id)
        .distinct()
        .subquery()
    )
    q = (
        select(Producto)
        .options(selectinload(Producto.fotos))
        .where(Producto.activo.is_(True))
        .where(Producto.ml_item_id.is_not(None))
        .where(~Producto.id.in_(select(con_compat.c.producto_id)))
        .order_by(Producto.created_at.desc())
        .limit(limit)
    )
    return list(db.execute(q).scalars().all())


def count_eligible(db: Session) -> int:
    """Cuántos productos activos+linked a ML están sin compats locales."""
    con_compat = (
        select(ProductoCompatibilidad.producto_id)
        .distinct()
        .subquery()
    )
    q = (
        select(sql_func.count(Producto.id))
        .where(Producto.activo.is_(True))
        .where(Producto.ml_item_id.is_not(None))
        .where(~Producto.id.in_(select(con_compat.c.producto_id)))
    )
    return db.execute(q).scalar() or 0


def process_batch(
    db: Session,
    batch_size: int = 25,
    *,
    min_votes: int = DEFAULT_MIN_VOTES,
    max_compats: int = DEFAULT_MAX_COMPATS,
) -> dict:
    """
    Procesa un batch de productos elegibles. Cada producto consume varias
    llamadas a ML (search + N fetches). batch_size=25 ≈ 60-90s, seguro
    para Render's 100s timeout.

    Devuelve summary:
      {processed, ok, fail, results: [...]}
    """
    productos = list_eligible_skus(db, limit=batch_size)
    if not productos:
        return {"processed": 0, "ok": 0, "fail": 0, "results": []}

    vehiculo_cache: dict = {}
    results = []
    n_ok = 0
    for prod in productos:
        r = process_one_producto(
            db, prod,
            push_to_ml=True,
            min_votes=min_votes,
            max_compats=max_compats,
            vehiculo_cache=vehiculo_cache,
        )
        if r["ok"]:
            n_ok += 1
        results.append(r)
        # Commit incremental: si crashea en uno, no perdemos los anteriores
        try:
            db.commit()
        except Exception:
            db.rollback()

    return {
        "processed": len(productos),
        "ok": n_ok,
        "fail": len(productos) - n_ok,
        "results": results,
    }
