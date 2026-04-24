"""
inventory/ml_linker.py
======================
Sincroniza las publicaciones activas de Mercado Libre con la tabla productos
de la base local. El match se hace por SKU.

Dos caminos de match (en orden):
  1. Match directo: sku_ml de ML == sku_proveedor o sku_ml del producto local.
  2. Match normalizado: mismo núcleo normalizado (permite pescar "0013ZEN"↔"0013").

Si un producto de ML no se encuentra en el inventario local, se igualmente se
guarda en publicaciones_ml (con producto_id = NULL) para no perder la información.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import db
from inventory import normalizer as norm
from logger import get_logger
from ml import client
from ml.auth import cargar_tokens

log = get_logger(__name__)


# ==========================================================
# Obtención de publicaciones desde ML (reutiliza lógica de extraer_publicaciones)
# ==========================================================
def _obtener_ids(user_id: int, solo_activas: bool = True) -> list[str]:
    ids: list[str] = []
    scroll_id: str | None = None
    endpoint = f"/users/{user_id}/items/search"

    while True:
        params: dict = {"search_type": "scan", "limit": 100}
        if solo_activas:
            params["status"] = "active"
        if scroll_id:
            params["scroll_id"] = scroll_id

        resp = client.get(endpoint, params=params)
        batch = resp.get("results", []) or []
        if not batch:
            break
        ids.extend(batch)
        scroll_id = resp.get("scroll_id")
        if not scroll_id:
            break
    return ids


def _obtener_detalles(item_ids: list[str]) -> list[dict]:
    detalles: list[dict] = []
    lote = 20
    for i in range(0, len(item_ids), lote):
        batch = item_ids[i:i + lote]
        ids_str = ",".join(batch)
        resp = client.get("/items", params={"ids": ids_str})
        for wrapper in resp:
            if wrapper.get("code") == 200 and wrapper.get("body"):
                detalles.append(wrapper["body"])
            else:
                log.warning(f"No se pudo obtener item: {wrapper}")
    return detalles


def _extraer_sku_ml(item: dict) -> str:
    sku = item.get("seller_custom_field") or ""
    if sku:
        return str(sku).strip()
    for attr in item.get("attributes", []) or []:
        if attr.get("id") == "SELLER_SKU":
            return str(attr.get("value_name") or "").strip()
    return ""


# ==========================================================
# Resultado del linkeo
# ==========================================================
@dataclass
class ResultadoLinkeo:
    total_publicaciones_ml: int = 0
    publicaciones_linkeadas: int = 0       # tenían match con un producto local
    publicaciones_sin_match: int = 0        # no encontramos producto local
    publicaciones_sin_sku: int = 0          # ML sin SKU → no se puede matchear
    productos_sin_publicacion: list[str] = field(default_factory=list)  # productos que no están en ML

    def resumen_texto(self) -> str:
        lineas = [
            "Resumen de sincronización con ML:",
            f"  Publicaciones activas en ML:      {self.total_publicaciones_ml}",
            f"  → Linkeadas a un producto local:  {self.publicaciones_linkeadas}",
            f"  → Sin match (producto no en base):{self.publicaciones_sin_match}",
            f"  → Sin SKU en ML:                  {self.publicaciones_sin_sku}",
            f"  Productos locales sin publicación:{len(self.productos_sin_publicacion)}",
        ]
        return "\n".join(lineas)


# ==========================================================
# Linker principal
# ==========================================================
def sincronizar_publicaciones_ml(solo_activas: bool = True) -> ResultadoLinkeo:
    """
    Trae todas las publicaciones de ML y las linkea a productos locales.

    Returns:
        ResultadoLinkeo con el detalle del match.
    """
    resultado = ResultadoLinkeo()

    tokens = cargar_tokens()
    if tokens is None:
        raise RuntimeError("No hay tokens guardados. Corré primero get_initial_token.py")

    log.info("Obteniendo IDs de publicaciones ML...")
    ids = _obtener_ids(tokens.user_id, solo_activas=solo_activas)
    log.info(f"  {len(ids)} publicaciones encontradas")

    if not ids:
        return resultado

    log.info("Descargando detalles...")
    items = _obtener_detalles(ids)
    resultado.total_publicaciones_ml = len(items)

    # Armar índice de productos locales por sku_master y por sku_proveedor
    with db.conexion() as c:
        filas = c.execute(
            "SELECT id, sku_master, sku_proveedor, sku_ml FROM productos WHERE activo = 1"
        ).fetchall()

    idx_por_master: dict[str, int] = {}
    idx_por_proveedor: dict[str, int] = {}
    idx_por_ml: dict[str, int] = {}
    for f in filas:
        if f["sku_master"]:
            idx_por_master[f["sku_master"].upper()] = f["id"]
        if f["sku_proveedor"]:
            idx_por_proveedor[f["sku_proveedor"].upper().strip()] = f["id"]
        if f["sku_ml"]:
            idx_por_ml[f["sku_ml"].upper().strip()] = f["id"]

    productos_con_match: set[int] = set()

    with db.conexion() as conn:
        for item in items:
            ml_item_id = item.get("id")
            sku_ml = _extraer_sku_ml(item)

            producto_id: int | None = None

            if sku_ml:
                # 1. Match directo por sku_ml
                producto_id = idx_por_ml.get(sku_ml.upper())
                # 2. Match por sku_proveedor
                if producto_id is None:
                    producto_id = idx_por_proveedor.get(sku_ml.upper())
                # 3. Match por núcleo normalizado
                if producto_id is None:
                    nucleo = norm.normalizar_sku(sku_ml)
                    if nucleo:
                        producto_id = idx_por_master.get(nucleo.upper())
            else:
                resultado.publicaciones_sin_sku += 1

            if producto_id:
                resultado.publicaciones_linkeadas += 1
                productos_con_match.add(producto_id)
            else:
                resultado.publicaciones_sin_match += 1

            shipping = item.get("shipping") or {}

            conn.execute(
                """
                INSERT INTO publicaciones_ml (
                    producto_id, ml_item_id, titulo_ml, sku_ml, status_ml, condition_ml,
                    precio_ml, currency_ml, available_quantity, sold_quantity,
                    category_id_ml, listing_type_id, shipping_mode,
                    permalink, thumbnail, date_created_ml, last_updated_ml,
                    ultima_sincronizacion
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, datetime('now','localtime'))
                ON CONFLICT(ml_item_id) DO UPDATE SET
                    producto_id           = COALESCE(excluded.producto_id, publicaciones_ml.producto_id),
                    titulo_ml             = excluded.titulo_ml,
                    sku_ml                = excluded.sku_ml,
                    status_ml             = excluded.status_ml,
                    condition_ml          = excluded.condition_ml,
                    precio_ml             = excluded.precio_ml,
                    currency_ml           = excluded.currency_ml,
                    available_quantity    = excluded.available_quantity,
                    sold_quantity         = excluded.sold_quantity,
                    category_id_ml        = excluded.category_id_ml,
                    listing_type_id       = excluded.listing_type_id,
                    shipping_mode         = excluded.shipping_mode,
                    permalink             = excluded.permalink,
                    thumbnail             = excluded.thumbnail,
                    last_updated_ml       = excluded.last_updated_ml,
                    ultima_sincronizacion = datetime('now','localtime')
                """,
                (
                    producto_id, ml_item_id, item.get("title"), sku_ml or None,
                    item.get("status"), item.get("condition"),
                    item.get("price"), item.get("currency_id"),
                    item.get("available_quantity"), item.get("sold_quantity"),
                    item.get("category_id"), item.get("listing_type_id"),
                    shipping.get("mode"),
                    item.get("permalink"), item.get("thumbnail"),
                    item.get("date_created"), item.get("last_updated"),
                ),
            )

    # Productos locales que no tienen publicación
    with db.conexion() as c:
        sin_pub = c.execute(
            """
            SELECT p.sku_master, p.descripcion
            FROM productos p
            LEFT JOIN publicaciones_ml m ON m.producto_id = p.id
            WHERE p.activo = 1 AND m.id IS NULL
            """
        ).fetchall()
        resultado.productos_sin_publicacion = [f["sku_master"] for f in sin_pub]

    return resultado
