"""
db.py
=====
Capa de acceso a la base de datos SQLite de Primi Motors.

Responsabilidades:
 - Crear el schema si no existe.
 - Proveer una conexión con row_factory = sqlite3.Row (acceso por nombre).
 - Manejar migraciones simples versionadas.

Todas las tablas usan claves foráneas con ON DELETE para mantener integridad.

Tablas:
  - categorias          : árbol chato de categorías internas con coeficiente
  - marcas_auto         : lista de marcas de vehículos (ford, chevrolet, etc.)
  - productos           : catálogo maestro (un registro por SKU real)
  - movimientos_stock   : log de cada movimiento de stock (ingreso/egreso/ajuste)
  - historico_precios   : log de cambios de costo y precio de venta
  - publicaciones_ml    : vínculo producto ↔ publicación en Mercado Libre
  - configuracion       : clave/valor para parámetros del sistema
  - importaciones       : log de cada importación de Excel/PDF
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import config
from logger import get_logger

log = get_logger(__name__)


# ==========================================================
# Schema (DDL)
# ==========================================================
SCHEMA_SQL = """
-- Control de versión de schema para futuras migraciones
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY
);

-- Categorías internas (impulsores, poleas, embragues, frenos, etc.)
CREATE TABLE IF NOT EXISTS categorias (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    nombre              TEXT NOT NULL UNIQUE,
    descripcion         TEXT,
    coeficiente_default REAL NOT NULL DEFAULT 1.50,
    activa              INTEGER NOT NULL DEFAULT 1,
    fecha_alta          TEXT NOT NULL DEFAULT (datetime('now','localtime'))
);

-- Marcas de autos (para publicar correctamente en ML con ficha técnica)
CREATE TABLE IF NOT EXISTS marcas_auto (
    id     INTEGER PRIMARY KEY AUTOINCREMENT,
    nombre TEXT NOT NULL UNIQUE
);

-- Productos: catálogo maestro
CREATE TABLE IF NOT EXISTS productos (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    sku_master             TEXT NOT NULL UNIQUE,           -- SKU normalizado (ej: "0013")
    sku_proveedor          TEXT,                           -- SKU original del proveedor (ej: "0013ZEN" o "ZE0013")
    sku_ml                 TEXT,                           -- SKU que usa ML (suele ser el sku_proveedor)
    descripcion            TEXT NOT NULL,
    descripcion_larga      TEXT,                           -- ficha técnica extendida (viene del PDF)
    marca_proveedor        TEXT,                           -- ej: "ZEN", "BOSCH", "DELCO REMY"
    proveedor              TEXT,                           -- ej: "ZEN" (a futuro puede linkear a tabla proveedores)
    marca_auto             TEXT,                           -- ej: "CHEVROLET", "FORD" (detectado automáticamente)
    modelo_auto            TEXT,                           -- ej: "APACHE", "C20" (opcional)
    rubro_origen           TEXT,                           -- rubro tal cual vino del proveedor
    categoria_id           INTEGER REFERENCES categorias(id) ON DELETE SET NULL,
    costo                  REAL NOT NULL DEFAULT 0,        -- costo en ARS (PRCOSTO del proveedor)
    coeficiente_override   REAL,                           -- si está seteado, pisa el coeficiente de categoría
    precio_venta           REAL NOT NULL DEFAULT 0,        -- calculado = costo * coef
    stock_actual           INTEGER NOT NULL DEFAULT 0,
    stock_minimo           INTEGER NOT NULL DEFAULT 0,
    por_pedido             INTEGER NOT NULL DEFAULT 0,     -- bool: (X PED) → delay extendido en ML
    es_reacondicionado     INTEGER NOT NULL DEFAULT 0,     -- bool: productos 'R' (no se usa en Fase 1 pero ya queda listo)
    activo                 INTEGER NOT NULL DEFAULT 1,
    notas                  TEXT,
    fecha_alta             TEXT NOT NULL DEFAULT (datetime('now','localtime')),
    fecha_modificacion     TEXT NOT NULL DEFAULT (datetime('now','localtime'))
);

CREATE INDEX IF NOT EXISTS idx_productos_sku_master   ON productos(sku_master);
CREATE INDEX IF NOT EXISTS idx_productos_sku_proveedor ON productos(sku_proveedor);
CREATE INDEX IF NOT EXISTS idx_productos_sku_ml       ON productos(sku_ml);
CREATE INDEX IF NOT EXISTS idx_productos_marca_auto   ON productos(marca_auto);
CREATE INDEX IF NOT EXISTS idx_productos_categoria    ON productos(categoria_id);
CREATE INDEX IF NOT EXISTS idx_productos_activo       ON productos(activo);

-- Movimientos de stock (trazabilidad total)
CREATE TABLE IF NOT EXISTS movimientos_stock (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    producto_id  INTEGER NOT NULL REFERENCES productos(id) ON DELETE CASCADE,
    tipo         TEXT NOT NULL CHECK (tipo IN ('ingreso','egreso','ajuste','venta_ml','importacion')),
    cantidad     INTEGER NOT NULL,              -- puede ser negativo (egreso/ajuste)
    stock_previo INTEGER NOT NULL,
    stock_nuevo  INTEGER NOT NULL,
    origen       TEXT,                          -- "importación ZEN", "venta ML #XXX", "manual", etc.
    notas        TEXT,
    fecha        TEXT NOT NULL DEFAULT (datetime('now','localtime'))
);

CREATE INDEX IF NOT EXISTS idx_mov_producto ON movimientos_stock(producto_id);
CREATE INDEX IF NOT EXISTS idx_mov_fecha    ON movimientos_stock(fecha);

-- Histórico de precios (cambios de costo y de precio de venta)
CREATE TABLE IF NOT EXISTS historico_precios (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    producto_id      INTEGER NOT NULL REFERENCES productos(id) ON DELETE CASCADE,
    costo_anterior   REAL,
    costo_nuevo      REAL,
    precio_anterior  REAL,
    precio_nuevo     REAL,
    coef_usado       REAL,
    motivo           TEXT,
    fecha            TEXT NOT NULL DEFAULT (datetime('now','localtime'))
);

CREATE INDEX IF NOT EXISTS idx_hist_producto ON historico_precios(producto_id);

-- Vínculo producto ↔ publicación ML
CREATE TABLE IF NOT EXISTS publicaciones_ml (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    producto_id          INTEGER REFERENCES productos(id) ON DELETE SET NULL,
    ml_item_id           TEXT NOT NULL UNIQUE,           -- MLA123456789
    titulo_ml            TEXT,
    sku_ml               TEXT,
    status_ml            TEXT,                           -- active, paused, closed, etc.
    condition_ml         TEXT,                           -- new, used
    precio_ml            REAL,
    currency_ml          TEXT,
    available_quantity   INTEGER,
    sold_quantity        INTEGER,
    category_id_ml       TEXT,
    listing_type_id      TEXT,
    shipping_mode        TEXT,
    permalink            TEXT,
    thumbnail            TEXT,
    date_created_ml      TEXT,
    last_updated_ml      TEXT,
    ultima_sincronizacion TEXT NOT NULL DEFAULT (datetime('now','localtime'))
);

CREATE INDEX IF NOT EXISTS idx_pub_producto ON publicaciones_ml(producto_id);
CREATE INDEX IF NOT EXISTS idx_pub_sku_ml   ON publicaciones_ml(sku_ml);

-- Configuración clave/valor
CREATE TABLE IF NOT EXISTS configuracion (
    clave TEXT PRIMARY KEY,
    valor TEXT,
    notas TEXT,
    fecha_modificacion TEXT NOT NULL DEFAULT (datetime('now','localtime'))
);

-- Log de importaciones
CREATE TABLE IF NOT EXISTS importaciones (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    tipo          TEXT NOT NULL,             -- "zen_precios", "zen_stock", "pdf_ficha", etc.
    archivo       TEXT NOT NULL,
    filas_leidas  INTEGER,
    filas_nuevas  INTEGER,
    filas_update  INTEGER,
    filas_error   INTEGER,
    resumen       TEXT,
    fecha         TEXT NOT NULL DEFAULT (datetime('now','localtime'))
);

-- ==========================================================
-- FASE 2: Clientes + Remitos + Notas de Crédito
-- ==========================================================

-- Clientes (datos fiscales + comerciales + historial por relaciones)
CREATE TABLE IF NOT EXISTS clientes (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    razon_social         TEXT NOT NULL,
    nombre_comercial     TEXT,
    cuit_dni             TEXT,                         -- puede ser CUIT o DNI (sin guiones)
    condicion_iva        TEXT,                         -- Responsable Inscripto, Monotributo, Consumidor Final, Exento
    direccion            TEXT,
    localidad            TEXT,
    provincia            TEXT,
    codigo_postal        TEXT,
    telefono             TEXT,
    email                TEXT,
    notas                TEXT,
    activo               INTEGER NOT NULL DEFAULT 1,
    fecha_alta           TEXT NOT NULL DEFAULT (datetime('now','localtime')),
    fecha_modificacion   TEXT NOT NULL DEFAULT (datetime('now','localtime'))
);

CREATE INDEX IF NOT EXISTS idx_clientes_cuit      ON clientes(cuit_dni);
CREATE INDEX IF NOT EXISTS idx_clientes_razon     ON clientes(razon_social);
CREATE INDEX IF NOT EXISTS idx_clientes_activo    ON clientes(activo);

-- Contadores correlativos (remitos, notas de crédito, etc.)
CREATE TABLE IF NOT EXISTS contadores (
    nombre           TEXT PRIMARY KEY,
    ultimo_numero    INTEGER NOT NULL DEFAULT 0,
    fecha_modificacion TEXT NOT NULL DEFAULT (datetime('now','localtime'))
);

-- Cabecera de remitos
CREATE TABLE IF NOT EXISTS remitos (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    numero                INTEGER NOT NULL UNIQUE,       -- correlativo único (0001 hacia arriba)
    numero_formateado     TEXT NOT NULL,                 -- "R-0000001"
    cliente_id            INTEGER NOT NULL REFERENCES clientes(id) ON DELETE RESTRICT,
    fecha                 TEXT NOT NULL DEFAULT (date('now','localtime')),
    condicion_venta       TEXT,                          -- "contado", "cuenta corriente", "transferencia", etc.
    forma_pago            TEXT,                          -- texto libre (ej: "Efectivo", "Transferencia BBVA", ...)
    subtotal              REAL NOT NULL DEFAULT 0,
    descuento_general     REAL NOT NULL DEFAULT 0,       -- descuento aplicado al total (en pesos)
    total                 REAL NOT NULL DEFAULT 0,
    observaciones         TEXT,
    estado                TEXT NOT NULL DEFAULT 'emitido',  -- emitido, anulado
    fecha_anulacion       TEXT,
    motivo_anulacion      TEXT,
    pdf_path              TEXT,                          -- ruta del PDF generado
    fecha_creacion        TEXT NOT NULL DEFAULT (datetime('now','localtime'))
);

CREATE INDEX IF NOT EXISTS idx_remitos_cliente   ON remitos(cliente_id);
CREATE INDEX IF NOT EXISTS idx_remitos_fecha     ON remitos(fecha);
CREATE INDEX IF NOT EXISTS idx_remitos_estado    ON remitos(estado);

-- Líneas/items de remitos
CREATE TABLE IF NOT EXISTS remitos_items (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    remito_id         INTEGER NOT NULL REFERENCES remitos(id) ON DELETE CASCADE,
    orden             INTEGER NOT NULL DEFAULT 0,        -- orden visual en el PDF
    producto_id       INTEGER REFERENCES productos(id) ON DELETE SET NULL,  -- NULL = línea libre
    sku               TEXT,                              -- snapshot del SKU al momento del remito
    descripcion       TEXT NOT NULL,
    cantidad          INTEGER NOT NULL,
    precio_unitario   REAL NOT NULL DEFAULT 0,
    descuento_porc    REAL NOT NULL DEFAULT 0,           -- % de descuento aplicado a la línea
    subtotal          REAL NOT NULL DEFAULT 0,           -- cantidad * precio * (1 - desc%/100)
    es_linea_libre    INTEGER NOT NULL DEFAULT 0         -- 1 = sin producto_id (mano de obra, flete, etc.)
);

CREATE INDEX IF NOT EXISTS idx_remitems_remito   ON remitos_items(remito_id);
CREATE INDEX IF NOT EXISTS idx_remitems_producto ON remitos_items(producto_id);

-- Cabecera de notas de crédito
CREATE TABLE IF NOT EXISTS notas_credito (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    numero                INTEGER NOT NULL UNIQUE,
    numero_formateado     TEXT NOT NULL,                 -- "NC-0000001"
    cliente_id            INTEGER NOT NULL REFERENCES clientes(id) ON DELETE RESTRICT,
    remito_id             INTEGER REFERENCES remitos(id) ON DELETE SET NULL,  -- NULL si es independiente
    fecha                 TEXT NOT NULL DEFAULT (date('now','localtime')),
    motivo                TEXT NOT NULL,                 -- "devolución", "bonificación", "ajuste", etc.
    detalle_motivo        TEXT,
    subtotal              REAL NOT NULL DEFAULT 0,
    total                 REAL NOT NULL DEFAULT 0,
    reingreso_stock       INTEGER NOT NULL DEFAULT 1,    -- bool: ¿vuelve stock al inventario?
    estado                TEXT NOT NULL DEFAULT 'emitida',  -- emitida, anulada
    pdf_path              TEXT,
    fecha_creacion        TEXT NOT NULL DEFAULT (datetime('now','localtime'))
);

CREATE INDEX IF NOT EXISTS idx_nc_cliente  ON notas_credito(cliente_id);
CREATE INDEX IF NOT EXISTS idx_nc_remito   ON notas_credito(remito_id);
CREATE INDEX IF NOT EXISTS idx_nc_fecha    ON notas_credito(fecha);

-- Líneas de notas de crédito
CREATE TABLE IF NOT EXISTS notas_credito_items (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    nota_credito_id   INTEGER NOT NULL REFERENCES notas_credito(id) ON DELETE CASCADE,
    orden             INTEGER NOT NULL DEFAULT 0,
    producto_id       INTEGER REFERENCES productos(id) ON DELETE SET NULL,
    sku               TEXT,
    descripcion       TEXT NOT NULL,
    cantidad          INTEGER NOT NULL,
    precio_unitario   REAL NOT NULL DEFAULT 0,
    descuento_porc    REAL NOT NULL DEFAULT 0,
    subtotal          REAL NOT NULL DEFAULT 0,
    es_linea_libre    INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_ncitems_nc        ON notas_credito_items(nota_credito_id);
CREATE INDEX IF NOT EXISTS idx_ncitems_producto  ON notas_credito_items(producto_id);

-- ==========================================================
-- FASE 3: Publicaciones ML masivas
-- ==========================================================

-- Compatibilidades vehiculares: un producto puede aplicar a N autos.
-- Los datos vienen del PDF ZEN (columna 'APLICACIÓN') o de cargas manuales
-- del usuario. ML pide marca + modelo como mínimo, y opcionalmente año y motor.
CREATE TABLE IF NOT EXISTS compatibilidades_vehiculares (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    producto_id   INTEGER NOT NULL REFERENCES productos(id) ON DELETE CASCADE,
    marca         TEXT NOT NULL,          -- FORD, CHEVROLET, VW, CUMMINS, etc.
    modelo        TEXT,                   -- APACHE, C20, F100, ISB, etc.
    anio_desde    INTEGER,                -- ej: 2008
    anio_hasta    INTEGER,                -- ej: 2015 (NULL = sin tope)
    motor         TEXT,                   -- "1.6 CARB", "2.8 TURBO", etc.
    notas         TEXT,                   -- texto libre para detalles adicionales
    fecha_alta    TEXT NOT NULL DEFAULT (datetime('now','localtime'))
);

CREATE INDEX IF NOT EXISTS idx_compat_producto   ON compatibilidades_vehiculares(producto_id);
CREATE INDEX IF NOT EXISTS idx_compat_marca      ON compatibilidades_vehiculares(marca);
CREATE INDEX IF NOT EXISTS idx_compat_modelo     ON compatibilidades_vehiculares(modelo);

-- Drafts de publicación: borradores armados por el builder antes de
-- postear a ML. Cada draft tiene producto_id y los campos del payload;
-- después del POST exitoso se copia el ml_item_id resultante.
CREATE TABLE IF NOT EXISTS publicaciones_drafts (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    producto_id        INTEGER NOT NULL REFERENCES productos(id) ON DELETE CASCADE,
    titulo             TEXT NOT NULL,
    descripcion        TEXT,
    category_id_ml     TEXT,
    listing_type_id    TEXT NOT NULL DEFAULT 'gold_pro',
    condition_ml       TEXT NOT NULL DEFAULT 'new',
    precio             REAL NOT NULL DEFAULT 0,
    currency           TEXT NOT NULL DEFAULT 'ARS',
    stock              INTEGER NOT NULL DEFAULT 0,
    atributos_json     TEXT,              -- JSON con atributos ML
    pictures_json      TEXT,              -- JSON con lista de paths/URLs
    shipping_mode      TEXT DEFAULT 'me2',
    warranty_type      TEXT,              -- "Garantía del vendedor", etc.
    warranty_time      TEXT,              -- "30 días", "6 meses", etc.
    estado             TEXT NOT NULL DEFAULT 'borrador',  -- borrador, validado, publicado, error
    mensaje_error      TEXT,
    ml_item_id         TEXT,              -- se llena después del POST exitoso
    ml_permalink       TEXT,              -- URL pública de la publicación en ML
    ml_status          TEXT,              -- "paused", "active", etc. (estado lado ML)
    fecha_creacion     TEXT NOT NULL DEFAULT (datetime('now','localtime')),
    fecha_publicacion  TEXT,
    fecha_modificacion TEXT NOT NULL DEFAULT (datetime('now','localtime'))
);

CREATE INDEX IF NOT EXISTS idx_drafts_producto   ON publicaciones_drafts(producto_id);
CREATE INDEX IF NOT EXISTS idx_drafts_estado     ON publicaciones_drafts(estado);

-- Ficha técnica por producto (key-value extensible).
-- ZEN aporta T/G/L/SPL/ID/D/FAM/CW-CCW (dientes, ø externo, largo, estrías,
-- ø bucha, ø capa, familia, sentido de giro). Otros proveedores podrían
-- aportar voltaje, KW, etc. sin cambiar schema.
CREATE TABLE IF NOT EXISTS fichas_tecnicas (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    producto_id    INTEGER NOT NULL REFERENCES productos(id) ON DELETE CASCADE,
    clave          TEXT NOT NULL,              -- "Dientes", "ø piñón", "Largo", "Giro", ...
    valor          TEXT,                       -- "9", "25.8", "80.5", "Horario", ...
    unidad         TEXT,                       -- "mm", "V", "KW", etc. (opcional)
    orden          INTEGER NOT NULL DEFAULT 0,
    fecha_alta     TEXT NOT NULL DEFAULT (datetime('now','localtime')),
    UNIQUE(producto_id, clave)
);

CREATE INDEX IF NOT EXISTS idx_ficha_producto ON fichas_tecnicas(producto_id);
"""


# Valores iniciales de categorías basados en los rubros de ZEN
# + los rubros nuevos del negocio (embragues, suspensión, frenos, tren delantero)
CATEGORIAS_INICIALES = [
    # (nombre, descripcion, coef_default)
    ("Impulsores de arranque",          "Impulsores y burros de arranque",              1.50),
    ("Poleas de alternador",            "Poleas de alternador y similares",             1.50),
    ("Cubre impulsores",                "Cubre impulsores de arranque",                 1.50),
    ("Tapas/portaescobillas/barrilito", "Tapas, portaescobillas y barrilitos",          1.50),
    ("Despiece de alternador",          "Repuestos sueltos de alternador",              1.50),
    ("Despiece de arranque",            "Repuestos sueltos de arranque",                1.50),
    ("Rodamientos",                     "Rodamientos tensores de correa (INA/SKF)",     1.50),
    ("Campos de arranque",              "Campos de arranque varios",                    1.50),
    ("Pistones",                        "Pistones para vehículos livianos y pesados",   1.50),
    ("Blocks de motor",                 "Blocks de motor",                              1.50),
    ("Tapas de cilindro",               "Tapas de cilindro",                            1.50),
    ("Árboles de levas",                "Árboles de levas",                             1.50),
    ("Piezas interior de motor",        "Componentes internos del motor",               1.50),
    ("Embragues",                       "Kits de embrague, platos, discos",             1.50),
    ("Suspensión",                      "Amortiguadores, espirales, bujes",             1.50),
    ("Tren delantero",                  "Extremos, rótulas, bieletas",                  1.50),
    ("Frenos",                          "Pastillas, discos, tambores, cilindros",       1.50),
    ("Herramientas / ferretería",       "Bulones, herramientas, consumibles",           1.50),
    ("Taller",                          "Servicios y mano de obra",                     1.00),
    ("Sin categoría",                   "A categorizar manualmente",                    1.50),
]


# Marcas de autos más comunes en el mercado argentino (para auto-detección)
MARCAS_AUTO_INICIALES = [
    "CHEVROLET", "FORD", "FIAT", "VOLKSWAGEN", "VW", "RENAULT", "PEUGEOT",
    "CITROEN", "TOYOTA", "HONDA", "NISSAN", "SUZUKI", "MITSUBISHI",
    "HYUNDAI", "KIA", "MERCEDES BENZ", "MERCEDES-BENZ", "M.BENZ", "MBB",
    "IVECO", "SCANIA", "VOLVO", "MAN", "DAF", "HINO", "ISUZU",
    "AUDI", "BMW", "JEEP", "DODGE", "RAM", "CHRYSLER",
    "JOHN DEERE", "JD", "CASE", "AGRALE", "IKA",
    "MASSEY FERGUSON", "NEW HOLLAND", "VALTRA", "DEUTZ",
    "MWM", "PERKINS", "CUMMINS", "MACK", "FREIGHTLINER",
]


CONFIG_INICIAL = [
    ("coeficiente_global_default", "1.50",
     "Coeficiente que se aplica a todo producto nuevo si su categoría no lo tiene configurado."),
    ("iva_alicuota",               "21",
     "Alícuota de IVA (en %) para cálculos de precio final."),
    ("ultima_sincronizacion_ml",   "",
     "Timestamp de la última sincronización con ML."),
]


SCHEMA_VERSION_ACTUAL = 5


# Semilla de contadores de Fase 2
CONTADORES_INICIALES = [
    ("remito",       0),
    ("nota_credito", 0),
]


# ==========================================================
# Conexión
# ==========================================================
def _conectar(path: Path | None = None) -> sqlite3.Connection:
    """Abre una conexión con row_factory y foreign_keys habilitado."""
    ruta = path or config.DATABASE_FILE
    conn = sqlite3.connect(str(ruta))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


@contextmanager
def conexion() -> Iterator[sqlite3.Connection]:
    """
    Context manager de conexión. Commit si no hubo excepción, rollback si hubo.

    Uso:
        with conexion() as c:
            c.execute("INSERT INTO ...")
    """
    conn = _conectar()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ==========================================================
# Migración incremental: asegurar columna
# ==========================================================
def _ensure_column(conn: sqlite3.Connection, tabla: str, col: str, tipo: str) -> bool:
    """
    Agrega una columna a una tabla existente si aún no existe. Usa PRAGMA
    table_info para detectar. Útil para migraciones entre versiones de schema
    sin romper DBs viejas.
    Devuelve True si efectivamente se agregó la columna.
    """
    rows = conn.execute(f"PRAGMA table_info({tabla})").fetchall()
    cols_existentes = {r["name"] if isinstance(r, sqlite3.Row) else r[1] for r in rows}
    if col in cols_existentes:
        return False
    conn.execute(f"ALTER TABLE {tabla} ADD COLUMN {col} {tipo}")
    log.info(f"  migración: agregada columna {tabla}.{col} ({tipo})")
    return True


# ==========================================================
# Inicialización del schema
# ==========================================================
def inicializar_db(path: Path | None = None) -> None:
    """
    Crea la base de datos si no existe. Idempotente: se puede correr muchas veces.
    - Crea las tablas.
    - Siembra categorías, marcas de auto y configuración.
    - Marca la versión de schema.
    """
    ruta = path or config.DATABASE_FILE
    ruta.parent.mkdir(parents=True, exist_ok=True)

    log.info(f"Inicializando base de datos en {ruta}")
    conn = _conectar(ruta)
    try:
        conn.executescript(SCHEMA_SQL)

        # --- Migraciones incrementales (ALTER TABLE para DBs existentes) ---
        _ensure_column(conn, "publicaciones_drafts", "ml_permalink", "TEXT")
        _ensure_column(conn, "publicaciones_drafts", "ml_status",    "TEXT")

        # Versión de schema
        cur = conn.execute("SELECT version FROM schema_version LIMIT 1")
        row = cur.fetchone()
        if row is None:
            conn.execute("INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION_ACTUAL,))

        # Seed categorías (solo si la tabla está vacía)
        total_cat = conn.execute("SELECT COUNT(*) AS n FROM categorias").fetchone()["n"]
        if total_cat == 0:
            conn.executemany(
                "INSERT INTO categorias (nombre, descripcion, coeficiente_default) VALUES (?,?,?)",
                CATEGORIAS_INICIALES,
            )
            log.info(f"  → {len(CATEGORIAS_INICIALES)} categorías cargadas")

        # Seed marcas de auto
        total_marcas = conn.execute("SELECT COUNT(*) AS n FROM marcas_auto").fetchone()["n"]
        if total_marcas == 0:
            conn.executemany(
                "INSERT INTO marcas_auto (nombre) VALUES (?)",
                [(m,) for m in MARCAS_AUTO_INICIALES],
            )
            log.info(f"  → {len(MARCAS_AUTO_INICIALES)} marcas de auto cargadas")

        # Seed configuración
        for clave, valor, notas in CONFIG_INICIAL:
            conn.execute(
                "INSERT OR IGNORE INTO configuracion (clave, valor, notas) VALUES (?,?,?)",
                (clave, valor, notas),
            )

        # Seed contadores (solo si no existen)
        for nombre, inicial in CONTADORES_INICIALES:
            conn.execute(
                "INSERT OR IGNORE INTO contadores (nombre, ultimo_numero) VALUES (?, ?)",
                (nombre, inicial),
            )

        # Actualizar la versión de schema si cambió
        conn.execute("UPDATE schema_version SET version = ?", (SCHEMA_VERSION_ACTUAL,))

        conn.commit()
        log.info("Base de datos lista.")
    finally:
        conn.close()


# ==========================================================
# Helpers varios
# ==========================================================
def obtener_config(clave: str, default: str | None = None) -> str | None:
    with conexion() as c:
        row = c.execute("SELECT valor FROM configuracion WHERE clave = ?", (clave,)).fetchone()
        return row["valor"] if row else default


def guardar_config(clave: str, valor: str, notas: str | None = None) -> None:
    with conexion() as c:
        c.execute(
            """
            INSERT INTO configuracion (clave, valor, notas)
            VALUES (?, ?, ?)
            ON CONFLICT(clave) DO UPDATE SET
                valor = excluded.valor,
                notas = COALESCE(excluded.notas, configuracion.notas),
                fecha_modificacion = datetime('now','localtime')
            """,
            (clave, valor, notas),
        )


def obtener_categorias(solo_activas: bool = True) -> list[sqlite3.Row]:
    with conexion() as c:
        sql = "SELECT * FROM categorias"
        if solo_activas:
            sql += " WHERE activa = 1"
        sql += " ORDER BY nombre"
        return list(c.execute(sql).fetchall())


def obtener_marcas_auto() -> list[str]:
    with conexion() as c:
        return [r["nombre"] for r in c.execute("SELECT nombre FROM marcas_auto ORDER BY nombre")]


def siguiente_numero(nombre: str, conn: sqlite3.Connection | None = None) -> int:
    """
    Incrementa el contador atómicamente y devuelve el nuevo valor.

    Si se pasa una conexión abierta, la usa (para integrarlo a una transacción
    mayor, como la creación de un remito). Si no, abre una transacción propia.
    """
    def _do(c: sqlite3.Connection) -> int:
        c.execute(
            "INSERT OR IGNORE INTO contadores (nombre, ultimo_numero) VALUES (?, 0)",
            (nombre,),
        )
        c.execute(
            "UPDATE contadores SET ultimo_numero = ultimo_numero + 1, "
            "fecha_modificacion = datetime('now','localtime') WHERE nombre = ?",
            (nombre,),
        )
        row = c.execute(
            "SELECT ultimo_numero FROM contadores WHERE nombre = ?", (nombre,)
        ).fetchone()
        return int(row["ultimo_numero"])

    if conn is not None:
        return _do(conn)
    with conexion() as c:
        return _do(c)


def formatear_numero_documento(prefijo: str, numero: int, ancho: int = 7) -> str:
    """'R', 42 → 'R-0000042'"""
    return f"{prefijo}-{numero:0{ancho}d}"


# ==========================================================
# Entry point (para ejecutar manualmente)
# ==========================================================
if __name__ == "__main__":
    inicializar_db()
    print("Base de datos inicializada correctamente.")
    print(f"  Ubicación: {config.DATABASE_FILE}")
    cats = obtener_categorias()
    print(f"  Categorías activas: {len(cats)}")
    for c in cats[:5]:
        print(f"    - {c['nombre']} (coef {c['coeficiente_default']})")
    if len(cats) > 5:
        print(f"    ... y {len(cats)-5} más")
