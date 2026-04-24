"""
remitos/importador_historico.py
===============================
Importación histórica de clientes, remitos y notas de crédito desde CSVs.

Diferencia con remitos/service.py y remitos/nc_service.py:
  - NO toca stock (los remitos históricos ya se entregaron antes de existir el sistema).
  - Respeta el número original de cada remito/NC (no usa el contador correlativo).
  - Acepta SKUs desconocidos convirtiéndolos en "línea libre".

Pensado para correrse UNA vez al migrar los datos del Google Sheet a SQLite.
"""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import db
from logger import get_logger

log = get_logger(__name__)


# ==========================================================
# Helpers de parseo
# ==========================================================
def parsear_fecha(raw: str) -> Optional[str]:
    """'10/9/2025' → '2025-09-10'. Devuelve None si no puede parsear."""
    if not raw or not raw.strip():
        return None
    s = raw.strip()
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", s)
    if not m:
        return None
    d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
    try:
        return f"{y:04d}-{mo:02d}-{d:02d}"
    except Exception:
        return None


def parsear_monto(raw: str) -> float:
    """
    Convierte texto a float. Los CSVs vienen con notación inglesa ('170000.00'),
    pero igual toleramos coma decimal por las dudas.
    """
    if raw is None:
        return 0.0
    s = str(raw).strip().replace("$", "").replace(" ", "")
    if not s:
        return 0.0
    # Si tiene coma y no tiene punto, es coma decimal (formato argentino)
    if s.count(",") == 1 and s.count(".") == 0:
        s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return 0.0


def limpiar_cuit(raw: str) -> Optional[str]:
    """
    Deja sólo dígitos. Acepta cualquier identificador con al menos 7 dígitos
    (cubre CUIT de 11, DNI de 7-8 y algunos identificadores parciales del Sheet).
    Descarta placeholders tipo 'X', 'XXXXXX', '-', vacío.
    """
    if raw is None:
        return None
    digits = re.sub(r"\D", "", str(raw))
    if len(digits) < 7:
        return None
    return digits


def separar_provincia_cp(raw: str) -> tuple[Optional[str], Optional[str]]:
    """
    Parser de la columna 'PROVINCIA_CP' del Sheet:
      'Buenos Aires, CP 1880' → ('Buenos Aires', '1880')
      'Buenos Aires'          → ('Buenos Aires', None)
      'Buenos Aires, CP'      → ('Buenos Aires', None)
      'Buenos Aires, 1880'    → ('Buenos Aires', '1880')
      'Buenos Aires,'         → ('Buenos Aires', None)
      'Buenos Aires, CP2800'  → ('Buenos Aires', '2800')
    """
    if not raw or not str(raw).strip():
        return None, None
    s = str(raw).strip().rstrip(",").strip()
    if "," in s:
        left, right = s.split(",", 1)
        provincia = left.strip() or None
        cp_raw = right.strip()
        cp_digits = re.sub(r"\D", "", cp_raw)
        cp = cp_digits if cp_digits else None
        return provincia, cp
    return s or None, None


# ==========================================================
# Limpieza de datos de prueba (Fase 2)
# ==========================================================
CLIENTES_PRUEBA_NOMBRES = {
    "taller mecánica del sur",
    "taller mecanica del sur",
    "juan pérez",
    "juan perez",
}
REMITOS_PRUEBA_NUMEROS = (1, 2)
NC_PRUEBA_NUMEROS = (1,)


def limpiar_datos_prueba() -> dict:
    """Borra remitos/NC/clientes de prueba creados en Fase 2. Idempotente."""
    resumen = {"remitos_borrados": 0, "nc_borradas": 0, "clientes_borrados": 0}

    with db.conexion() as c:
        # NC primero (puede referenciar remitos)
        for n in NC_PRUEBA_NUMEROS:
            row = c.execute("SELECT id FROM notas_credito WHERE numero = ?", (n,)).fetchone()
            if row:
                c.execute("DELETE FROM notas_credito_items WHERE nota_credito_id = ?", (row["id"],))
                c.execute("DELETE FROM notas_credito WHERE id = ?", (row["id"],))
                resumen["nc_borradas"] += 1

        for n in REMITOS_PRUEBA_NUMEROS:
            row = c.execute("SELECT id FROM remitos WHERE numero = ?", (n,)).fetchone()
            if row:
                c.execute("DELETE FROM remitos_items WHERE remito_id = ?", (row["id"],))
                c.execute("DELETE FROM remitos WHERE id = ?", (row["id"],))
                resumen["remitos_borrados"] += 1

        for nombre in CLIENTES_PRUEBA_NOMBRES:
            rows = c.execute(
                "SELECT id, razon_social FROM clientes WHERE LOWER(razon_social) = ?",
                (nombre,),
            ).fetchall()
            for r in rows:
                tiene_r = c.execute(
                    "SELECT 1 FROM remitos WHERE cliente_id = ? LIMIT 1", (r["id"],)
                ).fetchone()
                tiene_nc = c.execute(
                    "SELECT 1 FROM notas_credito WHERE cliente_id = ? LIMIT 1", (r["id"],)
                ).fetchone()
                if tiene_r or tiene_nc:
                    log.warning(
                        f"Cliente de prueba '{r['razon_social']}' (id={r['id']}) "
                        f"tiene documentos asociados: no lo borro."
                    )
                    continue
                c.execute("DELETE FROM clientes WHERE id = ?", (r["id"],))
                resumen["clientes_borrados"] += 1

    log.info(f"Limpieza de datos de prueba: {resumen}")
    return resumen


# ==========================================================
# Resultado común
# ==========================================================
@dataclass
class ResultadoImport:
    creados: int = 0
    omitidos: int = 0
    errores: int = 0
    detalle_errores: list[str] = field(default_factory=list)


# ==========================================================
# Búsquedas auxiliares
# ==========================================================
def _buscar_cliente_por_nombre(conn, nombre: str) -> Optional[int]:
    """Devuelve el id del cliente por razón social (case-insensitive)."""
    if not nombre:
        return None
    row = conn.execute(
        "SELECT id FROM clientes WHERE LOWER(TRIM(razon_social)) = LOWER(TRIM(?)) LIMIT 1",
        (nombre,),
    ).fetchone()
    return int(row["id"]) if row else None


def _buscar_producto_por_sku(conn, sku: str) -> tuple[Optional[int], Optional[str]]:
    """
    Busca un producto por sku_master, sku_proveedor o sku_ml (en ese orden).
    Devuelve (producto_id, sku_master_real).
    """
    if not sku:
        return None, None
    s = sku.strip()
    row = conn.execute(
        """
        SELECT id, sku_master FROM productos
        WHERE sku_master = ? OR sku_proveedor = ? OR sku_ml = ?
        LIMIT 1
        """,
        (s, s, s),
    ).fetchone()
    if row:
        return int(row["id"]), row["sku_master"]
    return None, None


# ==========================================================
# Clientes
# ==========================================================
def importar_clientes(csv_path: Path) -> ResultadoImport:
    """
    Lee el CSV de clientes y los inserta. Si ya existe uno con la misma
    razón social (case-insensitive), se omite.
    """
    res = ResultadoImport()

    with db.conexion() as conn, open(csv_path, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for linea, row in enumerate(reader, start=2):
            nombre = (row.get("NOMBRE") or "").strip()
            if not nombre:
                res.omitidos += 1
                continue

            if _buscar_cliente_por_nombre(conn, nombre) is not None:
                res.omitidos += 1
                continue

            try:
                provincia, cp = separar_provincia_cp(row.get("PROVINCIA_CP", ""))
                cuit = limpiar_cuit(row.get("CUIT", ""))
                email = (row.get("MAIL") or "").strip() or None
                telefono = (row.get("TELEFONO") or "").strip() or None
                direccion = (row.get("DIRECCION") or "").strip() or None

                conn.execute(
                    """
                    INSERT INTO clientes (
                        razon_social, cuit_dni, direccion, provincia, codigo_postal,
                        telefono, email, activo
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 1)
                    """,
                    (nombre, cuit, direccion, provincia, cp, telefono, email),
                )
                res.creados += 1
            except Exception as e:
                res.errores += 1
                res.detalle_errores.append(f"Línea {linea} '{nombre}': {e}")
                log.warning(f"Error importando cliente '{nombre}' (línea {linea}): {e}")

    log.info(
        f"Clientes: creados={res.creados} omitidos={res.omitidos} errores={res.errores}"
    )
    return res


# ==========================================================
# Remitos
# ==========================================================
def _agrupar_por(csv_path: Path, clave: str) -> dict[int, list[dict]]:
    """Lee un CSV y agrupa por la columna `clave` (que debe ser numérica)."""
    grupos: dict[int, list[dict]] = {}
    with open(csv_path, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                n = int(float((row.get(clave) or "").strip()))
            except (TypeError, ValueError):
                continue
            grupos.setdefault(n, []).append(row)
    return grupos


def _construir_items(conn, filas: list[dict]) -> tuple[list[dict], float]:
    """Arma la lista de items (dicts) y devuelve (items, subtotal)."""
    items: list[dict] = []
    subtotal = 0.0

    for idx, fila in enumerate(filas, start=1):
        sku_raw = (fila.get("CODIGO") or "").strip()
        descripcion = (fila.get("DESCRIPCION") or "").strip() or "(sin descripción)"

        try:
            cantidad = int(float((fila.get("CANTIDAD") or "1").strip()))
        except ValueError:
            cantidad = 1
        if cantidad == 0:
            cantidad = 1

        precio = parsear_monto(fila.get("PRECIO_UNITARIO", ""))
        importe = parsear_monto(fila.get("IMPORTE", ""))

        # IMPORTE del Sheet manda como subtotal autoritativo.
        # Si falta, lo calculamos.
        if importe == 0.0 and cantidad and precio:
            importe = round(cantidad * precio, 2)

        producto_id: Optional[int] = None
        sku_master: Optional[str] = None
        es_linea_libre = False

        if sku_raw:
            producto_id, sku_master = _buscar_producto_por_sku(conn, sku_raw)
            if producto_id is None:
                # SKU desconocido → línea libre, preservando el código original
                es_linea_libre = True
                sku_master = sku_raw
        else:
            es_linea_libre = True
            sku_master = None

        items.append(
            {
                "orden": idx,
                "producto_id": producto_id,
                "sku": sku_master,
                "descripcion": descripcion,
                "cantidad": cantidad,
                "precio_unitario": precio,
                "subtotal": importe,
                "es_linea_libre": 1 if es_linea_libre else 0,
            }
        )
        subtotal += importe

    return items, round(subtotal, 2)


def importar_remitos(csv_path: Path) -> ResultadoImport:
    """Importa remitos históricos respetando el número original y sin tocar stock."""
    res = ResultadoImport()
    grupos = _agrupar_por(csv_path, "REMITO")

    with db.conexion() as conn:
        for numero, filas in sorted(grupos.items()):
            # Evitar pisar un remito ya existente
            ya = conn.execute(
                "SELECT id FROM remitos WHERE numero = ?", (numero,)
            ).fetchone()
            if ya:
                res.omitidos += 1
                continue

            primera = filas[0]
            nombre_cli = (primera.get("NOMBRE") or "").strip()
            cliente_id = _buscar_cliente_por_nombre(conn, nombre_cli)
            if cliente_id is None:
                res.errores += 1
                res.detalle_errores.append(
                    f"Remito {numero}: cliente no encontrado '{nombre_cli}'"
                )
                log.warning(f"Remito {numero}: cliente '{nombre_cli}' no existe, se omite.")
                continue

            fecha = parsear_fecha(primera.get("FECHA", ""))

            notas_unicas: list[str] = []
            for fila in filas:
                n = (fila.get("NOTAS") or "").strip()
                if n and n not in notas_unicas:
                    notas_unicas.append(n)
            observaciones = " | ".join(notas_unicas) if notas_unicas else None

            items_data, subtotal = _construir_items(conn, filas)
            total = subtotal
            numero_fmt = db.formatear_numero_documento("R", numero)

            try:
                conn.execute(
                    """
                    INSERT INTO remitos (
                        numero, numero_formateado, cliente_id, fecha,
                        subtotal, descuento_general, total,
                        observaciones, estado
                    ) VALUES (?, ?, ?, ?, ?, 0, ?, ?, 'emitido')
                    """,
                    (
                        numero, numero_fmt, cliente_id, fecha,
                        subtotal, total, observaciones,
                    ),
                )
                remito_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]

                for it in items_data:
                    conn.execute(
                        """
                        INSERT INTO remitos_items (
                            remito_id, orden, producto_id, sku, descripcion,
                            cantidad, precio_unitario, descuento_porc, subtotal, es_linea_libre
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
                        """,
                        (
                            remito_id, it["orden"], it["producto_id"], it["sku"],
                            it["descripcion"], it["cantidad"], it["precio_unitario"],
                            it["subtotal"], it["es_linea_libre"],
                        ),
                    )
                res.creados += 1
            except Exception as e:
                res.errores += 1
                res.detalle_errores.append(f"Remito {numero}: {e}")
                log.warning(f"Error insertando remito {numero}: {e}")

    log.info(
        f"Remitos: creados={res.creados} omitidos={res.omitidos} errores={res.errores}"
    )
    return res


# ==========================================================
# Notas de crédito
# ==========================================================
def importar_nc(csv_path: Path) -> ResultadoImport:
    """Importa NCs históricas respetando el número original y sin tocar stock."""
    res = ResultadoImport()
    grupos = _agrupar_por(csv_path, "NC_NUMERO")

    with db.conexion() as conn:
        for numero, filas in sorted(grupos.items()):
            ya = conn.execute(
                "SELECT id FROM notas_credito WHERE numero = ?", (numero,)
            ).fetchone()
            if ya:
                res.omitidos += 1
                continue

            primera = filas[0]
            nombre_cli = (primera.get("NOMBRE") or "").strip()
            cliente_id = _buscar_cliente_por_nombre(conn, nombre_cli)
            if cliente_id is None:
                res.errores += 1
                res.detalle_errores.append(
                    f"NC {numero}: cliente no encontrado '{nombre_cli}'"
                )
                continue

            fecha = parsear_fecha(primera.get("FECHA", ""))

            notas_unicas: list[str] = []
            for fila in filas:
                n = (fila.get("NOTAS") or "").strip()
                if n and n not in notas_unicas:
                    notas_unicas.append(n)
            detalle_motivo = " | ".join(notas_unicas) if notas_unicas else None

            items_data, subtotal = _construir_items(conn, filas)
            total = subtotal
            numero_fmt = db.formatear_numero_documento("NC", numero)

            try:
                conn.execute(
                    """
                    INSERT INTO notas_credito (
                        numero, numero_formateado, cliente_id, fecha,
                        motivo, detalle_motivo, subtotal, total,
                        reingreso_stock, estado
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 'emitida')
                    """,
                    (
                        numero, numero_fmt, cliente_id, fecha,
                        "Histórico", detalle_motivo, subtotal, total,
                    ),
                )
                nc_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]

                for it in items_data:
                    conn.execute(
                        """
                        INSERT INTO notas_credito_items (
                            nota_credito_id, orden, producto_id, sku, descripcion,
                            cantidad, precio_unitario, descuento_porc, subtotal, es_linea_libre
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
                        """,
                        (
                            nc_id, it["orden"], it["producto_id"], it["sku"],
                            it["descripcion"], it["cantidad"], it["precio_unitario"],
                            it["subtotal"], it["es_linea_libre"],
                        ),
                    )
                res.creados += 1
            except Exception as e:
                res.errores += 1
                res.detalle_errores.append(f"NC {numero}: {e}")
                log.warning(f"Error insertando NC {numero}: {e}")

    log.info(f"NC: creadas={res.creados} omitidas={res.omitidos} errores={res.errores}")
    return res


# ==========================================================
# Contadores
# ==========================================================
def ajustar_contadores() -> dict:
    """Setea ultimo_numero al MAX real de cada tabla para que el próximo correlativo sea correcto."""
    with db.conexion() as c:
        rem_max = c.execute("SELECT COALESCE(MAX(numero), 0) AS n FROM remitos").fetchone()["n"]
        nc_max = c.execute("SELECT COALESCE(MAX(numero), 0) AS n FROM notas_credito").fetchone()["n"]
        c.execute(
            """
            INSERT INTO contadores (nombre, ultimo_numero) VALUES ('remito', ?)
            ON CONFLICT(nombre) DO UPDATE SET
                ultimo_numero = excluded.ultimo_numero,
                fecha_modificacion = datetime('now','localtime')
            """,
            (rem_max,),
        )
        c.execute(
            """
            INSERT INTO contadores (nombre, ultimo_numero) VALUES ('nota_credito', ?)
            ON CONFLICT(nombre) DO UPDATE SET
                ultimo_numero = excluded.ultimo_numero,
                fecha_modificacion = datetime('now','localtime')
            """,
            (nc_max,),
        )
    log.info(f"Contadores ajustados: remito={rem_max}, nota_credito={nc_max}")
    return {"remito": rem_max, "nota_credito": nc_max}
