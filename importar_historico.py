"""
importar_historico.py
=====================
CLI para cargar a la base SQLite los datos históricos (clientes + remitos + NC)
que vivían en el Google Sheet antes del sistema.

Flujo:
 1. Chequea que existan los 3 CSVs en data/import/.
 2. Muestra el estado inicial.
 3. Paso a paso, con confirmación:
      - Limpia datos de prueba (Fase 2).
      - Importa clientes.
      - Importa remitos (sin tocar stock, respetando el N° original).
      - Importa notas de crédito (idem).
 4. Ajusta los contadores al MAX real.
 5. Muestra un resumen final con totales y próximo correlativo.

Pensado para correr UNA sola vez.

Uso:
    (venv) PS > python importar_historico.py
"""

from __future__ import annotations

import sys

import config
import db
from logger import get_logger
from remitos import importador_historico as imp

log = get_logger(__name__)


IMPORT_DIR = config.DATA_DIR / "import"
PATH_CLIENTES = IMPORT_DIR / "clientes.csv"
PATH_REMITOS = IMPORT_DIR / "remitos.csv"
PATH_NC = IMPORT_DIR / "nc.csv"


def _confirmar(pregunta: str, *, default_si: bool = True) -> bool:
    sufijo = "[S/n]" if default_si else "[s/N]"
    try:
        r = input(f"{pregunta} {sufijo}: ").strip().lower()
    except EOFError:
        return default_si
    if not r:
        return default_si
    return r in ("s", "si", "sí", "y", "yes")


def _linea(char: str = "=", largo: int = 64) -> str:
    return char * largo


def _estado_actual() -> dict:
    with db.conexion() as c:
        n_cli = c.execute("SELECT COUNT(*) AS n FROM clientes").fetchone()["n"]
        n_rem = c.execute("SELECT COUNT(*) AS n FROM remitos").fetchone()["n"]
        n_nc = c.execute("SELECT COUNT(*) AS n FROM notas_credito").fetchone()["n"]
        rows = c.execute(
            "SELECT nombre, ultimo_numero FROM contadores ORDER BY nombre"
        ).fetchall()
        contadores = {r["nombre"]: int(r["ultimo_numero"]) for r in rows}
    return {
        "clientes": int(n_cli),
        "remitos": int(n_rem),
        "notas_credito": int(n_nc),
        "contadores": contadores,
    }


def _imprimir_errores(titulo: str, errores: list[str], max_mostrar: int = 10) -> None:
    if not errores:
        return
    print(f"\n  Detalle de {titulo} (hasta {max_mostrar}):")
    for e in errores[:max_mostrar]:
        print(f"    - {e}")
    if len(errores) > max_mostrar:
        print(f"    ... y {len(errores) - max_mostrar} más. Mirá el log para el detalle completo.")


def main() -> int:
    print(_linea())
    print("  Primi Motors — Importador histórico")
    print(_linea())

    # Verificar CSVs
    faltantes = [p for p in (PATH_CLIENTES, PATH_REMITOS, PATH_NC) if not p.exists()]
    if faltantes:
        print("\n✗ Faltan CSVs en data/import/:")
        for p in faltantes:
            print(f"    - {p}")
        return 1

    # Asegurar schema
    db.inicializar_db()

    estado_0 = _estado_actual()
    print("\nEstado inicial de la base:")
    print(f"  Clientes:         {estado_0['clientes']}")
    print(f"  Remitos:          {estado_0['remitos']}")
    print(f"  Notas de crédito: {estado_0['notas_credito']}")
    print(f"  Contadores:       {estado_0['contadores']}")

    # ------------------------------------------------------------------
    # 1) Limpieza de datos de prueba
    # ------------------------------------------------------------------
    print(f"\n{_linea('-')}")
    print("Paso 1 — Limpiar datos de prueba (Fase 2)")
    print("       Borra remitos #1 y #2, NC #1, y clientes 'Taller Mecánica del Sur' / 'Juan Pérez'.")
    if _confirmar("¿Avanzo con la limpieza?"):
        r = imp.limpiar_datos_prueba()
        print(f"  → Remitos borrados:  {r['remitos_borrados']}")
        print(f"  → NC borradas:       {r['nc_borradas']}")
        print(f"  → Clientes borrados: {r['clientes_borrados']}")
    else:
        print("  (saltado)")

    # ------------------------------------------------------------------
    # 2) Clientes
    # ------------------------------------------------------------------
    print(f"\n{_linea('-')}")
    print("Paso 2 — Importar clientes")
    print(f"       Archivo: {PATH_CLIENTES}")
    if _confirmar("¿Importo los clientes?"):
        r = imp.importar_clientes(PATH_CLIENTES)
        print(f"  → Creados:  {r.creados}")
        print(f"  → Omitidos: {r.omitidos}  (ya existían o sin nombre)")
        print(f"  → Errores:  {r.errores}")
        _imprimir_errores("errores de clientes", r.detalle_errores)
    else:
        print("  (saltado)")

    # ------------------------------------------------------------------
    # 3) Remitos
    # ------------------------------------------------------------------
    print(f"\n{_linea('-')}")
    print("Paso 3 — Importar remitos (sin mover stock, respetando N° original)")
    print(f"       Archivo: {PATH_REMITOS}")
    if _confirmar("¿Importo los remitos?"):
        r = imp.importar_remitos(PATH_REMITOS)
        print(f"  → Creados:  {r.creados}")
        print(f"  → Omitidos: {r.omitidos}  (ya existían)")
        print(f"  → Errores:  {r.errores}")
        _imprimir_errores("errores de remitos", r.detalle_errores)
    else:
        print("  (saltado)")

    # ------------------------------------------------------------------
    # 4) Notas de crédito
    # ------------------------------------------------------------------
    print(f"\n{_linea('-')}")
    print("Paso 4 — Importar notas de crédito")
    print(f"       Archivo: {PATH_NC}")
    if _confirmar("¿Importo las notas de crédito?"):
        r = imp.importar_nc(PATH_NC)
        print(f"  → Creadas:  {r.creados}")
        print(f"  → Omitidas: {r.omitidos}  (ya existían)")
        print(f"  → Errores:  {r.errores}")
        _imprimir_errores("errores de NC", r.detalle_errores)
    else:
        print("  (saltado)")

    # ------------------------------------------------------------------
    # 5) Ajuste de contadores
    # ------------------------------------------------------------------
    print(f"\n{_linea('-')}")
    print("Paso 5 — Ajustar contadores al máximo real")
    ajuste = imp.ajustar_contadores()
    print(f"  → contador remito        = {ajuste['remito']}       (próximo: R-{ajuste['remito'] + 1:07d})")
    print(f"  → contador nota_credito  = {ajuste['nota_credito']}  (próximo: NC-{ajuste['nota_credito'] + 1:07d})")

    # ------------------------------------------------------------------
    # Resumen final
    # ------------------------------------------------------------------
    estado_1 = _estado_actual()
    print(f"\n{_linea()}")
    print("  RESUMEN FINAL")
    print(_linea())
    print(f"  Clientes:         {estado_0['clientes']:>5}  →  {estado_1['clientes']:>5}   (+{estado_1['clientes'] - estado_0['clientes']})")
    print(f"  Remitos:          {estado_0['remitos']:>5}  →  {estado_1['remitos']:>5}   (+{estado_1['remitos'] - estado_0['remitos']})")
    print(f"  Notas de crédito: {estado_0['notas_credito']:>5}  →  {estado_1['notas_credito']:>5}   (+{estado_1['notas_credito'] - estado_0['notas_credito']})")
    print(f"\n  Próximo remito:  R-{estado_1['contadores'].get('remito', 0) + 1:07d}")
    print(f"  Próxima NC:      NC-{estado_1['contadores'].get('nota_credito', 0) + 1:07d}")
    print(_linea())
    print("  Listo. Si algo no te cerró, revisá data/logs/ para el log completo.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
