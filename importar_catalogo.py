"""
importar_catalogo.py
====================
Script CLI interactivo para importar catálogos de proveedores a la base local.

Por ahora soporta el formato ZEN (dos Excel: precios + stock).

Uso:
    python importar_catalogo.py

Flujo:
  1. Inicializa la DB si no existe.
  2. Pregunta las rutas de los archivos.
  3. Corre el importador (con o sin dry-run).
  4. Muestra el resumen.
  5. Ofrece sincronizar con las publicaciones ML.
"""

from __future__ import annotations

from pathlib import Path

import db
from inventory.importer import importar_zen
from inventory.ml_linker import sincronizar_publicaciones_ml
from inventory.search import resumen_catalogo
from logger import get_logger

log = get_logger(__name__)


def _pedir_archivo(nombre: str, ejemplo: str) -> Path | None:
    """Pide al usuario la ruta de un archivo. ENTER vacío = omitir."""
    while True:
        raw = input(f"Ruta del archivo de {nombre} (ENTER = omitir, ej: {ejemplo}): ").strip()
        if not raw:
            return None
        # Permitir que el usuario escriba la ruta con o sin comillas
        raw = raw.strip('"').strip("'")
        p = Path(raw)
        if p.exists() and p.is_file():
            return p
        print(f"  ❌ No encuentro el archivo: {p}")
        print("     Probá de nuevo o ENTER para omitir.")


def _mostrar_resumen_catalogo() -> None:
    r = resumen_catalogo()
    print()
    print("📊 Estado actual del catálogo")
    print("─" * 60)
    print(f"  Productos activos:        {r.total_productos}")
    print(f"    con stock:              {r.con_stock}")
    print(f"    sin stock:              {r.sin_stock}")
    print(f"    por pedido:             {r.por_pedido}")
    print(f"  Valor en inventario a costo:  ${r.total_valor_costo:,.2f}")
    print(f"  Valor en inventario a venta:  ${r.total_valor_venta:,.2f}")
    print()
    print("  Top categorías:")
    for nombre, n in r.categorias[:8]:
        print(f"    • {nombre:<35} {n}")
    if r.marcas_auto:
        print()
        print("  Top marcas de auto:")
        for nombre, n in r.marcas_auto[:8]:
            print(f"    • {nombre:<20} {n}")


def main() -> None:
    print("=" * 70)
    print("   PRIMI MOTORS — Importación de catálogo desde proveedor")
    print("=" * 70)
    print()

    # 1. Asegurar DB inicializada
    db.inicializar_db()

    # 2. Pedir archivos
    print("Proveedor a importar: ZEN (formato actualmente soportado)")
    print()
    archivo_precios = _pedir_archivo(
        "PRECIOS ZEN (CODART/DESCRIPCIO/MARCA/PRCOSTO)",
        r"C:\...\LISTA PRECIOS ZEN (2).XLS",
    )
    archivo_stock = _pedir_archivo(
        "STOCK ZEN (CODIGO/RUBRO/DESCRIPCION/STK)",
        r"C:\...\LISTA ZEN STOCK SIN PRECIO.xlsx",
    )

    if not archivo_precios and not archivo_stock:
        print("❌ No se especificó ningún archivo. Saliendo.")
        return

    # 3. ¿Dry run o de verdad?
    print()
    raw = input("¿Correr en modo SIMULACIÓN (dry run) primero? [s/N]: ").strip().lower()
    dry_run = raw in ("s", "si", "sí", "y", "yes")

    print()
    print("🚀 Iniciando importación..." + (" (dry run)" if dry_run else ""))
    resultado = importar_zen(
        archivo_precios,
        archivo_stock,
        saltar_reacondicionados=True,
        proveedor="ZEN",
        dry_run=dry_run,
    )

    print()
    print(resultado.resumen_texto())
    print()

    if dry_run:
        raw = input("¿Ejecutar de verdad ahora? [s/N]: ").strip().lower()
        if raw in ("s", "si", "sí", "y", "yes"):
            print()
            print("🚀 Ejecutando importación real...")
            resultado = importar_zen(
                archivo_precios,
                archivo_stock,
                saltar_reacondicionados=True,
                proveedor="ZEN",
                dry_run=False,
            )
            print()
            print(resultado.resumen_texto())

    # 4. Resumen
    _mostrar_resumen_catalogo()

    # 5. Ofrecer sincronización con ML
    print()
    raw = input("¿Linkear ahora con las publicaciones activas de Mercado Libre? [S/n]: ").strip().lower()
    if raw in ("", "s", "si", "sí", "y", "yes"):
        print()
        print("🔗 Sincronizando con Mercado Libre...")
        try:
            res_ml = sincronizar_publicaciones_ml(solo_activas=True)
            print()
            print(res_ml.resumen_texto())
            if res_ml.productos_sin_publicacion:
                n = len(res_ml.productos_sin_publicacion)
                print()
                print(f"💡 Tenés {n} productos en el inventario que NO están publicados en ML.")
                print("   Los podrás publicar masivamente desde la Fase 3 (módulo Publicaciones).")
        except Exception as e:
            print(f"⚠ No pude sincronizar con ML: {e}")
            log.exception("Error sincronizando con ML")

    print()
    print("=" * 70)
    print("  ✅ LISTO")
    print("=" * 70)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⚠ Cancelado por el usuario.")
    except Exception as e:
        log.exception("Error fatal en importación")
        print(f"\n❌ Ocurrió un error: {e}")
        print("   Revisá data/logs/primi_motors.log para más detalle.")
