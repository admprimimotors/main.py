"""
nuevo_remito.py
===============
CLI interactiva para emitir un remito nuevo.

Flujo:
  1. Elegir cliente (buscando por texto).
  2. Agregar items (productos del inventario o líneas libres).
  3. Opcionales: condición de venta, forma de pago, descuento general, observaciones.
  4. Confirmar → crea remito + descuenta stock + genera PDF.

Uso:
    python nuevo_remito.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import db
from clientes import repo as clientes_repo
from clientes.repo import formatear_cuit
from inventory.search import buscar as buscar_productos
from logger import get_logger
from remitos import pdf as remito_pdf
from remitos import service
from remitos.service import ItemRemito

log = get_logger(__name__)


# ==========================================================
# Helpers
# ==========================================================
def _elegir_cliente():
    while True:
        texto = input("  Texto a buscar (razón social, CUIT, email): ").strip()
        resultados = clientes_repo.buscar(texto or None, limite=15)
        if not resultados:
            print("  No se encontraron clientes.")
            raw = input("  ¿Dar de alta un cliente nuevo ahora? [s/N]: ").strip().lower()
            if raw in ("s", "si", "sí", "y", "yes"):
                from gestion_clientes import alta_cliente
                alta_cliente()
                continue
            return None

        print()
        for i, c in enumerate(resultados, start=1):
            cuit = formatear_cuit(c.cuit_dni) if c.cuit_dni else ""
            print(f"    {i:>2}) [{c.id:>4}] {c.razon_social:<35} {cuit:<15} {c.localidad or ''}")
        raw = input("  Elegí número (ENTER = buscar de nuevo): ").strip()
        if raw.isdigit() and 1 <= int(raw) <= len(resultados):
            return resultados[int(raw) - 1]


def _agregar_item_producto() -> ItemRemito | None:
    texto = input("    Buscar producto (SKU o descripción): ").strip()
    if not texto:
        return None
    resultados = buscar_productos(texto, limite=15)
    if not resultados:
        print("    No se encontraron productos.")
        return None
    for i, r in enumerate(resultados, start=1):
        stock = r["stock_actual"]
        marca = r.get("marca_auto") or ""
        print(f"      {i:>2}) {r['sku_master']:<10}  stock={stock:>4}  ${r['precio_venta']:>10,.2f}  {marca[:12]:<12}  {r['descripcion'][:60]}")
    raw = input("    Elegí número (ENTER = cancelar): ").strip()
    if not raw.isdigit():
        return None
    idx = int(raw)
    if idx < 1 or idx > len(resultados):
        return None
    producto = resultados[idx - 1]

    # Cantidad
    cant_raw = input(f"    Cantidad (stock actual = {producto['stock_actual']}): ").strip()
    if not cant_raw.isdigit() or int(cant_raw) <= 0:
        print("    Cantidad inválida.")
        return None
    cantidad = int(cant_raw)

    # Precio (default = precio_venta del catálogo)
    precio_raw = input(f"    Precio unitario [{producto['precio_venta']:.2f}]: ").strip()
    precio = float(precio_raw.replace(",", ".")) if precio_raw else float(producto["precio_venta"] or 0)

    # Descuento %
    desc_raw = input("    Descuento % [0]: ").strip()
    descuento = float(desc_raw.replace(",", ".")) if desc_raw else 0.0

    return ItemRemito(
        descripcion=producto["descripcion"],
        cantidad=cantidad,
        precio_unitario=precio,
        descuento_porc=descuento,
        producto_id=producto["id"],
        sku=producto["sku_master"],
        es_linea_libre=False,
    )


def _agregar_linea_libre() -> ItemRemito | None:
    descripcion = input("    Descripción de la línea: ").strip()
    if not descripcion:
        return None
    cant_raw = input("    Cantidad: ").strip()
    if not cant_raw.isdigit() or int(cant_raw) <= 0:
        return None
    precio_raw = input("    Precio unitario: ").strip()
    precio = float(precio_raw.replace(",", ".")) if precio_raw else 0.0
    desc_raw = input("    Descuento % [0]: ").strip()
    descuento = float(desc_raw.replace(",", ".")) if desc_raw else 0.0
    return ItemRemito(
        descripcion=descripcion,
        cantidad=int(cant_raw),
        precio_unitario=precio,
        descuento_porc=descuento,
        producto_id=None,
        sku=None,
        es_linea_libre=True,
    )


def _mostrar_items(items: list[ItemRemito]) -> None:
    if not items:
        print("    (sin items)")
        return
    print()
    print(f"    {'#':<3}{'SKU':<12}{'Cant':>5}  {'P.Unit':>12}  {'Desc%':>6}  {'Subtotal':>14}  Descripción")
    print(f"    {'-' * 90}")
    for i, it in enumerate(items, start=1):
        sku = it.sku or ("—" if it.es_linea_libre else "")
        print(f"    {i:<3}{sku:<12}{it.cantidad:>5}  ${it.precio_unitario:>10,.2f}  {it.descuento_porc:>5.0f}%  ${it.subtotal:>12,.2f}  {it.descripcion[:40]}")


# ==========================================================
# Flujo principal
# ==========================================================
def main() -> None:
    print()
    print("═" * 60)
    print("  PRIMI MOTORS — Nuevo remito")
    print("═" * 60)

    # 1) Cliente
    print()
    print("  Paso 1/3 — Cliente")
    cliente = _elegir_cliente()
    if cliente is None:
        print("  Cancelado.")
        return
    print(f"  ✓ Cliente: [{cliente.id}] {cliente.razon_social}")

    # 2) Items
    print()
    print("  Paso 2/3 — Items del remito")
    items: list[ItemRemito] = []
    while True:
        _mostrar_items(items)
        print()
        print("    P) Agregar producto del inventario")
        print("    L) Agregar línea libre (mano de obra, flete, etc.)")
        print("    Q) Quitar última línea")
        print("    F) Finalizar y continuar")
        opc = input("  Opción: ").strip().lower()
        if opc == "p":
            it = _agregar_item_producto()
            if it:
                items.append(it)
        elif opc == "l":
            it = _agregar_linea_libre()
            if it:
                items.append(it)
        elif opc == "q":
            if items:
                quitado = items.pop()
                print(f"    ✂ Quitado: {quitado.descripcion[:60]}")
        elif opc == "f":
            if not items:
                print("    ⚠ Agregá al menos un item antes de finalizar.")
                continue
            break
        else:
            print("    Opción inválida.")

    # 3) Extras
    print()
    print("  Paso 3/3 — Datos finales")
    condicion = input("  Condición de venta (ej: 'Contado', 'Cuenta corriente'): ").strip() or None
    forma_pago = input("  Forma de pago (ej: 'Transferencia', 'Efectivo'): ").strip() or None
    desc_gral_raw = input("  Descuento general en $ [0]: ").strip()
    descuento_general = float(desc_gral_raw.replace(",", ".")) if desc_gral_raw else 0.0
    observaciones = input("  Observaciones: ").strip() or None

    # 4) Resumen y confirmación
    print()
    print("─" * 60)
    print("  RESUMEN DEL REMITO")
    print("─" * 60)
    print(f"  Cliente:            {cliente.razon_social}")
    print(f"  Items:              {len(items)}")
    subtotal = round(sum(it.subtotal for it in items), 2)
    print(f"  Subtotal:           ${subtotal:,.2f}")
    if descuento_general > 0:
        print(f"  Descuento general:  ${descuento_general:,.2f}")
    print(f"  TOTAL:              ${subtotal - descuento_general:,.2f}")
    _mostrar_items(items)

    print()
    raw = input("  ¿Confirmar y emitir el remito? [S/n]: ").strip().lower()
    if raw not in ("", "s", "si", "sí", "y", "yes"):
        print("  Cancelado.")
        return

    # 5) Crear
    try:
        remito = service.crear_remito(
            cliente_id=cliente.id or 0,
            items=items,
            condicion_venta=condicion,
            forma_pago=forma_pago,
            descuento_general=descuento_general,
            observaciones=observaciones,
        )
        print()
        print(f"  ✅ Remito {remito.numero_formateado} emitido — total ${remito.total:,.2f}")
    except service.StockInsuficienteError as e:
        print()
        print(f"  ❌ {e}")
        print("     Ajustá cantidades y probá de nuevo.")
        return
    except service.RemitoError as e:
        print(f"  ❌ {e}")
        return

    # 6) PDF
    try:
        pdf_path = remito_pdf.generar_pdf(remito.id)
        print(f"  📄 PDF generado: {pdf_path}")
    except Exception as e:
        log.exception("Error generando PDF")
        print(f"  ⚠ El remito se emitió pero falló la generación del PDF: {e}")


if __name__ == "__main__":
    try:
        db.inicializar_db()
        main()
    except KeyboardInterrupt:
        print("\n⚠ Cancelado por el usuario.")
        sys.exit(0)
    except Exception as e:
        log.exception("Error fatal en nuevo remito")
        print(f"\n❌ Ocurrió un error: {e}")
        print("   Revisá data/logs/primi_motors.log para más detalle.")
