"""
nueva_nota_credito.py
=====================
CLI interactiva para emitir una nota de crédito.

Dos caminos:
  - Asociada a un remito: se traen los items del remito y se ajustan.
  - Independiente: se cargan items desde cero.

Siempre reingresa stock al inventario (decisión de Primi Motors).

Uso:
    python nueva_nota_credito.py
"""

from __future__ import annotations

import sys

import db
from clientes import repo as clientes_repo
from clientes.repo import formatear_cuit
from inventory.search import buscar as buscar_productos
from logger import get_logger
from remitos import nc_pdf, nc_service
from remitos import service as remitos_service
from remitos.nc_service import MOTIVOS_NC, ItemNC

log = get_logger(__name__)


# ==========================================================
# Helpers
# ==========================================================
def _elegir_cliente():
    while True:
        texto = input("  Buscar cliente: ").strip()
        resultados = clientes_repo.buscar(texto or None, limite=15)
        if not resultados:
            print("  No se encontraron clientes.")
            return None
        print()
        for i, c in enumerate(resultados, start=1):
            cuit = formatear_cuit(c.cuit_dni) if c.cuit_dni else ""
            print(f"    {i:>2}) [{c.id:>4}] {c.razon_social:<35} {cuit:<15}")
        raw = input("  Elegí número (ENTER = buscar de nuevo): ").strip()
        if raw.isdigit() and 1 <= int(raw) <= len(resultados):
            return resultados[int(raw) - 1]


def _elegir_remito_de_cliente(cliente_id: int) -> int | None:
    remitos = remitos_service.listar(cliente_id=cliente_id, estado="emitido", limite=30)
    if not remitos:
        print("  Este cliente no tiene remitos emitidos. La NC será independiente.")
        return None
    print()
    for i, r in enumerate(remitos, start=1):
        print(f"    {i:>2}) {r['numero_formateado']}  fecha {r['fecha']}  total ${r['total']:,.2f}")
    raw = input("  Número (ENTER = NC independiente, sin remito asociado): ").strip()
    if raw.isdigit() and 1 <= int(raw) <= len(remitos):
        return int(remitos[int(raw) - 1]["id"])
    return None


def _elegir_motivo() -> str:
    print("  Motivo:")
    for i, m in enumerate(MOTIVOS_NC, start=1):
        print(f"    {i}) {m}")
    while True:
        raw = input(f"  Elegí 1-{len(MOTIVOS_NC)}: ").strip()
        if raw.isdigit() and 1 <= int(raw) <= len(MOTIVOS_NC):
            return MOTIVOS_NC[int(raw) - 1]
        print("  Opción inválida.")


def _ajustar_items_desde_remito(items_remito: list[ItemNC]) -> list[ItemNC]:
    print()
    print("  Items del remito:")
    for i, it in enumerate(items_remito, start=1):
        sku = it.sku or ("—" if it.es_linea_libre else "")
        print(f"    {i:>2}) {sku:<10}  cant {it.cantidad:>3}  ${it.precio_unitario:>10,.2f}  {it.descripcion[:50]}")
    print()
    print("  Para cada línea te pregunto cuántas unidades entran en la NC.")
    print("  (0 = descartar la línea)")
    resultado: list[ItemNC] = []
    for it in items_remito:
        cant_raw = input(f"    {it.descripcion[:50]} (remito: {it.cantidad}): ").strip()
        if not cant_raw:
            resultado.append(it)
            continue
        try:
            nueva = int(cant_raw)
        except ValueError:
            print("    Valor inválido, mantengo la cantidad original.")
            resultado.append(it)
            continue
        if nueva <= 0:
            continue
        if nueva > it.cantidad:
            print(f"    ⚠ No podés exceder la cantidad del remito ({it.cantidad}). Ajustado.")
            nueva = it.cantidad
        it.cantidad = nueva
        resultado.append(it)
    return resultado


def _agregar_item_libre() -> ItemNC | None:
    print()
    print("  Agregar item:")
    print("    P) Producto del inventario")
    print("    L) Línea libre")
    opc = input("  Opción: ").strip().lower()
    if opc == "p":
        texto = input("    Buscar: ").strip()
        if not texto:
            return None
        resultados = buscar_productos(texto, limite=10)
        for i, r in enumerate(resultados, start=1):
            print(f"      {i}) {r['sku_master']:<10} {r['descripcion'][:60]}  ${r['precio_venta']:,.2f}")
        raw = input("    Elegí número: ").strip()
        if not raw.isdigit():
            return None
        prod = resultados[int(raw) - 1]
        cant_raw = input("    Cantidad: ").strip()
        if not cant_raw.isdigit() or int(cant_raw) <= 0:
            return None
        precio_raw = input(f"    Precio [{prod['precio_venta']:.2f}]: ").strip()
        precio = float(precio_raw.replace(",", ".")) if precio_raw else float(prod["precio_venta"] or 0)
        return ItemNC(
            descripcion=prod["descripcion"], cantidad=int(cant_raw),
            precio_unitario=precio, producto_id=prod["id"], sku=prod["sku_master"],
            es_linea_libre=False,
        )
    elif opc == "l":
        desc = input("    Descripción: ").strip()
        if not desc:
            return None
        cant_raw = input("    Cantidad: ").strip()
        if not cant_raw.isdigit() or int(cant_raw) <= 0:
            return None
        precio_raw = input("    Precio: ").strip()
        precio = float(precio_raw.replace(",", ".")) if precio_raw else 0.0
        return ItemNC(
            descripcion=desc, cantidad=int(cant_raw), precio_unitario=precio,
            producto_id=None, sku=None, es_linea_libre=True,
        )
    return None


# ==========================================================
# Flujo principal
# ==========================================================
def main() -> None:
    print()
    print("═" * 60)
    print("  PRIMI MOTORS — Nueva Nota de Crédito")
    print("═" * 60)

    # 1) Cliente
    cliente = _elegir_cliente()
    if cliente is None:
        print("  Cancelado.")
        return

    # 2) ¿Asociar a un remito?
    print()
    print("  ¿La NC refiere a un remito existente?")
    print("    S) Sí, elegir remito")
    print("    N) No, NC independiente")
    asoc = input("  Opción [S/n]: ").strip().lower()
    remito_id = None
    items: list[ItemNC] = []

    if asoc in ("", "s", "si", "sí", "y", "yes"):
        remito_id = _elegir_remito_de_cliente(cliente.id or 0)
        if remito_id is not None:
            items = nc_service.copiar_items_de_remito(remito_id)
            items = _ajustar_items_desde_remito(items)

    # Si no hay remito (o fue independiente), cargar items manualmente
    if not items:
        print()
        print("  Cargá los items de la NC.")
        while True:
            nuevo = _agregar_item_libre()
            if nuevo:
                items.append(nuevo)
                print(f"    + {nuevo.descripcion[:60]}  subtotal ${nuevo.subtotal:,.2f}")
            raw = input("  ¿Agregar otro item? [s/N]: ").strip().lower()
            if raw not in ("s", "si", "sí", "y", "yes"):
                break

    if not items:
        print("  La NC no tiene items. Cancelado.")
        return

    # 3) Motivo
    print()
    motivo = _elegir_motivo()
    detalle = input("  Detalle adicional (ENTER = omitir): ").strip() or None

    # 4) Confirmar
    subtotal = sum(it.subtotal for it in items)
    print()
    print("─" * 60)
    print("  RESUMEN")
    print("─" * 60)
    print(f"  Cliente:    {cliente.razon_social}")
    print(f"  Remito:     {remito_id if remito_id else 'independiente'}")
    print(f"  Motivo:     {motivo}")
    print(f"  Items:      {len(items)}")
    print(f"  Total:      ${subtotal:,.2f}")
    print(f"  Stock:      se reingresará al inventario")

    raw = input("  ¿Confirmar emisión? [S/n]: ").strip().lower()
    if raw not in ("", "s", "si", "sí", "y", "yes"):
        print("  Cancelado.")
        return

    # 5) Crear + PDF
    try:
        nc = nc_service.crear_nota_credito(
            cliente_id=cliente.id or 0,
            items=items,
            motivo=motivo,
            detalle_motivo=detalle,
            remito_id=remito_id,
            reingreso_stock=True,
        )
        print()
        print(f"  ✅ NC {nc.numero_formateado} emitida — total ${nc.total:,.2f}")

        pdf_path = nc_pdf.generar_pdf(nc.id)
        print(f"  📄 PDF generado: {pdf_path}")

    except nc_service.NotaCreditoError as e:
        print(f"  ❌ {e}")


if __name__ == "__main__":
    try:
        db.inicializar_db()
        main()
    except KeyboardInterrupt:
        print("\n⚠ Cancelado por el usuario.")
        sys.exit(0)
    except Exception as e:
        log.exception("Error fatal en nueva NC")
        print(f"\n❌ Ocurrió un error: {e}")
        print("   Revisá data/logs/primi_motors.log para más detalle.")
