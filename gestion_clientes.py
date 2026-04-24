"""
gestion_clientes.py
===================
CLI interactiva para gestionar clientes de Primi Motors.

Uso:
    python gestion_clientes.py
"""

from __future__ import annotations

import sys

import db
from clientes import repo as clientes_repo
from clientes.repo import (
    CONDICIONES_IVA,
    Cliente,
    formatear_cuit,
    validar_cuit,
)
from logger import get_logger

log = get_logger(__name__)


# ==========================================================
# Helpers de input
# ==========================================================
def _pedir(label: str, default: str | None = None, obligatorio: bool = False) -> str | None:
    if default:
        raw = input(f"  {label} [{default}]: ").strip()
        return raw or default
    sufijo = " *" if obligatorio else " (ENTER para omitir)"
    while True:
        raw = input(f"  {label}{sufijo}: ").strip()
        if raw:
            return raw
        if not obligatorio:
            return None
        print("    ⚠ Este campo es obligatorio.")


def _pedir_condicion_iva(default: str | None = None) -> str | None:
    print("  Condición IVA:")
    for i, c in enumerate(CONDICIONES_IVA, start=1):
        marca = "→" if c == default else " "
        print(f"    {marca} {i}) {c}")
    while True:
        raw = input(f"  Elegí 1-{len(CONDICIONES_IVA)} (ENTER = {default or 'omitir'}): ").strip()
        if not raw:
            return default
        if raw.isdigit() and 1 <= int(raw) <= len(CONDICIONES_IVA):
            return CONDICIONES_IVA[int(raw) - 1]
        print("    ⚠ Opción inválida.")


# ==========================================================
# Operaciones
# ==========================================================
def _mostrar_cliente(c: Cliente) -> None:
    print()
    print(f"  ID:                 {c.id}")
    print(f"  Razón social:       {c.razon_social}")
    if c.nombre_comercial:
        print(f"  Nombre comercial:   {c.nombre_comercial}")
    if c.cuit_dni:
        print(f"  CUIT/DNI:           {formatear_cuit(c.cuit_dni)}")
    if c.condicion_iva:
        print(f"  Condición IVA:      {c.condicion_iva}")
    if c.direccion_completa():
        print(f"  Dirección:          {c.direccion_completa()}")
    if c.telefono:
        print(f"  Teléfono:           {c.telefono}")
    if c.email:
        print(f"  Email:              {c.email}")
    if c.notas:
        print(f"  Notas:              {c.notas}")
    print(f"  Activo:             {'sí' if c.activo else 'no'}")
    print(f"  Alta:               {c.fecha_alta}")


def _mostrar_historial(cliente_id: int) -> None:
    resumen = clientes_repo.resumen_historial(cliente_id)
    print()
    print("  📊 Resumen del historial:")
    print(f"     Remitos emitidos:          {resumen.total_remitos}")
    print(f"     Notas de crédito:          {resumen.total_notas_credito}")
    print(f"     Monto total remitos:       ${resumen.monto_total_remitos:,.2f}")
    print(f"     Monto total NC:            ${resumen.monto_total_nc:,.2f}")
    print(f"     Monto neto (rem - NC):     ${resumen.monto_neto:,.2f}")
    if resumen.ultima_compra:
        print(f"     Última operación:          {resumen.ultima_compra}")

    entradas = clientes_repo.historial(cliente_id, limite=20)
    if not entradas:
        return
    print()
    print("  🕐 Últimas operaciones:")
    for e in entradas:
        tipo = "REMITO" if e.tipo == "remito" else "NC    "
        ref = f"  → {e.referencia}" if e.referencia else ""
        print(f"     {e.fecha}  {tipo}  {e.numero:<14}  ${e.total:>12,.2f}  {e.estado}{ref}")


def alta_cliente() -> None:
    print()
    print("═" * 60)
    print("  ALTA DE CLIENTE")
    print("═" * 60)

    razon = _pedir("Razón social", obligatorio=True)
    nombre_comercial = _pedir("Nombre comercial / fantasía")
    cuit_raw = _pedir("CUIT o DNI (con o sin guiones)")
    if cuit_raw:
        if len(clientes_repo.normalizar_cuit_dni(cuit_raw) or "") == 11 and not validar_cuit(cuit_raw):
            print("    ⚠ El dígito verificador del CUIT no es válido. Lo guardamos igual.")
    condicion = _pedir_condicion_iva()
    direccion = _pedir("Dirección (calle y número)")
    localidad = _pedir("Localidad")
    provincia = _pedir("Provincia")
    cp = _pedir("Código postal")
    telefono = _pedir("Teléfono")
    email = _pedir("Email")
    notas = _pedir("Notas")

    try:
        nuevo_id = clientes_repo.crear(Cliente(
            razon_social=razon or "",
            nombre_comercial=nombre_comercial,
            cuit_dni=cuit_raw,
            condicion_iva=condicion,
            direccion=direccion,
            localidad=localidad,
            provincia=provincia,
            codigo_postal=cp,
            telefono=telefono,
            email=email,
            notas=notas,
        ))
        print()
        print(f"  ✅ Cliente creado con id = {nuevo_id}")
    except ValueError as e:
        print(f"  ❌ {e}")


def buscar_cliente() -> Cliente | None:
    print()
    texto = input("  Texto a buscar (razón social, CUIT, email, localidad): ").strip()
    resultados = clientes_repo.buscar(texto or None, limite=20)
    if not resultados:
        print("  No se encontraron clientes.")
        return None
    print()
    print(f"  {len(resultados)} resultado(s):")
    for i, c in enumerate(resultados, start=1):
        linea = f"    {i:>2}) [{c.id:>4}] {c.razon_social}"
        if c.cuit_dni:
            linea += f"  ({formatear_cuit(c.cuit_dni)})"
        if c.localidad:
            linea += f"  {c.localidad}"
        print(linea)

    raw = input("  Elegí número (ENTER = volver): ").strip()
    if not raw.isdigit():
        return None
    idx = int(raw)
    if 1 <= idx <= len(resultados):
        return resultados[idx - 1]
    return None


def editar_cliente(c: Cliente) -> None:
    print()
    print(f"  ✏ Editando cliente id={c.id}  —  {c.razon_social}")
    print("    (ENTER = dejar valor actual)")
    c.razon_social     = _pedir("Razón social",     c.razon_social,     obligatorio=True) or c.razon_social
    c.nombre_comercial = _pedir("Nombre comercial", c.nombre_comercial)
    c.cuit_dni         = _pedir("CUIT/DNI",         formatear_cuit(c.cuit_dni) if c.cuit_dni else None)
    c.condicion_iva    = _pedir_condicion_iva(c.condicion_iva)
    c.direccion        = _pedir("Dirección",        c.direccion)
    c.localidad        = _pedir("Localidad",        c.localidad)
    c.provincia        = _pedir("Provincia",        c.provincia)
    c.codigo_postal    = _pedir("Código postal",    c.codigo_postal)
    c.telefono         = _pedir("Teléfono",         c.telefono)
    c.email            = _pedir("Email",            c.email)
    c.notas            = _pedir("Notas",            c.notas)

    try:
        clientes_repo.actualizar(c)
        print("  ✅ Cliente actualizado.")
    except ValueError as e:
        print(f"  ❌ {e}")


def baja_cliente(c: Cliente) -> None:
    raw = input(f"  ¿Seguro que querés dar de baja a '{c.razon_social}'? [s/N]: ").strip().lower()
    if raw in ("s", "si", "sí", "y", "yes"):
        clientes_repo.dar_de_baja(c.id or 0)
        print("  ✅ Cliente marcado como inactivo.")


# ==========================================================
# Menú principal
# ==========================================================
def menu() -> None:
    while True:
        print()
        print("═" * 60)
        print("   PRIMI MOTORS — Gestión de clientes")
        print("═" * 60)
        print("  1) Alta de cliente")
        print("  2) Buscar y ver cliente")
        print("  3) Listar todos los clientes")
        print("  0) Salir")
        op = input("  Opción: ").strip()

        if op == "1":
            alta_cliente()
        elif op == "2":
            c = buscar_cliente()
            if c is None:
                continue
            _mostrar_cliente(c)
            _mostrar_historial(c.id or 0)
            print()
            print("  Acciones:")
            print("    E) Editar    B) Dar de baja    ENTER) volver")
            acc = input("  Acción: ").strip().lower()
            if acc == "e":
                editar_cliente(c)
            elif acc == "b":
                baja_cliente(c)
        elif op == "3":
            clientes = clientes_repo.listar_todos(solo_activos=True)
            if not clientes:
                print("  (sin clientes activos)")
            else:
                print()
                print(f"  Total activos: {len(clientes)}")
                for cl in clientes:
                    print(f"    [{cl.id:>4}] {cl.razon_social:<40} {formatear_cuit(cl.cuit_dni) if cl.cuit_dni else '':<15} {cl.localidad or ''}")
        elif op == "0":
            return
        else:
            print("  Opción inválida.")


if __name__ == "__main__":
    try:
        db.inicializar_db()
        menu()
    except KeyboardInterrupt:
        print("\n⚠ Cancelado por el usuario.")
        sys.exit(0)
    except Exception as e:
        log.exception("Error fatal en gestión de clientes")
        print(f"\n❌ Ocurrió un error: {e}")
        print("   Revisá data/logs/primi_motors.log para más detalle.")
