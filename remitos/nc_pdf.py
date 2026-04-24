"""
remitos/nc_pdf.py
=================
Generación del PDF de Notas de Crédito. Reutiliza los estilos definidos
en remitos/pdf.py para mantener la identidad visual.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.platypus import (
    HRFlowable,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

import config
from clientes import repo as clientes_repo
from clientes.repo import formatear_cuit
from logger import get_logger
from remitos import nc_service
from remitos.pdf import (  # reutilizamos estilos/colores del remito
    COLOR_AZUL,
    COLOR_CLARO,
    COLOR_LINEA,
    _bloque_firma,
    _encabezado,
    _estilos,
    _formatear_monto,
    _tabla_items,
)

log = get_logger(__name__)


# ==========================================================
# Bloques específicos de NC
# ==========================================================
def _encabezado_nc(nc, estilos) -> Table:
    """Mismo layout que el encabezado del remito, pero dice 'NOTA DE CRÉDITO'."""
    from reportlab.lib.enums import TA_RIGHT
    from reportlab.lib.styles import ParagraphStyle

    titulo = ParagraphStyle(
        "titulo_nc", parent=estilos["titulo_remito"], textColor=colors.HexColor("#8B2E2E"),
    )
    subtitulo = estilos["subtitulo_remito"]
    num_style = ParagraphStyle(
        "num_nc", parent=estilos["numero_remito"], textColor=colors.HexColor("#8B2E2E"),
    )

    logo_path = config.ASSETS_DIR / "logo.png"
    col_izq_items = []
    if logo_path.exists():
        try:
            from reportlab.platypus import Image
            col_izq_items.append(Image(str(logo_path), width=32 * mm, height=32 * mm, kind="proportional"))
            col_izq_items.append(Spacer(1, 2 * mm))
        except Exception as e:
            log.warning(f"No pude cargar logo: {e}")

    empresa = config.EMPRESA
    col_izq_items += [
        Paragraph(empresa["nombre"], estilos["empresa_nombre"]),
        Paragraph(empresa.get("direccion", ""), estilos["empresa_dato"]),
        Paragraph(empresa.get("provincia_cp", ""), estilos["empresa_dato"]),
        Paragraph(f"CUIT: {empresa.get('cuit', '')}", estilos["empresa_dato"]),
        Paragraph(empresa.get("email", ""), estilos["empresa_dato"]),
    ]

    col_der = [
        Paragraph("NOTA DE CRÉDITO", titulo),
        Paragraph("Documento interno — no válido como factura", subtitulo),
        Paragraph(f"N° {nc.numero_formateado}", num_style),
        Paragraph(f"Fecha: {nc.fecha}", estilos["texto_normal"]),
    ]
    if nc.remito_numero:
        col_der.append(Paragraph(f"Referencia: Remito {nc.remito_numero}", estilos["texto_pequeno"]))

    tabla = Table([[col_izq_items, col_der]], colWidths=[110 * mm, 70 * mm])
    tabla.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
    ]))
    return tabla


def _bloque_cliente(nc, estilos):
    cliente = clientes_repo.obtener(nc.cliente_id)
    if cliente is None:
        return Paragraph("Cliente no encontrado.", estilos["texto_normal"])

    razon = f"<b>{cliente.razon_social}</b>"
    if cliente.nombre_comercial:
        razon += f" <font color='#808080'>({cliente.nombre_comercial})</font>"

    linea_cuit = ""
    if cliente.cuit_dni:
        linea_cuit = f"CUIT/DNI: <b>{formatear_cuit(cliente.cuit_dni)}</b>"
    if cliente.condicion_iva:
        linea_cuit = (linea_cuit + "    " if linea_cuit else "") + f"Condición IVA: {cliente.condicion_iva}"

    items = [
        Paragraph("CLIENTE", estilos["etiqueta_seccion"]),
        Paragraph(razon, estilos["texto_normal"]),
    ]
    if linea_cuit:
        items.append(Paragraph(linea_cuit, estilos["texto_normal"]))
    if cliente.direccion_completa():
        items.append(Paragraph(cliente.direccion_completa(), estilos["texto_normal"]))

    t = Table([[items]], colWidths=[180 * mm])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), COLOR_CLARO),
        ("BOX", (0, 0), (-1, -1), 0.5, COLOR_LINEA),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
        ("TOPPADDING", (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
    ]))
    return t


def _bloque_motivo(nc, estilos):
    bits = [f"<b>Motivo:</b> {nc.motivo}"]
    if nc.detalle_motivo:
        bits.append(nc.detalle_motivo.replace("\n", "<br/>"))
    if nc.reingreso_stock:
        bits.append("<i>Esta NC reingresó mercadería al inventario.</i>")
    return Paragraph("<br/>".join(bits), estilos["texto_normal"])


def _bloque_total_nc(nc, estilos) -> Table:
    total_fmt = _formatear_monto(nc.total)
    filas = [[
        Paragraph("<b>TOTAL A ACREDITAR</b>", estilos["total_final"]),
        Paragraph(total_fmt, estilos["total_final"]),
    ]]
    t = Table(filas, colWidths=[60 * mm, 35 * mm])
    t.setStyle(TableStyle([
        ("LINEABOVE", (0, 0), (-1, 0), 0.5, COLOR_AZUL),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
    ]))
    caja = Table([["", t]], colWidths=[85 * mm, 95 * mm])
    caja.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP")]))
    return caja


# ==========================================================
# Generación
# ==========================================================
def generar_pdf(nc_id: int, destino: Optional[Path] = None) -> Path:
    nc = nc_service.obtener_nc(nc_id)
    if nc is None:
        raise ValueError(f"NC id={nc_id} no existe.")

    if destino is None:
        carpeta = config.DATA_DIR / "notas_credito"
        carpeta.mkdir(parents=True, exist_ok=True)
        destino = carpeta / f"{nc.numero_formateado}.pdf"

    destino = Path(destino)
    destino.parent.mkdir(parents=True, exist_ok=True)

    estilos = _estilos()

    doc = SimpleDocTemplate(
        str(destino), pagesize=A4,
        leftMargin=15 * mm, rightMargin=15 * mm,
        topMargin=15 * mm, bottomMargin=15 * mm,
        title=f"Nota de Crédito {nc.numero_formateado}",
        author="Primi Motors",
    )

    # Reutilizamos la tabla de items del remito pasándole un objeto con la misma forma
    # (cabezal .items con los mismos campos).
    class _ShimParaTablaItems:
        def __init__(self, items): self.items = items
    shim = _ShimParaTablaItems(nc.items)

    story = [
        _encabezado_nc(nc, estilos),
        Spacer(1, 6 * mm),
        HRFlowable(width="100%", thickness=0.5, color=COLOR_LINEA, spaceBefore=0, spaceAfter=6),
        _bloque_cliente(nc, estilos),
        Spacer(1, 3 * mm),
        _bloque_motivo(nc, estilos),
        Spacer(1, 6 * mm),
        _tabla_items(shim, estilos),
        Spacer(1, 4 * mm),
        _bloque_total_nc(nc, estilos),
        Spacer(1, 10 * mm),
        Paragraph(
            "Nota de crédito interna. Documento no válido como factura.",
            estilos["pie"],
        ),
    ]

    doc.build(story)
    nc_service.guardar_pdf_path(nc_id, str(destino))
    log.info(f"PDF NC generado: {destino}")
    return destino
