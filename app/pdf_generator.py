"""
app/pdf_generator.py
====================
Generación de PDFs de remitos y notas de crédito con ReportLab.

Portado y simplificado del sistema viejo (`remitos/pdf.py` + `nc_pdf.py`).
Devuelve bytes — el caller decide si los sirve por HTTP, los guarda a disco, etc.

Datos de la empresa: hardcoded como defaults, overridables por env vars
(EMPRESA_NOMBRE, EMPRESA_DIRECCION, EMPRESA_PROVINCIA_CP, EMPRESA_CUIT, EMPRESA_EMAIL).
"""

from __future__ import annotations

import io
import os
from typing import Optional

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import (
    HRFlowable,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

from .clientes import format_cuit_display
from .models import NotaCredito, Remito


# =============================================================
# Constantes y datos de empresa
# =============================================================

EMPRESA_DEFAULTS = {
    "nombre": "Primi Motors",
    "direccion": "Calle 15 4971, Berazategui",
    "provincia_cp": "Buenos Aires CP 1884",
    "cuit": "23-37354799-9",
    "email": "adm.primimotors@gmail.com",
}

COLOR_AZUL = colors.HexColor("#1F3864")
COLOR_GRIS = colors.HexColor("#404040")
COLOR_CLARO = colors.HexColor("#EAEAEA")
COLOR_LINEA = colors.HexColor("#B0B0B0")


def _empresa() -> dict:
    """Datos de empresa, con override por env vars."""
    return {
        "nombre": os.environ.get("EMPRESA_NOMBRE") or EMPRESA_DEFAULTS["nombre"],
        "direccion": os.environ.get("EMPRESA_DIRECCION") or EMPRESA_DEFAULTS["direccion"],
        "provincia_cp": os.environ.get("EMPRESA_PROVINCIA_CP") or EMPRESA_DEFAULTS["provincia_cp"],
        "cuit": os.environ.get("EMPRESA_CUIT") or EMPRESA_DEFAULTS["cuit"],
        "email": os.environ.get("EMPRESA_EMAIL") or EMPRESA_DEFAULTS["email"],
    }


# =============================================================
# Helpers de formato
# =============================================================

def _fmt_monto(valor) -> str:
    """29276.84 → '$ 29.276,84' (formato argentino)."""
    valor = float(valor or 0)
    entero = int(abs(valor))
    decimal = int(round((abs(valor) - entero) * 100))
    entero_fmt = f"{entero:,}".replace(",", ".")
    signo = "-" if valor < 0 else ""
    return f"{signo}$ {entero_fmt},{decimal:02d}"


def _estilos() -> dict:
    base = getSampleStyleSheet()
    return {
        "titulo_doc": ParagraphStyle(
            "titulo_doc", parent=base["Title"],
            fontSize=20, leading=24, textColor=COLOR_AZUL, alignment=TA_RIGHT, spaceAfter=2,
        ),
        "subtitulo_doc": ParagraphStyle(
            "subtitulo_doc", parent=base["Normal"],
            fontSize=8, textColor=COLOR_GRIS, alignment=TA_RIGHT, spaceAfter=6,
        ),
        "empresa_nombre": ParagraphStyle(
            "empresa_nombre", parent=base["Normal"],
            fontSize=16, leading=18, textColor=COLOR_AZUL, spaceAfter=2, fontName="Helvetica-Bold",
        ),
        "empresa_dato": ParagraphStyle(
            "empresa_dato", parent=base["Normal"],
            fontSize=8, textColor=COLOR_GRIS, leading=11,
        ),
        "numero_doc": ParagraphStyle(
            "numero_doc", parent=base["Normal"],
            fontSize=14, leading=16, textColor=COLOR_AZUL, alignment=TA_RIGHT, fontName="Helvetica-Bold",
        ),
        "etiqueta_seccion": ParagraphStyle(
            "etiqueta_seccion", parent=base["Normal"],
            fontSize=9, leading=11, textColor=COLOR_AZUL, fontName="Helvetica-Bold", spaceAfter=2,
        ),
        "texto_normal": ParagraphStyle(
            "texto_normal", parent=base["Normal"],
            fontSize=9, leading=11, textColor=COLOR_GRIS,
        ),
        "texto_pequeno": ParagraphStyle(
            "texto_pequeno", parent=base["Normal"],
            fontSize=8, leading=10, textColor=COLOR_GRIS,
        ),
        "pie": ParagraphStyle(
            "pie", parent=base["Normal"],
            fontSize=7, leading=9, textColor=COLOR_GRIS, alignment=TA_CENTER,
        ),
        "totales_label": ParagraphStyle(
            "totales_label", parent=base["Normal"],
            fontSize=9, leading=11, textColor=COLOR_GRIS, alignment=TA_RIGHT,
        ),
        "totales_valor": ParagraphStyle(
            "totales_valor", parent=base["Normal"],
            fontSize=9, leading=11, textColor=COLOR_GRIS, alignment=TA_RIGHT, fontName="Helvetica-Bold",
        ),
        "total_final": ParagraphStyle(
            "total_final", parent=base["Normal"],
            fontSize=12, leading=14, textColor=COLOR_AZUL, alignment=TA_RIGHT, fontName="Helvetica-Bold",
        ),
    }


# =============================================================
# Bloques compartidos
# =============================================================

def _encabezado(numero: int, fecha_str: str, tipo_doc: str, estilos: dict) -> Table:
    """Encabezado: empresa a la izquierda, tipo doc + número + fecha a la derecha."""
    empresa = _empresa()

    col_izq = [
        Paragraph(empresa["nombre"], estilos["empresa_nombre"]),
        Paragraph(empresa["direccion"], estilos["empresa_dato"]),
        Paragraph(empresa["provincia_cp"], estilos["empresa_dato"]),
        Paragraph(f"CUIT: {empresa['cuit']}", estilos["empresa_dato"]),
        Paragraph(empresa["email"], estilos["empresa_dato"]),
    ]
    subtitulo = (
        "Documento no válido como factura"
        if tipo_doc == "REMITO"
        else "Documento de crédito interno"
    )
    col_der = [
        Paragraph(tipo_doc, estilos["titulo_doc"]),
        Paragraph(subtitulo, estilos["subtitulo_doc"]),
        Paragraph(f"N° {numero:07d}", estilos["numero_doc"]),
        Paragraph(f"Fecha: {fecha_str}", estilos["texto_normal"]),
    ]

    tabla = Table([[col_izq, col_der]], colWidths=[110 * mm, 70 * mm])
    tabla.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
    ]))
    return tabla


def _bloque_cliente(cliente, estilos: dict) -> Table:
    """Caja con datos del cliente."""
    if cliente is None:
        return Paragraph("Cliente no encontrado.", estilos["texto_normal"])

    razon = f"<b>{cliente.razon_social}</b>"
    if cliente.nombre_comercial:
        razon += f" <font color='#808080'>({cliente.nombre_comercial})</font>"

    linea_cuit = ""
    if cliente.cuit_dni:
        linea_cuit = f"CUIT/DNI: <b>{format_cuit_display(cliente.cuit_dni)}</b>"
    if cliente.condicion_iva:
        linea_cuit = (linea_cuit + "    " if linea_cuit else "") + f"Condición IVA: {cliente.condicion_iva}"

    direccion_partes = [cliente.direccion, cliente.localidad, cliente.provincia]
    cp = f"CP {cliente.codigo_postal}" if cliente.codigo_postal else ""
    base = ", ".join(p for p in direccion_partes if p)
    direccion = f"{base} {cp}".strip(", ").strip()

    contacto_bits = []
    if cliente.telefono:
        contacto_bits.append(f"Tel: {cliente.telefono}")
    if cliente.email:
        contacto_bits.append(f"Email: {cliente.email}")
    contacto = "    ".join(contacto_bits)

    items = [
        Paragraph("CLIENTE", estilos["etiqueta_seccion"]),
        Paragraph(razon, estilos["texto_normal"]),
    ]
    if linea_cuit:
        items.append(Paragraph(linea_cuit, estilos["texto_normal"]))
    if direccion:
        items.append(Paragraph(direccion, estilos["texto_normal"]))
    if contacto:
        items.append(Paragraph(contacto, estilos["texto_pequeno"]))

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


def _tabla_items(items, estilos: dict) -> Table:
    """Tabla de items del documento."""
    hdr_izq = ParagraphStyle("hdr_izq", parent=estilos["texto_pequeno"], textColor=colors.white, alignment=TA_LEFT)
    hdr_cent = ParagraphStyle("hdr_cent", parent=estilos["texto_pequeno"], textColor=colors.white, alignment=TA_CENTER)
    hdr_der = ParagraphStyle("hdr_der", parent=estilos["texto_pequeno"], textColor=colors.white, alignment=TA_RIGHT)

    encabezados = [
        Paragraph("<b>SKU</b>", hdr_izq),
        Paragraph("<b>Descripción</b>", hdr_izq),
        Paragraph("<b>Cant.</b>", hdr_cent),
        Paragraph("<b>P. Unit.</b>", hdr_der),
        Paragraph("<b>Desc.%</b>", hdr_cent),
        Paragraph("<b>Subtotal</b>", hdr_der),
    ]
    data = [encabezados]

    estilo_der = ParagraphStyle("der", parent=estilos["texto_pequeno"], alignment=TA_RIGHT)
    estilo_cent = ParagraphStyle("cen", parent=estilos["texto_pequeno"], alignment=TA_CENTER)

    for it in items:
        sku = it.sku or ("—" if it.es_linea_libre else "")
        desc_pc = float(it.descuento_porc or 0)
        data.append([
            Paragraph(sku or "", estilos["texto_pequeno"]),
            Paragraph(it.descripcion or "", estilos["texto_pequeno"]),
            Paragraph(str(it.cantidad), estilo_cent),
            Paragraph(_fmt_monto(it.precio_unitario), estilo_der),
            Paragraph(f"{desc_pc:.0f}%" if desc_pc else "", estilo_cent),
            Paragraph(_fmt_monto(it.subtotal), estilo_der),
        ])

    t = Table(data, colWidths=[22 * mm, 78 * mm, 15 * mm, 25 * mm, 15 * mm, 25 * mm])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), COLOR_AZUL),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 6),
        ("TOPPADDING", (0, 0), (-1, 0), 6),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F7F7F7")]),
        ("GRID", (0, 0), (-1, -1), 0.25, COLOR_LINEA),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 1), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 1), (-1, -1), 3),
    ]))
    return t


def _bloque_totales(subtotal, descuento_general, total, estilos: dict) -> Table:
    """Caja de totales alineada a la derecha."""
    filas = [[
        Paragraph("Subtotal:", estilos["totales_label"]),
        Paragraph(_fmt_monto(subtotal), estilos["totales_valor"]),
    ]]
    if descuento_general and float(descuento_general) > 0:
        filas.append([
            Paragraph("Descuento general:", estilos["totales_label"]),
            Paragraph(f"- {_fmt_monto(descuento_general)}", estilos["totales_valor"]),
        ])
    filas.append([
        Paragraph("<b>TOTAL</b>", estilos["total_final"]),
        Paragraph(_fmt_monto(total), estilos["total_final"]),
    ])

    t = Table(filas, colWidths=[60 * mm, 35 * mm])
    t.setStyle(TableStyle([
        ("LINEABOVE", (0, -1), (-1, -1), 0.5, COLOR_AZUL),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
    ]))

    caja = Table([["", t]], colWidths=[85 * mm, 95 * mm])
    caja.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
    ]))
    return caja


def _bloque_firma(estilos: dict) -> Table:
    """Líneas de firma para el receptor (solo en remitos)."""
    tabla = Table(
        [[
            Paragraph("_________________________<br/>Firma del receptor", estilos["pie"]),
            Paragraph("_________________________<br/>Aclaración / DNI", estilos["pie"]),
        ]],
        colWidths=[90 * mm, 90 * mm],
    )
    tabla.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING", (0, 0), (-1, -1), 20),
    ]))
    return tabla


# =============================================================
# Generadores principales
# =============================================================

def generate_remito_pdf(remito: Remito) -> bytes:
    """Genera el PDF de un remito y lo devuelve como bytes."""
    estilos = _estilos()
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=A4,
        leftMargin=15 * mm, rightMargin=15 * mm,
        topMargin=15 * mm, bottomMargin=15 * mm,
        title=f"Remito {remito.numero:07d}",
        author="Primi Motors",
    )

    fecha_str = remito.fecha.strftime("%d/%m/%Y") if remito.fecha else "—"

    story = [
        _encabezado(remito.numero, fecha_str, "REMITO", estilos),
        Spacer(1, 6 * mm),
        HRFlowable(width="100%", thickness=0.5, color=COLOR_LINEA, spaceBefore=0, spaceAfter=6),
        _bloque_cliente(remito.cliente, estilos),
    ]

    if remito.condicion_venta or remito.forma_pago:
        bits = []
        if remito.condicion_venta:
            bits.append(f"<b>Condición:</b> {remito.condicion_venta}")
        if remito.forma_pago:
            bits.append(f"<b>Forma de pago:</b> {remito.forma_pago}")
        story.append(Spacer(1, 3 * mm))
        story.append(Paragraph("    ".join(bits), estilos["texto_normal"]))

    story.append(Spacer(1, 6 * mm))
    story.append(_tabla_items(remito.items, estilos))

    story.append(Spacer(1, 4 * mm))
    story.append(_bloque_totales(
        remito.subtotal, remito.descuento_general, remito.total, estilos
    ))

    if remito.observaciones:
        story.append(Spacer(1, 5 * mm))
        story.append(Paragraph("OBSERVACIONES", estilos["etiqueta_seccion"]))
        story.append(Paragraph(remito.observaciones.replace("\n", "<br/>"), estilos["texto_normal"]))

    if remito.estado == "anulado":
        story.append(Spacer(1, 4 * mm))
        story.append(Paragraph(
            f"<b>** REMITO ANULADO **</b>" + (f" — Motivo: {remito.motivo_anulacion}" if remito.motivo_anulacion else ""),
            estilos["etiqueta_seccion"],
        ))

    story.append(Spacer(1, 10 * mm))
    story.append(_bloque_firma(estilos))
    story.append(Spacer(1, 4 * mm))
    story.append(Paragraph(
        "Documento no válido como factura. Emitido por Primi Motors.",
        estilos["pie"],
    ))

    doc.build(story)
    return buf.getvalue()


def generate_nc_pdf(nc: NotaCredito) -> bytes:
    """Genera el PDF de una nota de crédito y lo devuelve como bytes."""
    estilos = _estilos()
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=A4,
        leftMargin=15 * mm, rightMargin=15 * mm,
        topMargin=15 * mm, bottomMargin=15 * mm,
        title=f"Nota de Crédito {nc.numero:07d}",
        author="Primi Motors",
    )

    fecha_str = nc.fecha.strftime("%d/%m/%Y") if nc.fecha else "—"

    story = [
        _encabezado(nc.numero, fecha_str, "NOTA DE CRÉDITO", estilos),
        Spacer(1, 6 * mm),
        HRFlowable(width="100%", thickness=0.5, color=COLOR_LINEA, spaceBefore=0, spaceAfter=6),
        _bloque_cliente(nc.cliente, estilos),
    ]

    # Bloque de motivo + remito de origen
    info_bits = []
    if nc.motivo:
        info_bits.append(f"<b>Motivo:</b> {nc.motivo}")
    if nc.remito_origen is not None:
        info_bits.append(f"<b>Remito de origen:</b> N° {nc.remito_origen.numero:07d}")
    if info_bits:
        story.append(Spacer(1, 3 * mm))
        story.append(Paragraph("    ".join(info_bits), estilos["texto_normal"]))

    story.append(Spacer(1, 6 * mm))
    story.append(_tabla_items(nc.items, estilos))

    story.append(Spacer(1, 4 * mm))
    story.append(_bloque_totales(
        nc.subtotal, nc.descuento_general, nc.total, estilos
    ))

    if nc.observaciones:
        story.append(Spacer(1, 5 * mm))
        story.append(Paragraph("OBSERVACIONES", estilos["etiqueta_seccion"]))
        story.append(Paragraph(nc.observaciones.replace("\n", "<br/>"), estilos["texto_normal"]))

    if nc.estado == "anulada":
        story.append(Spacer(1, 4 * mm))
        story.append(Paragraph(
            f"<b>** NC ANULADA **</b>" + (f" — Motivo: {nc.motivo_anulacion}" if nc.motivo_anulacion else ""),
            estilos["etiqueta_seccion"],
        ))

    story.append(Spacer(1, 10 * mm))
    story.append(Paragraph(
        "Documento de crédito interno. Emitido por Primi Motors.",
        estilos["pie"],
    ))

    doc.build(story)
    return buf.getvalue()
