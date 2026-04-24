"""
remitos/pdf.py
==============
Generación del PDF del remito con ReportLab.

Usa los datos de `config.EMPRESA` para el letterhead, y `remitos/service.obtener_remito()`
para obtener el contenido a renderizar.

Diseño (A4):
  - Encabezado: logo (si existe) + nombre, dirección, CUIT, email
  - Banda derecha: "REMITO X (no válido como factura)" + número + fecha
  - Datos del cliente: razón social, CUIT, condición IVA, dirección
  - Condición de venta + forma de pago (si vienen)
  - Tabla de items: SKU | Descripción | Cant. | P.Unit. | Desc% | Subtotal
  - Totales a la derecha: Subtotal, Descuento general, TOTAL
  - Observaciones
  - Pie: "Documento no válido como factura" + línea para firma de recepción
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import (
    HRFlowable,
    Image,
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
from remitos import service

log = get_logger(__name__)


# ==========================================================
# Paleta corporativa (se puede afinar a futuro)
# ==========================================================
COLOR_AZUL = colors.HexColor("#1F3864")
COLOR_GRIS = colors.HexColor("#404040")
COLOR_CLARO = colors.HexColor("#EAEAEA")
COLOR_LINEA = colors.HexColor("#B0B0B0")


# ==========================================================
# Helpers de formato
# ==========================================================
def _formatear_monto(valor: float) -> str:
    """29276.84 → '$ 29.276,84' (formato argentino)."""
    entero = int(abs(valor))
    decimal = int(round((abs(valor) - entero) * 100))
    entero_fmt = f"{entero:,}".replace(",", ".")
    signo = "-" if valor < 0 else ""
    return f"{signo}$ {entero_fmt},{decimal:02d}"


def _estilos():
    base = getSampleStyleSheet()
    return {
        "titulo_remito": ParagraphStyle(
            "titulo_remito", parent=base["Title"],
            fontSize=20, leading=24, textColor=COLOR_AZUL, alignment=TA_RIGHT, spaceAfter=2,
        ),
        "subtitulo_remito": ParagraphStyle(
            "subtitulo_remito", parent=base["Normal"],
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
        "numero_remito": ParagraphStyle(
            "numero_remito", parent=base["Normal"],
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


# ==========================================================
# Componentes del PDF
# ==========================================================
def _encabezado(remito, estilos) -> Table:
    """Franja superior: a la izquierda la empresa, a la derecha 'REMITO' + número + fecha."""
    empresa = config.EMPRESA
    logo_path = config.ASSETS_DIR / "logo.png"

    # Columna izquierda: logo + datos de la empresa
    col_izq_items = []
    if logo_path.exists():
        try:
            img = Image(str(logo_path), width=32 * mm, height=32 * mm, kind="proportional")
            col_izq_items.append(img)
            col_izq_items.append(Spacer(1, 2 * mm))
        except Exception as e:
            log.warning(f"No pude cargar logo: {e}")

    col_izq_items += [
        Paragraph(empresa["nombre"], estilos["empresa_nombre"]),
        Paragraph(empresa.get("direccion", ""), estilos["empresa_dato"]),
        Paragraph(empresa.get("provincia_cp", ""), estilos["empresa_dato"]),
        Paragraph(f"CUIT: {empresa.get('cuit', '')}", estilos["empresa_dato"]),
        Paragraph(empresa.get("email", ""), estilos["empresa_dato"]),
    ]

    # Columna derecha: "REMITO" + número + fecha
    col_der_items = [
        Paragraph("REMITO", estilos["titulo_remito"]),
        Paragraph("Documento no válido como factura", estilos["subtitulo_remito"]),
        Paragraph(f"N° {remito.numero_formateado}", estilos["numero_remito"]),
        Paragraph(f"Fecha: {remito.fecha}", estilos["texto_normal"]),
    ]

    tabla = Table(
        [[col_izq_items, col_der_items]],
        colWidths=[110 * mm, 70 * mm],
    )
    tabla.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
    ]))
    return tabla


def _bloque_cliente(remito, estilos):
    """Datos del cliente en una caja con fondo gris claro."""
    cliente = clientes_repo.obtener(remito.cliente_id)
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

    direccion = cliente.direccion_completa()

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


def _bloque_condiciones(remito, estilos):
    """Chip de condición de venta y forma de pago (solo si fueron provistos)."""
    if not (remito.condicion_venta or remito.forma_pago):
        return None
    bits = []
    if remito.condicion_venta:
        bits.append(f"<b>Condición:</b> {remito.condicion_venta}")
    if remito.forma_pago:
        bits.append(f"<b>Forma de pago:</b> {remito.forma_pago}")
    return Paragraph("    ".join(bits), estilos["texto_normal"])


def _tabla_items(remito, estilos) -> Table:
    """Tabla de items."""
    # Estilos de encabezado en blanco (para contraste con el fondo azul)
    hdr_izq = ParagraphStyle(
        "hdr_izq", parent=estilos["texto_pequeno"],
        textColor=colors.white, alignment=TA_LEFT,
    )
    hdr_cent = ParagraphStyle(
        "hdr_cent", parent=estilos["texto_pequeno"],
        textColor=colors.white, alignment=TA_CENTER,
    )
    hdr_der = ParagraphStyle(
        "hdr_der", parent=estilos["texto_pequeno"],
        textColor=colors.white, alignment=TA_RIGHT,
    )

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

    for it in remito.items:
        sku = it.get("sku") or ("—" if it.get("es_linea_libre") else "")
        data.append([
            Paragraph(sku or "", estilos["texto_pequeno"]),
            Paragraph(it["descripcion"] or "", estilos["texto_pequeno"]),
            Paragraph(str(it["cantidad"]), estilo_cent),
            Paragraph(_formatear_monto(float(it["precio_unitario"] or 0)), estilo_der),
            Paragraph(f"{float(it['descuento_porc'] or 0):.0f}%" if it["descuento_porc"] else "", estilo_cent),
            Paragraph(_formatear_monto(float(it["subtotal"] or 0)), estilo_der),
        ])

    t = Table(data, colWidths=[22 * mm, 78 * mm, 15 * mm, 25 * mm, 15 * mm, 25 * mm])
    t.setStyle(TableStyle([
        # Header
        ("BACKGROUND", (0, 0), (-1, 0), COLOR_AZUL),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("ALIGN", (2, 0), (2, 0), "CENTER"),
        ("ALIGN", (4, 0), (4, 0), "CENTER"),
        ("ALIGN", (3, 0), (3, 0), "RIGHT"),
        ("ALIGN", (5, 0), (5, 0), "RIGHT"),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 6),
        ("TOPPADDING", (0, 0), (-1, 0), 6),
        # Body
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F7F7F7")]),
        ("GRID", (0, 0), (-1, -1), 0.25, COLOR_LINEA),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 1), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 1), (-1, -1), 3),
    ]))
    return t


def _bloque_totales(remito, estilos) -> Table:
    """Caja de totales alineada a la derecha."""
    subtotal_fmt = _formatear_monto(remito.subtotal)
    descuento_fmt = _formatear_monto(remito.descuento_general)
    total_fmt = _formatear_monto(remito.total)

    filas = [
        [Paragraph("Subtotal:", estilos["totales_label"]),
         Paragraph(subtotal_fmt, estilos["totales_valor"])],
    ]
    if remito.descuento_general and remito.descuento_general > 0:
        filas.append([
            Paragraph("Descuento general:", estilos["totales_label"]),
            Paragraph(f"- {descuento_fmt}", estilos["totales_valor"]),
        ])
    filas.append([
        Paragraph("<b>TOTAL</b>", estilos["total_final"]),
        Paragraph(total_fmt, estilos["total_final"]),
    ])

    t = Table(filas, colWidths=[60 * mm, 35 * mm])
    t.setStyle(TableStyle([
        ("LINEABOVE", (0, -1), (-1, -1), 0.5, COLOR_AZUL),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
    ]))

    # Cajonera: total box alineada a la derecha
    caja = Table([["", t]], colWidths=[85 * mm, 95 * mm])
    caja.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
    ]))
    return caja


def _bloque_firma(estilos) -> Table:
    """Dos líneas para firma y aclaración del receptor."""
    tabla = Table(
        [
            [
                Paragraph("_________________________<br/>Firma del receptor", estilos["pie"]),
                Paragraph("_________________________<br/>Aclaración / DNI", estilos["pie"]),
            ]
        ],
        colWidths=[90 * mm, 90 * mm],
    )
    tabla.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING", (0, 0), (-1, -1), 20),
    ]))
    return tabla


# ==========================================================
# Generación principal
# ==========================================================
def generar_pdf(remito_id: int, destino: Optional[Path] = None) -> Path:
    """
    Genera el PDF del remito y devuelve el path.

    Si no se pasa destino, lo guarda en data/remitos/<numero>.pdf
    y registra esa ruta en la base (remitos.pdf_path).
    """
    remito = service.obtener_remito(remito_id)
    if remito is None:
        raise ValueError(f"Remito id={remito_id} no existe.")

    if destino is None:
        carpeta = config.DATA_DIR / "remitos"
        carpeta.mkdir(parents=True, exist_ok=True)
        destino = carpeta / f"{remito.numero_formateado}.pdf"

    destino = Path(destino)
    destino.parent.mkdir(parents=True, exist_ok=True)

    estilos = _estilos()

    doc = SimpleDocTemplate(
        str(destino),
        pagesize=A4,
        leftMargin=15 * mm, rightMargin=15 * mm,
        topMargin=15 * mm, bottomMargin=15 * mm,
        title=f"Remito {remito.numero_formateado}",
        author="Primi Motors",
    )

    story = []
    story.append(_encabezado(remito, estilos))
    story.append(Spacer(1, 6 * mm))
    story.append(HRFlowable(width="100%", thickness=0.5, color=COLOR_LINEA, spaceBefore=0, spaceAfter=6))

    story.append(_bloque_cliente(remito, estilos))

    condiciones = _bloque_condiciones(remito, estilos)
    if condiciones is not None:
        story.append(Spacer(1, 3 * mm))
        story.append(condiciones)

    story.append(Spacer(1, 6 * mm))
    story.append(_tabla_items(remito, estilos))

    story.append(Spacer(1, 4 * mm))
    story.append(_bloque_totales(remito, estilos))

    if remito.observaciones:
        story.append(Spacer(1, 5 * mm))
        story.append(Paragraph("OBSERVACIONES", estilos["etiqueta_seccion"]))
        story.append(Paragraph(remito.observaciones.replace("\n", "<br/>"), estilos["texto_normal"]))

    story.append(Spacer(1, 10 * mm))
    story.append(Paragraph(
        "Documento no válido como factura. Emitido por Primi Motors.",
        estilos["pie"],
    ))

    doc.build(story)
    service.guardar_pdf_path(remito_id, str(destino))
    log.info(f"PDF generado: {destino}")
    return destino
