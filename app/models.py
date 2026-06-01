"""
app/models.py
=============
Modelos de SQLAlchemy 2.x para Primi Motors.

Tablas:
  - productos                 → catálogo central (SKU, título, ficha, precios, stock)
  - fotos_producto            → 1:N con productos (URLs en R2)
  - vehiculos                 → definición de vehículo + motor
  - producto_compatibilidades → M:N entre productos y vehículos

Decisiones de diseño:

  * `ficha_tecnica` como JSONB.
    Cada categoría de producto tiene campos distintos (un aro: diámetro,
    espesor; un starter: voltaje, dientes, rotación; un alternador: amperaje,
    voltaje, polea). Modelar cada categoría como tabla separada explota a 30+
    tablas. Modelarlo como columnas opcionales en `productos` ensucia la tabla
    con 80 columnas casi siempre nulas. JSONB en Postgres nos da: flexibilidad
    total, indexable con GIN, queriable con operadores `@>` y `->>`, y sin
    migraciones cada vez que sumamos un atributo.

  * SKU único pero NO es la PK.
    PK es un autoincrement integer (joins más rápidos, menos pesado en FKs).
    SKU es UNIQUE INDEX, lo seguimos usando como identificador de negocio.

  * Fotos en tabla separada (no como columnas foto_1_url, foto_2_url).
    Algunos productos tendrán 1 foto, otros 5. Tabla separada con `orden`
    da flexibilidad sin schema migrations.

  * Storage de fotos: R2 (Cloudflare). Solo guardamos `storage_key` y `url`.
    El archivo binario vive en R2, NO en Postgres. A 50K productos × 3 fotos
    son ~45 GB — Postgres se moriría, R2 cuesta ~$0.50/mes.

  * Soft delete con `activo: bool`.
    En lugar de borrar, marcamos `activo=false`. Útil porque ML guarda el ID
    del item y queremos preservar el histórico.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .database import Base


# =============================================================
# Producto (catálogo central)
# =============================================================

class Producto(Base):
    __tablename__ = "productos"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Identidad de negocio
    sku: Mapped[str] = mapped_column(String(64), unique=True, index=True, nullable=False)
    # SKU que se manda a Mercado Libre como SELLER_SKU. Puede repetirse entre
    # productos: es el caso típico de "publicar 2 versiones del mismo código de
    # proveedor con distintas configuraciones". Si está vacío, se usa `sku`
    # como fallback al publicar/pushear.
    sku_ml: Mapped[Optional[str]] = mapped_column(
        String(64), nullable=True, index=True
    )
    titulo: Mapped[str] = mapped_column(String(500), nullable=False)
    descripcion: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Clasificación (para filtros y búsqueda)
    categoria: Mapped[Optional[str]] = mapped_column(String(80), nullable=True, index=True)
    marca: Mapped[Optional[str]] = mapped_column(String(80), nullable=True, index=True)

    # Ficha técnica — estructura específica por categoría.
    # Ejemplos:
    #   aro:       {"diametro_mm": 75, "espesor_mm": 1.5, "material": "acero"}
    #   starter:   {"voltaje": 12, "potencia_kw": 1.4, "dientes": 9, "rotacion": "horario"}
    #   pistón:    {"diametro_mm": 82.5, "compresion_mm": 38, "perno_mm": 22}
    ficha_tecnica: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    # Precios — Numeric (no float) para evitar errores de redondeo en plata.
    precio_costo: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2), nullable=True)
    precio_final: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2), nullable=True)
    moneda: Mapped[str] = mapped_column(String(3), default="ARS", nullable=False)

    # Stock disponible
    stock_actual: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    # Timestamp del último write sobre stock_actual (cualquier path: upload masivo,
    # ajuste manual desde la vista detalle, descuento por venta ML, remito, NC).
    # Sirve para la lista "últimos modificados" sin confundirla con `updated_at`
    # que también cambia ante edits de precio, título, ficha, etc.
    stock_updated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True, index=True
    )

    # Días de disponibilidad / tiempo de fabricación que se publica en ML.
    # Si está seteado, ML muestra "Disponible en X días después de tu compra"
    # en lugar de "Llega gratis mañana". Se mapea al sale_term MANUFACTURING_TIME.
    # Se carga desde el Excel master con la columna `dias_disponibilidad`.
    # NULL = no se manda nada a ML (default = "Llega mañana" si hay stock).
    dias_disponibilidad: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Soft delete: en vez de borrar, marcamos activo=false.
    activo: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False, index=True)

    # ----- Vínculo con Mercado Libre -----
    # Estos campos se cargan a través del Excel master (columnas ML_Item_ID,
    # ML_Permalink, ML_Status). El sync real con la API de ML se activa en una
    # fase posterior — por ahora son solo metadata para mostrar en el panel.
    ml_item_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    # Si este producto es una variante dentro de una publicación matriz de ML,
    # ml_variation_id guarda el ID de su variation específica. NULL = producto
    # publicado como ítem simple (sin variantes) o todavía no publicado.
    # Combinación (ml_item_id, ml_variation_id) identifica unívocamente la variante.
    ml_variation_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    # ID de categoría ML para usar al publicar este producto (override del
    # auto-predict). Si está seteado, lo usamos directamente y no llamamos al
    # predictor de ML. Se puede cargar desde el Excel con la columna
    # `ML_Category_ID` / `categoria_id_ml`.
    ml_category_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    ml_permalink: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    ml_status: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    # Snapshots de lo que ML reportó la última vez que sincronizamos.
    # Usados para detectar drift (DB local vs ML).
    ml_stock: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    ml_precio: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2), nullable=True)
    ml_last_synced_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    # Costos variables por producto que afectan la rentabilidad real en ML.
    # Si están en NULL, se usa el default global del módulo Precios.
    ml_envio_fijo: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2), nullable=True)
    ml_impuestos_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 2), nullable=True)
    # Comisión real ML para esta publicación (varía por categoría + tipo).
    # Se autocompleta al sincronizar con ML via /sites/MLA/listing_prices.
    ml_comision_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 2), nullable=True)
    # Atributos crudos como ML los entrega (con value_id, value_struct, etc.).
    # Necesarios para PUSH: nos dan los IDs originales para mantener integridad
    # cuando enviamos cambios de la ficha técnica de vuelta a ML.
    ml_raw_attributes: Mapped[Optional[list]] = mapped_column(JSONB, nullable=True)

    # Auditoría
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    # Relaciones
    fotos: Mapped[list["FotoProducto"]] = relationship(
        back_populates="producto",
        cascade="all, delete-orphan",
        order_by="FotoProducto.orden",
    )
    compatibilidades: Mapped[list["ProductoCompatibilidad"]] = relationship(
        back_populates="producto",
        cascade="all, delete-orphan",
    )
    publicaciones_ml: Mapped[list["ProductoPublicacionML"]] = relationship(
        back_populates="producto",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        # Índice GIN sobre JSONB → permite queries tipo:
        #   WHERE ficha_tecnica @> '{"voltaje": 12}'
        # rápidas en Postgres.
        Index("ix_producto_ficha_tecnica_gin", "ficha_tecnica", postgresql_using="gin"),
    )

    def __repr__(self) -> str:
        return f"<Producto sku={self.sku!r} titulo={self.titulo!r}>"


# =============================================================
# Publicaciones ML (1 producto → N publicaciones)
# =============================================================
#
# Antes el modelo era 1:1 — Producto.ml_item_id, ml_status, ml_stock, etc.
# vivían directo en la fila de productos. Eso limita: no podés tener la misma
# pieza publicada como FULL y tradicional, ni en dos categorías, ni con dos
# títulos para A/B testear.
#
# Ahora cada Producto puede tener N publicaciones en ML, cada una con su
# propia identidad (ml_item_id), categoría, listing_type, shipping_mode,
# título, precio y stock asignado. En F1 el stock se considera compartido
# (push el del producto a todas las publicaciones); F2 va a usar el
# `ml_stock_asignado` por publicación para asignación manual.
#
# Compat: durante la transición seguimos manteniendo Producto.ml_item_id /
# ml_status / ml_stock / ml_precio como "cache de la primera publicación"
# para no romper queries legacy. La tabla nueva es la fuente de verdad.

class ProductoPublicacionML(Base):
    __tablename__ = "producto_publicaciones_ml"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    producto_id: Mapped[int] = mapped_column(
        ForeignKey("productos.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    # ID de ML — único en toda la base. NULL solo durante la creación, después
    # se llena con el id devuelto por POST /items.
    ml_item_id: Mapped[str] = mapped_column(
        String(64), nullable=False, unique=True, index=True
    )
    # ID de variation dentro de la publicación (solo para items con variations[]).
    ml_variation_id: Mapped[Optional[str]] = mapped_column(
        String(64), nullable=True, index=True
    )
    ml_permalink: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    # Status reportado por ML (active, paused, closed, under_review, etc.).
    ml_status: Mapped[Optional[str]] = mapped_column(String(20), nullable=True, index=True)
    # Categoría con la que se publicó (puede diferir entre publicaciones del
    # mismo producto — caso "publicado en 2 categorías").
    ml_category_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    # listing_type_id: "gold_special" (clásica), "gold_pro" (premium), "free".
    # Define costo de comisión y duración.
    ml_listing_type: Mapped[Optional[str]] = mapped_column(String(40), nullable=True)
    # shipping_mode: "me2" (MercadoEnvíos), "custom" (a coordinar), "not_specified".
    # `me2` + `tags=[fulfillment]` = FULL.
    ml_shipping_mode: Mapped[Optional[str]] = mapped_column(String(40), nullable=True)
    # True si la publicación está linkeada al catálogo de ML (sale en la
    # ficha técnica oficial del producto).
    ml_catalog_listing: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    # Título de esta publicación específicamente. Puede ser distinto del
    # `producto.titulo` (caso A/B test de SEO). NULL → usa el del producto.
    ml_titulo: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    # Precio de esta publicación. Puede diferir del producto.precio_final.
    ml_precio: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2), nullable=True)
    # Stock asignado a esta publicación (F2). En F1 ignorado — push el del producto.
    # NULL = "no asignación manual, se reparte el del producto".
    ml_stock_asignado: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    # Stock que ML reporta actualmente (snapshot para detectar drift).
    ml_stock_snapshot: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    # Atributos crudos como ML los entrega (con value_id, value_struct, etc.).
    # Necesarios para push correcto.
    ml_raw_attributes: Mapped[Optional[list]] = mapped_column(JSONB, nullable=True)
    ml_last_synced_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Auditoría
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    producto: Mapped["Producto"] = relationship(back_populates="publicaciones_ml")

    __table_args__ = (
        Index("ix_ppml_producto_status", "producto_id", "ml_status"),
    )

    def __repr__(self) -> str:
        return f"<PublicacionML {self.ml_item_id} producto_id={self.producto_id}>"


# =============================================================
# Fotos del producto (1:N, blobs en R2)
# =============================================================

class FotoProducto(Base):
    __tablename__ = "fotos_producto"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    producto_id: Mapped[int] = mapped_column(
        ForeignKey("productos.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # Path dentro del bucket R2 (ej: "productos/0001-AROS-FORD/foto-01.jpg")
    storage_key: Mapped[str] = mapped_column(String(512), nullable=False)
    # URL pública — derivada de storage_key + dominio del bucket
    url: Mapped[str] = mapped_column(String(1024), nullable=False)

    # Orden en que se muestran (0 = primera, foto principal)
    orden: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    # Metadata del archivo (útil para debug, no obligatoria)
    bytes_size: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    width_px: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    height_px: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    producto: Mapped["Producto"] = relationship(back_populates="fotos")


# =============================================================
# Vehículo (definición + motor)
# =============================================================

class Vehiculo(Base):
    __tablename__ = "vehiculos"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Identidad
    marca: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    modelo: Mapped[str] = mapped_column(String(120), nullable=False, index=True)

    # Motor — todos opcionales porque hay compatibilidades genéricas
    # (ej: "cualquier Falcon", sin importar motor)
    combustible: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)  # nafta/diesel/gnc/electrico
    cilindros: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    valvulas: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    cilindrada_cc: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Rango de años — un mismo vehículo puede cubrir varios años
    # (ej: Ford Falcon 1969-1985 con el mismo motor)
    anio_desde: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, index=True)
    anio_hasta: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, index=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    __table_args__ = (
        # Índice compuesto para búsquedas tipo "qué fits Ford Falcon 1980"
        Index(
            "ix_vehiculo_busqueda",
            "marca", "modelo", "anio_desde", "anio_hasta",
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<Vehiculo {self.marca} {self.modelo} "
            f"{self.anio_desde or '?'}-{self.anio_hasta or '?'}>"
        )


# =============================================================
# Compatibilidad producto ↔ vehículo (M:N)
# =============================================================

class Cliente(Base):
    """
    Cliente del negocio. Schema compatible con el sistema viejo de Primi Motors
    (mismos campos que el dataclass de `clientes/repo.py`) para que la migración
    desde el SQLite local sea directa.
    """
    __tablename__ = "clientes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Identidad
    razon_social: Mapped[str] = mapped_column(String(200), nullable=False, index=True)
    nombre_comercial: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)

    # Tributario
    cuit_dni: Mapped[Optional[str]] = mapped_column(String(20), nullable=True, index=True)
    condicion_iva: Mapped[Optional[str]] = mapped_column(String(40), nullable=True, index=True)

    # Contacto
    telefono: Mapped[Optional[str]] = mapped_column(String(40), nullable=True)
    email: Mapped[Optional[str]] = mapped_column(String(120), nullable=True, index=True)

    # Dirección
    direccion: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    localidad: Mapped[Optional[str]] = mapped_column(String(100), nullable=True, index=True)
    provincia: Mapped[Optional[str]] = mapped_column(String(100), nullable=True, index=True)
    codigo_postal: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)

    # Otros
    notas: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    activo: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False, index=True)

    # Auditoría
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    def __repr__(self) -> str:
        return f"<Cliente id={self.id} razon_social={self.razon_social!r}>"


class Remito(Base):
    """
    Remito de venta. Documento que acompaña la entrega de mercadería al cliente.
    Al crearlo, se descuenta automáticamente el stock de los productos del catálogo
    que estén linkeados (los items "línea libre" no afectan stock).
    Al anularlo, el stock se reincorpora.
    """
    __tablename__ = "remitos"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    # Número correlativo legible (ej: 1494). Único, asignado al crear.
    numero: Mapped[int] = mapped_column(Integer, unique=True, nullable=False, index=True)

    cliente_id: Mapped[int] = mapped_column(
        ForeignKey("clientes.id", ondelete="RESTRICT"), nullable=False, index=True
    )

    fecha: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    condicion_venta: Mapped[Optional[str]] = mapped_column(String(40), nullable=True)
    forma_pago: Mapped[Optional[str]] = mapped_column(String(40), nullable=True)

    # Totales en pesos
    subtotal: Mapped[Decimal] = mapped_column(Numeric(12, 2), default=Decimal("0"), nullable=False)
    descuento_general: Mapped[Decimal] = mapped_column(Numeric(12, 2), default=Decimal("0"), nullable=False)
    total: Mapped[Decimal] = mapped_column(Numeric(12, 2), default=Decimal("0"), nullable=False)

    observaciones: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    estado: Mapped[str] = mapped_column(String(20), default="emitido", nullable=False, index=True)
    fecha_anulacion: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    motivo_anulacion: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    items: Mapped[list["RemitoItem"]] = relationship(
        back_populates="remito",
        cascade="all, delete-orphan",
        order_by="RemitoItem.orden",
    )
    cliente: Mapped["Cliente"] = relationship()

    def __repr__(self) -> str:
        return f"<Remito numero={self.numero} cliente_id={self.cliente_id} total={self.total}>"


class RemitoItem(Base):
    __tablename__ = "remito_items"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    remito_id: Mapped[int] = mapped_column(
        ForeignKey("remitos.id", ondelete="CASCADE"), nullable=False, index=True
    )

    # Si producto_id es NULL, es una línea libre (producto no del catálogo).
    producto_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("productos.id", ondelete="SET NULL"), nullable=True, index=True
    )
    # Snapshot de SKU + descripción al momento del remito (sobrevive si se borra
    # el producto del catálogo después).
    sku: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    descripcion: Mapped[str] = mapped_column(String(500), nullable=False)

    cantidad: Mapped[int] = mapped_column(Integer, nullable=False)
    precio_unitario: Mapped[Decimal] = mapped_column(Numeric(12, 2), default=Decimal("0"), nullable=False)
    descuento_porc: Mapped[Decimal] = mapped_column(Numeric(5, 2), default=Decimal("0"), nullable=False)
    subtotal: Mapped[Decimal] = mapped_column(Numeric(12, 2), default=Decimal("0"), nullable=False)

    orden: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    es_linea_libre: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    remito: Mapped["Remito"] = relationship(back_populates="items")


class NotaCredito(Base):
    """
    Nota de crédito. Documento que acredita un saldo a favor del cliente
    (devolución, bonificación, error de facturación, etc.).
    Al crearla, el stock de los productos del catálogo se SUMA
    (se reincorporan unidades). Al anularla, se resta de nuevo.

    Puede o no estar linkeada a un Remito de origen (algunas NC son
    sin remito previo, ej. ajustes).
    """
    __tablename__ = "notas_credito"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    numero: Mapped[int] = mapped_column(Integer, unique=True, nullable=False, index=True)

    cliente_id: Mapped[int] = mapped_column(
        ForeignKey("clientes.id", ondelete="RESTRICT"), nullable=False, index=True
    )

    fecha: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    motivo: Mapped[Optional[str]] = mapped_column(String(80), nullable=True)
    # Remito de origen opcional (algunas NC se emiten sin remito previo)
    remito_origen_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("remitos.id", ondelete="SET NULL"), nullable=True, index=True
    )

    subtotal: Mapped[Decimal] = mapped_column(Numeric(12, 2), default=Decimal("0"), nullable=False)
    descuento_general: Mapped[Decimal] = mapped_column(Numeric(12, 2), default=Decimal("0"), nullable=False)
    total: Mapped[Decimal] = mapped_column(Numeric(12, 2), default=Decimal("0"), nullable=False)

    observaciones: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    estado: Mapped[str] = mapped_column(String(20), default="emitida", nullable=False, index=True)
    fecha_anulacion: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    motivo_anulacion: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    items: Mapped[list["NotaCreditoItem"]] = relationship(
        back_populates="nota_credito",
        cascade="all, delete-orphan",
        order_by="NotaCreditoItem.orden",
    )
    cliente: Mapped["Cliente"] = relationship()
    remito_origen: Mapped[Optional["Remito"]] = relationship(foreign_keys=[remito_origen_id])


class NotaCreditoItem(Base):
    __tablename__ = "nota_credito_items"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    nc_id: Mapped[int] = mapped_column(
        ForeignKey("notas_credito.id", ondelete="CASCADE"), nullable=False, index=True
    )

    producto_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("productos.id", ondelete="SET NULL"), nullable=True, index=True
    )
    sku: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    descripcion: Mapped[str] = mapped_column(String(500), nullable=False)

    cantidad: Mapped[int] = mapped_column(Integer, nullable=False)
    precio_unitario: Mapped[Decimal] = mapped_column(Numeric(12, 2), default=Decimal("0"), nullable=False)
    descuento_porc: Mapped[Decimal] = mapped_column(Numeric(5, 2), default=Decimal("0"), nullable=False)
    subtotal: Mapped[Decimal] = mapped_column(Numeric(12, 2), default=Decimal("0"), nullable=False)

    orden: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    es_linea_libre: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    nota_credito: Mapped["NotaCredito"] = relationship(back_populates="items")


class MLToken(Base):
    """
    Singleton (id=1) — guarda el refresh_token de Mercado Libre.

    ML rota el refresh_token en cada llamada a /oauth/token. Si lo dejamos solo
    en env vars, después del primer refresh queda obsoleto y la app pierde acceso.
    Acá lo persistimos: en cada refresh, sobreescribimos el row con el nuevo
    refresh_token. La env var ML_REFRESH_TOKEN sirve solo de bootstrap inicial.
    """
    __tablename__ = "ml_tokens"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    refresh_token: Mapped[str] = mapped_column(String(512), nullable=False)
    last_refreshed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )


class CategoriaMLMapping(Base):
    """
    Cache nuestra-categoría → ML category_id.

    Cuando publicamos por primera vez un producto de "filtros_aceite", el
    predictor de ML sugiere una categoría (ej MLA1234). El usuario confirma y
    queda guardado acá. La próxima vez que publiquemos un producto con
    `categoria='filtros_aceite'`, vamos directo a esa MLA1234 sin volver a
    pedir confirmación.

    El usuario puede sobrescribir el mapeo en cualquier momento desde la UI.
    """
    __tablename__ = "categoria_ml_mapping"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    nuestra_categoria: Mapped[str] = mapped_column(
        String(120), unique=True, nullable=False, index=True
    )
    ml_category_id: Mapped[str] = mapped_column(String(64), nullable=False)
    ml_category_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    # Si el mapeo lo confirmó manualmente el usuario (vs ser solo el último predicho).
    confirmado: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class MLOrder(Base):
    """
    Orden de venta sincronizada desde Mercado Libre.

    Una orden ML puede contener múltiples items, pero modelamos UN MLOrder POR
    ITEM dentro de la orden (con order_id + item_id como clave compuesta) para
    poder hacer joins limpios con productos vía ml_item_id.

    Cada row representa una "linea de venta" que afecta el stock de un SKU.

    Lifecycle:
      - status = "paid" (o "confirmed") → stock_applied=cantidad, decrementa producto.
      - status = "cancelled" o "refunded" → revertimos stock_applied a 0 y re-incrementamos.

    `last_status_at` permite detectar cambios (cancelaciones después de pagadas).
    """
    __tablename__ = "ml_orders"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    # Order ID + Item ID de ML — clave de negocio única
    ml_order_id: Mapped[str] = mapped_column(String(40), nullable=False, index=True)
    ml_item_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    # Vínculo con el producto local (puede ser NULL si no encontramos match al
    # momento de la sync — caso raro, se completa en re-sync)
    producto_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("productos.id", ondelete="SET NULL"),
        nullable=True, index=True,
    )
    # Snapshot del SKU al momento de la venta (estable aunque el producto se borre)
    sku_snapshot: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    titulo_snapshot: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)

    cantidad: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    precio_unitario: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2), nullable=True)
    total_amount: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 2), nullable=True)
    moneda: Mapped[str] = mapped_column(String(3), default="ARS", nullable=False)

    # Status de la orden en ML: "paid", "confirmed", "cancelled", "invalid",
    # "partially_paid", "partially_refunded", "refunded"
    status: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    # Variation ID si la orden corresponde a una variante específica
    ml_variation_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    # Comprador (snapshot)
    buyer_nickname: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)

    # Fechas (todas de ML)
    date_created: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )
    date_closed: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_status_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Cuánto stock aplicamos efectivamente (=cantidad cuando paid, 0 cuando cancelled).
    # Permite calcular delta y revertir cuando una orden cambia de estado.
    stock_applied: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    # Auditoría local
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(), onupdate=func.now(), nullable=False
    )

    __table_args__ = (
        # Una row por (order, item) — si la misma orden tiene 2 items distintos,
        # son 2 rows; si la misma orden viene 2 veces de ML, se actualiza la misma.
        UniqueConstraint("ml_order_id", "ml_item_id", name="uq_ml_order_item"),
        Index("ix_ml_orders_date_status", "date_created", "status"),
    )


class ProductoCompatibilidad(Base):
    __tablename__ = "producto_compatibilidades"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    producto_id: Mapped[int] = mapped_column(
        ForeignKey("productos.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    vehiculo_id: Mapped[int] = mapped_column(
        ForeignKey("vehiculos.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # Notas opcionales sobre la compatibilidad
    # (ej: "lado izquierdo", "solo para versión naftera")
    notas: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)

    # ID de la compatibilidad en ML (cuando viene sincronizada desde ML).
    # Permite round-trip: si la borrás localmente, sabemos qué borrar de ML.
    ml_compat_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    producto: Mapped["Producto"] = relationship(back_populates="compatibilidades")
    vehiculo: Mapped["Vehiculo"] = relationship()

    __table_args__ = (
        # Un mismo producto no se duplica para el mismo vehículo
        UniqueConstraint("producto_id", "vehiculo_id", name="uq_producto_vehiculo"),
    )


# =============================================================
# MLPriceSnapshot — snapshots de precios ML para construir histórico
# =============================================================

class MLPriceSnapshot(Base):
    """
    Snapshot de precio de una publicación ML en un momento dado.
    Como ML no expone histórico vía API, construimos el nuestro
    capturando snapshots periódicos (auto-sync diario desde el dashboard).
    """
    __tablename__ = "ml_price_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    ml_item_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    title: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    sku: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)

    # Datos de precio
    price: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    base_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2), nullable=True)
    original_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2), nullable=True)
    currency: Mapped[Optional[str]] = mapped_column(String(8), nullable=True)

    # Metadata de la publicación al momento del snapshot
    status: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    available_quantity: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    sold_quantity: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Flag: True si el precio cambió respecto al snapshot anterior del mismo item.
    # Permite filtrar fácilmente solo los cambios reales (sin ruido de snapshots iguales).
    is_change: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False, index=True)

    # Origen del snapshot. Permite distinguir entre:
    #   "api"            → captura automática vía /items/{id} (snapshot regular)
    #   "sale_backfill"  → reconstruido desde MLOrder.precio_unitario (backfill histórico)
    #   "manual"         → forzado por usuario desde el botón "Capturar ahora"
    source: Mapped[str] = mapped_column(String(20), default="api", nullable=False, index=True)

    captured_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False, index=True
    )

    __table_args__ = (
        Index("ix_mlpricesnap_item_capt", "ml_item_id", "captured_at"),
    )


# =============================================================
# PrecioCambioLog — audit log de cambios de precio del sistema
# =============================================================

class PrecioCambioLog(Base):
    """
    Audit log de cambios de precio efectuados DESDE el sistema (no detectados
    desde ML). Cada fila es un evento de "alguien cambió el precio_final de un
    producto" o "alguien pusheó precio a ML desde acá".

    Diferencia con MLPriceSnapshot:
      - MLPriceSnapshot captura el estado del precio de ML en un momento dado
        (lectura).
      - PrecioCambioLog registra la INTENCIÓN del sistema de cambiar el precio
        (escritura) — sirve para saber "yo (Primi) cambié esto cuándo y por qué".

    Las dos fuentes juntas en /precios-historial dan trazabilidad completa.
    """
    __tablename__ = "precio_cambios_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    producto_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("productos.id", ondelete="SET NULL"), nullable=True, index=True
    )
    sku: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    ml_item_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    titulo_snapshot: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)

    # Precios. precio_anterior puede ser NULL (primera vez que se setea).
    precio_anterior: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2), nullable=True)
    precio_nuevo: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)

    # Fuente del cambio. Valores:
    #   "sistema_bulk"      → bulk update vía /precios apply
    #   "sistema_individual" → edit individual vía /catalogo/{sku} edit
    #   "ml_push"           → push manual a ML (push_to_ml con push_price=True)
    #   "ml_sync_in"        → detectado al sincronizar desde ML (sync_producto_from_ml)
    fonte: Mapped[str] = mapped_column(String(40), nullable=False, index=True)

    # Origen humano-legible (endpoint, operación, etc.) para debug.
    # Ej: "precios_bulk_apply", "catalogo_edit", "push_to_ml_manual".
    origen: Mapped[Optional[str]] = mapped_column(String(80), nullable=True)

    # Usuario que disparó el cambio (admin del panel). Para multi-user a futuro.
    usuario: Mapped[Optional[str]] = mapped_column(String(80), nullable=True)

    # Nota libre. Para bulk: "redondeo a 9", "ajuste -15%", etc.
    nota: Mapped[Optional[str]] = mapped_column(String(300), nullable=True)

    # Si el cambio se pusheó a ML (true) o quedó solo en DB local (false).
    pushed_to_ml: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False, index=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False, index=True
    )

    __table_args__ = (
        Index("ix_preccambiolog_sku_created", "sku", "created_at"),
        Index("ix_preccambiolog_fonte_created", "fonte", "created_at"),
    )
