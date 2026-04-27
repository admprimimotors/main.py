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

    # Soft delete: en vez de borrar, marcamos activo=false.
    activo: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False, index=True)

    # ----- Vínculo con Mercado Libre -----
    # Estos campos se cargan a través del Excel master (columnas ML_Item_ID,
    # ML_Permalink, ML_Status). El sync real con la API de ML se activa en una
    # fase posterior — por ahora son solo metadata para mostrar en el panel.
    ml_item_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    ml_permalink: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    ml_status: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    # Snapshots de lo que ML reportó la última vez que sincronizamos.
    # Usados para detectar drift (DB local vs ML).
    ml_stock: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    ml_precio: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2), nullable=True)
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

    __table_args__ = (
        # Índice GIN sobre JSONB → permite queries tipo:
        #   WHERE ficha_tecnica @> '{"voltaje": 12}'
        # rápidas en Postgres.
        Index("ix_producto_ficha_tecnica_gin", "ficha_tecnica", postgresql_using="gin"),
    )

    def __repr__(self) -> str:
        return f"<Producto sku={self.sku!r} titulo={self.titulo!r}>"


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

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    producto: Mapped["Producto"] = relationship(back_populates="compatibilidades")
    vehiculo: Mapped["Vehiculo"] = relationship()

    __table_args__ = (
        # Un mismo producto no se duplica para el mismo vehículo
        UniqueConstraint("producto_id", "vehiculo_id", name="uq_producto_vehiculo"),
    )
