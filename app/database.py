"""
app/database.py
===============
Conexión a Postgres (Render) vía SQLAlchemy 2.x.

Diseño:
  - `DATABASE_URL` viene como env var desde Render (inyectada por el bloque
    `databases:` de render.yaml). En local podés setearla a mano o dejarla
    vacía (la app arranca igual, solo que sin DB).
  - Usamos el driver sync `psycopg2`. Más simple que async y alcanza
    para el volumen que va a tener Primi Motors.
  - `Base` es la clase declarativa base para los modelos (ver app/models.py
    cuando los armemos).
  - `get_db()` es la dependency de FastAPI: abre sesión por request y la
    cierra en el finally. Las rutas la usan con `Depends(get_db)`.
  - `ping()` hace un SELECT 1 para validar conectividad — lo usa /status.

Detalle: Render entrega la URL como `postgres://` (legacy). SQLAlchemy 2.x
exige `postgresql://`. Lo normalizamos acá una sola vez.
"""

from __future__ import annotations

import os
from typing import Iterator, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker


def _get_db_url() -> str:
    url = (os.environ.get("DATABASE_URL") or "").strip()
    # Compat: Render/Heroku entregan postgres://, SQLAlchemy 2.x pide postgresql://
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return url


DATABASE_URL = _get_db_url()

# Si no hay DB configurada, no explotamos al importar — dejamos engine=None
# y que las rutas que la necesiten se quejen por su cuenta.
engine: Optional[Engine] = None
SessionLocal: Optional[sessionmaker[Session]] = None

if DATABASE_URL:
    engine = create_engine(
        DATABASE_URL,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,   # chequea conexión antes de usarla (evita errores por idle kill)
        pool_recycle=1800,    # recicla conexiones cada 30min
        future=True,
    )
    SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False, future=True)


class Base(DeclarativeBase):
    """Clase base declarativa para todos los modelos."""
    pass


def get_db() -> Iterator[Session]:
    """
    FastAPI dependency. Uso:
        @app.get("/foo")
        def foo(db: Session = Depends(get_db)):
            ...
    """
    if SessionLocal is None:
        raise RuntimeError(
            "DATABASE_URL no configurada. "
            "En Render se inyecta sola desde render.yaml; "
            "en local, poneéla en tu .env o export."
        )
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def ping() -> bool:
    """
    Chequea que la DB esté viva (SELECT 1). Devuelve True/False,
    nunca levanta excepción (lo usa /status).
    """
    if engine is None:
        return False
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


def init_db() -> None:
    """
    Crea todas las tablas registradas en `Base.metadata`.
    Lo llamamos al arrancar la app — idempotente, no rompe si ya existen.
    Cuando el schema se complique, migramos a Alembic.
    """
    if engine is None:
        return
    # Importamos modelos para que se registren en metadata antes del create_all.
    # El import tiene side-effect: las clases que extienden Base se registran
    # en Base.metadata al ser definidas.
    from . import models  # noqa: F401
    Base.metadata.create_all(bind=engine)


def count_tables() -> int:
    """Devuelve cuántas tablas hay en el schema 'public'. Para /status."""
    if engine is None:
        return 0
    try:
        with engine.connect() as conn:
            result = conn.execute(text(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_type = 'BASE TABLE'"
            ))
            return int(result.scalar() or 0)
    except Exception:
        return 0
