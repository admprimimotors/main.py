"""
config.py
=========
Carga centralizada de variables de entorno y constantes del sistema.
Todas las credenciales y configuraciones sensibles viven en .env
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

# Cargar variables de entorno desde .env
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")


def _require(name: str) -> str:
    """Devuelve el valor de una variable de entorno obligatoria o lanza error."""
    value = os.getenv(name)
    if not value:
        raise RuntimeError(
            f"Falta la variable de entorno '{name}'. "
            f"Revisá tu archivo .env y asegurate de que esté configurada."
        )
    return value


# ==========================================================
# Credenciales Mercado Libre
# ==========================================================
ML_CLIENT_ID: str = _require("ML_CLIENT_ID")
ML_CLIENT_SECRET: str = _require("ML_CLIENT_SECRET")
ML_REDIRECT_URI: str = os.getenv("ML_REDIRECT_URI", "https://www.google.com")
ML_SITE_ID: str = os.getenv("ML_SITE_ID", "MLA")

# Endpoints oficiales de Mercado Libre
ML_AUTH_BASE_URL: str = f"https://auth.mercadolibre.com.{'ar' if ML_SITE_ID == 'MLA' else 'com'}"
ML_AUTH_URL: str = f"{ML_AUTH_BASE_URL}/authorization"
ML_TOKEN_URL: str = "https://api.mercadolibre.com/oauth/token"
ML_API_BASE: str = "https://api.mercadolibre.com"


# ==========================================================
# Rutas del proyecto
# ==========================================================
DATA_DIR: Path = BASE_DIR / "data"
LOGS_DIR: Path = DATA_DIR / "logs"
ASSETS_DIR: Path = BASE_DIR / "assets"

# Archivos clave
TOKENS_FILE: Path = DATA_DIR / "tokens.json"
DATABASE_FILE: Path = DATA_DIR / "primi_motors.db"

# Crear directorios si no existen
DATA_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)
ASSETS_DIR.mkdir(parents=True, exist_ok=True)


# ==========================================================
# Datos del negocio (para PDFs de remitos)
# ==========================================================
EMPRESA = {
    "nombre": "Primi Motors",
    "direccion": "Calle 15 4971, Berazategui",
    "provincia_cp": "Buenos Aires CP 1884",
    "cuit": "23-37354799-9",
    "email": "adm.primimotors@gmail.com",
}


# ==========================================================
# Horarios de atención (Fase 5 — respuestas automáticas)
# ==========================================================
HORARIO_ATENCION_MANUAL = {
    "inicio_hora": 8,
    "inicio_minuto": 0,
    "fin_hora": 18,
    "fin_minuto": 30,
    "timezone": "America/Argentina/Buenos_Aires",
}


# ==========================================================
# Polling / sincronización con ML
# ==========================================================
POLLING_STOCK_MINUTOS = 10       # cada cuánto se sincroniza stock con ML
POLLING_PREGUNTAS_MINUTOS = 15   # cada cuánto se leen preguntas nuevas


if __name__ == "__main__":
    # Ejecutar `python config.py` para verificar configuración
    print("Configuración cargada correctamente")
    print(f"  Client ID: {ML_CLIENT_ID[:6]}...{ML_CLIENT_ID[-4:]}")
    print(f"  Client Secret: {'*' * 10}")
    print(f"  Redirect URI: {ML_REDIRECT_URI}")
    print(f"  Site ID: {ML_SITE_ID}")
    print(f"  Base de datos: {DATABASE_FILE}")
    print(f"  Empresa: {EMPRESA['nombre']} — CUIT {EMPRESA['cuit']}")
