"""
logger.py
=========
Configuración del sistema de logs.
Todos los módulos usan este logger para tener formato y destino unificado.
"""

from __future__ import annotations

import logging
import logging.handlers
from pathlib import Path

import config


# Formato legible: [FECHA HORA] [NIVEL] [módulo] mensaje
_FORMATO = "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
_FECHA_FORMATO = "%Y-%m-%d %H:%M:%S"

# Archivo de log rotativo: máximo 5 MB por archivo, se guardan 5 backups
_ARCHIVO_LOG = config.LOGS_DIR / "primi_motors.log"


def get_logger(nombre: str) -> logging.Logger:
    """
    Devuelve un logger configurado para el módulo dado.

    Uso:
        from logger import get_logger
        log = get_logger(__name__)
        log.info("Iniciando sincronización de stock")
    """
    log = logging.getLogger(nombre)
    if log.handlers:
        # Ya fue configurado, devolver tal cual
        return log

    log.setLevel(logging.DEBUG)

    formatter = logging.Formatter(_FORMATO, datefmt=_FECHA_FORMATO)

    # Handler para consola (solo INFO y superior)
    consola = logging.StreamHandler()
    consola.setLevel(logging.INFO)
    consola.setFormatter(formatter)
    log.addHandler(consola)

    # Handler para archivo rotativo (todo desde DEBUG)
    archivo = logging.handlers.RotatingFileHandler(
        _ARCHIVO_LOG,
        maxBytes=5 * 1024 * 1024,  # 5 MB
        backupCount=5,
        encoding="utf-8",
    )
    archivo.setLevel(logging.DEBUG)
    archivo.setFormatter(formatter)
    log.addHandler(archivo)

    # Evitar que los mensajes se dupliquen si el root logger está configurado
    log.propagate = False

    return log
