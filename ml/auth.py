"""
ml/auth.py
==========
Gestión de autenticación OAuth 2.0 con Mercado Libre.

Flujo:
    1. Se construye una URL de autorización.
    2. Federico la abre, se loguea en ML y autoriza la app.
    3. ML redirige a https://www.google.com con un parámetro ?code=TG-xxxx
    4. Federico copia ese código y lo pega en la terminal.
    5. Con el code, pedimos a ML el access_token + refresh_token y los guardamos.
    6. De ahí en adelante, el access_token se renueva automáticamente con el refresh_token
       cuando esté por vencerse (cada 6 horas aprox).
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, asdict
from typing import Optional
from urllib.parse import urlencode, urlparse, parse_qs

import requests

import config
from logger import get_logger

log = get_logger(__name__)


# ==========================================================
# Modelo de tokens
# ==========================================================
@dataclass
class Tokens:
    """Contenedor de los tokens de ML con metadatos de expiración."""
    access_token: str
    refresh_token: str
    expires_at: float         # timestamp unix en que expira el access_token
    user_id: int
    scope: str
    token_type: str = "bearer"

    def esta_vencido(self, margen_segundos: int = 300) -> bool:
        """
        Devuelve True si el access_token ya venció o está por vencer.
        Usamos un margen de 5 minutos por seguridad.
        """
        return time.time() >= (self.expires_at - margen_segundos)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Tokens":
        return cls(**data)


# ==========================================================
# Persistencia de tokens
# ==========================================================
def guardar_tokens(tokens: Tokens) -> None:
    """Guarda los tokens en data/tokens.json."""
    with open(config.TOKENS_FILE, "w", encoding="utf-8") as f:
        json.dump(tokens.to_dict(), f, indent=2)
    log.info(f"Tokens guardados en {config.TOKENS_FILE}")


def cargar_tokens() -> Optional[Tokens]:
    """Carga tokens desde disco. Devuelve None si no existen."""
    if not config.TOKENS_FILE.exists():
        return None
    with open(config.TOKENS_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    return Tokens.from_dict(data)


# ==========================================================
# Flujo OAuth: paso 1 — URL de autorización
# ==========================================================
def construir_url_autorizacion(state: str = "primi-motors") -> str:
    """
    Construye la URL que Federico debe abrir en el navegador para autorizar la app.
    ML después redirige a https://www.google.com?code=TG-xxxx&state=primi-motors
    """
    params = {
        "response_type": "code",
        "client_id": config.ML_CLIENT_ID,
        "redirect_uri": config.ML_REDIRECT_URI,
        "state": state,
    }
    return f"{config.ML_AUTH_URL}?{urlencode(params)}"


# ==========================================================
# Flujo OAuth: paso 2 — Intercambio de code por tokens
# ==========================================================
def intercambiar_code_por_tokens(authorization_code: str) -> Tokens:
    """
    Toma el `code` que ML devolvió en la URL de Google y lo intercambia
    por un access_token + refresh_token.
    """
    log.info("Intercambiando authorization code por tokens...")
    payload = {
        "grant_type": "authorization_code",
        "client_id": config.ML_CLIENT_ID,
        "client_secret": config.ML_CLIENT_SECRET,
        "code": authorization_code,
        "redirect_uri": config.ML_REDIRECT_URI,
    }
    resp = requests.post(
        config.ML_TOKEN_URL,
        data=payload,
        headers={"Accept": "application/json"},
        timeout=30,
    )
    if resp.status_code != 200:
        log.error(f"Error intercambiando code: {resp.status_code} — {resp.text}")
        resp.raise_for_status()

    data = resp.json()
    tokens = Tokens(
        access_token=data["access_token"],
        refresh_token=data["refresh_token"],
        expires_at=time.time() + data["expires_in"],
        user_id=data["user_id"],
        scope=data.get("scope", ""),
        token_type=data.get("token_type", "bearer"),
    )
    guardar_tokens(tokens)
    log.info(f"Tokens obtenidos para user_id={tokens.user_id}")
    return tokens


# ==========================================================
# Flujo OAuth: paso 3 — Refresh automático
# ==========================================================
def refrescar_tokens(tokens: Tokens) -> Tokens:
    """
    Usa el refresh_token para conseguir un nuevo access_token.
    ML devuelve también un nuevo refresh_token (rotación), que reemplaza al anterior.
    """
    log.info("Refrescando access_token con refresh_token...")
    payload = {
        "grant_type": "refresh_token",
        "client_id": config.ML_CLIENT_ID,
        "client_secret": config.ML_CLIENT_SECRET,
        "refresh_token": tokens.refresh_token,
    }
    resp = requests.post(
        config.ML_TOKEN_URL,
        data=payload,
        headers={"Accept": "application/json"},
        timeout=30,
    )
    if resp.status_code != 200:
        log.error(f"Error refrescando tokens: {resp.status_code} — {resp.text}")
        resp.raise_for_status()

    data = resp.json()
    nuevos = Tokens(
        access_token=data["access_token"],
        refresh_token=data.get("refresh_token", tokens.refresh_token),
        expires_at=time.time() + data["expires_in"],
        user_id=data.get("user_id", tokens.user_id),
        scope=data.get("scope", tokens.scope),
        token_type=data.get("token_type", "bearer"),
    )
    guardar_tokens(nuevos)
    log.info("Tokens refrescados correctamente.")
    return nuevos


# ==========================================================
# API principal: obtener token válido (auto-refresh)
# ==========================================================
def obtener_token_valido() -> str:
    """
    Devuelve un access_token vigente, refrescándolo si hace falta.
    Esta es la función que usan todos los demás módulos.
    """
    tokens = cargar_tokens()
    if tokens is None:
        raise RuntimeError(
            "No hay tokens guardados. Ejecutá primero 'python get_initial_token.py' "
            "para hacer la autenticación inicial."
        )

    if tokens.esta_vencido():
        log.info("El access_token está vencido o por vencer — refrescando.")
        tokens = refrescar_tokens(tokens)

    return tokens.access_token


# ==========================================================
# Utilidad — extraer code desde URL completa de Google
# ==========================================================
def extraer_code_de_url(url_completa: str) -> str:
    """
    Federico puede pegar la URL completa (ej:
    https://www.google.com/?code=TG-xxx&state=yyy) en vez del código suelto.
    Esta función extrae el code.
    """
    url_limpia = url_completa.strip()
    # Si ya parece un code puro (empieza con TG-), devolverlo
    if url_limpia.startswith("TG-"):
        return url_limpia

    parsed = urlparse(url_limpia)
    params = parse_qs(parsed.query)
    code_list = params.get("code", [])
    if not code_list:
        raise ValueError(
            "No se encontró el parámetro 'code' en la URL. "
            "Revisá que hayas copiado bien la URL después de autorizar."
        )
    return code_list[0]


# ==========================================================
# Test rápido de conexión (opcional)
# ==========================================================
def probar_conexion() -> dict:
    """
    Llama al endpoint /users/me de ML para validar que el token funciona.
    Devuelve los datos del usuario autenticado.
    """
    token = obtener_token_valido()
    resp = requests.get(
        f"{config.ML_API_BASE}/users/me",
        headers={"Authorization": f"Bearer {token}"},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()
