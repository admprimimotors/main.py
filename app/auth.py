"""
app/auth.py
===========
Autenticación del panel admin.

Decisiones de diseño:
  - UN SOLO usuario admin (no hay tabla de users). Credenciales en env vars
    de Render: ADMIN_USER + ADMIN_PASSWORD_HASH.
  - Password se guarda hasheada con PBKDF2-HMAC-SHA256 (stdlib, sin
    dependencias externas). 260k iteraciones → coste ~300ms en CPU normal.
  - Sesión en cookie firmada con itsdangerous (Starlette's SessionMiddleware).
  - Cookie HttpOnly + Secure + SameSite=Lax. Expira a los 7 días.

Formato del hash almacenado en ADMIN_PASSWORD_HASH:
    pbkdf2_sha256$<iter>$<salt_b64>$<hash_b64>

Rotar la password: generar nuevo hash con `python -m app.auth hash <nueva>`
y actualizar la env var en Render.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import os
import secrets
from typing import Optional

from fastapi import Depends, HTTPException, Request, status
from fastapi.responses import RedirectResponse


# ---------------------------------------------------------------------
# Hashing
# ---------------------------------------------------------------------

PBKDF2_ITER = 260_000
PBKDF2_ALGO = "sha256"


def hash_password(password: str) -> str:
    """Genera un hash PBKDF2 para guardar en ADMIN_PASSWORD_HASH."""
    salt = os.urandom(16)
    dk = hashlib.pbkdf2_hmac(PBKDF2_ALGO, password.encode("utf-8"), salt, PBKDF2_ITER)
    return f"pbkdf2_sha256${PBKDF2_ITER}${base64.b64encode(salt).decode()}${base64.b64encode(dk).decode()}"


def verify_password(password: str, stored: str) -> bool:
    """Verifica un intento de password contra el hash almacenado."""
    try:
        algo, iter_s, salt_b64, hash_b64 = stored.split("$")
    except ValueError:
        return False
    if algo != "pbkdf2_sha256":
        return False
    try:
        iterations = int(iter_s)
        salt = base64.b64decode(salt_b64)
        expected = base64.b64decode(hash_b64)
    except Exception:
        return False
    candidate = hashlib.pbkdf2_hmac(PBKDF2_ALGO, password.encode("utf-8"), salt, iterations)
    # hmac.compare_digest evita timing attacks
    return hmac.compare_digest(candidate, expected)


# ---------------------------------------------------------------------
# Sesiones
# ---------------------------------------------------------------------

def _admin_user() -> str:
    return (os.environ.get("ADMIN_USER") or "").strip()


def _admin_hash() -> str:
    return (os.environ.get("ADMIN_PASSWORD_HASH") or "").strip()


def check_credentials(user: str, password: str) -> bool:
    """¿Las credenciales son válidas contra las env vars?"""
    admin_u = _admin_user()
    admin_h = _admin_hash()
    if not admin_u or not admin_h:
        # Sin config → no dejamos entrar a nadie, ni siquiera con cred vacías
        return False
    # Comparación del user en tiempo constante también
    if not hmac.compare_digest(user.strip(), admin_u):
        return False
    return verify_password(password, admin_h)


def login_session(request: Request, user: str) -> None:
    """Graba en la cookie de sesión que este request ya está logueado."""
    request.session["user"] = user
    request.session["token"] = secrets.token_urlsafe(16)  # para invalidar con logout


def logout_session(request: Request) -> None:
    request.session.clear()


def current_user(request: Request) -> Optional[str]:
    return request.session.get("user")


# ---------------------------------------------------------------------
# Dependency para rutas protegidas
# ---------------------------------------------------------------------

def require_user(request: Request) -> str:
    """
    FastAPI dependency: si no hay sesión, redirige a /login.
    Las rutas protegidas la usan como `Depends(require_user)`.
    """
    user = current_user(request)
    if not user:
        # Para páginas HTML, redirigimos. Para JSON, devolvemos 401.
        accept = request.headers.get("accept", "")
        if "text/html" in accept:
            raise HTTPException(
                status_code=status.HTTP_303_SEE_OTHER,
                headers={"Location": "/login"},
            )
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="not authenticated")
    return user


# ---------------------------------------------------------------------
# CLI: `python -m app.auth hash <password>`
# ---------------------------------------------------------------------

if __name__ == "__main__":
    import getpass
    import sys

    if len(sys.argv) >= 2 and sys.argv[1] == "hash":
        # Modo interactivo seguro: no queda en historial ni se ve al tipear.
        if len(sys.argv) >= 3:
            # Para scripts automatizados (menos seguro, password en argv).
            pwd = sys.argv[2]
        else:
            pwd = getpass.getpass("Password nueva (no se muestra): ")
            pwd2 = getpass.getpass("Repetir para confirmar: ")
            if pwd != pwd2:
                print("✗ Las passwords no coinciden.", file=sys.stderr)
                sys.exit(1)
            if len(pwd) < 8:
                print("✗ Password muy corta (mínimo 8 caracteres).", file=sys.stderr)
                sys.exit(1)
        print()
        print("Pegá este valor en Render → Environment → ADMIN_PASSWORD_HASH:")
        print()
        print(hash_password(pwd))
        print()
    else:
        print("Uso: python -m app.auth hash           (interactivo, recomendado)", file=sys.stderr)
        print("     python -m app.auth hash <password>  (argumento, NO recomendado)", file=sys.stderr)
        sys.exit(2)
