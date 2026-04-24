"""
app/main.py
===========
FastAPI — backend web de Primi Motors.

Estructura:
  - `/health`, `/status`      → públicos (para Render + debug).
  - `/login`, `/logout`       → autenticación.
  - `/`                       → home protegida (sólo admin logueado).
  - Todo lo demás: protegido por `Depends(require_user)`.

Sesión: cookie firmada con `SESSION_SECRET` (env var). Expira a los 7 días.
"""

from __future__ import annotations

import os
import platform
from datetime import datetime, timezone
from pathlib import Path

from fastapi import Depends, FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from . import auth, database

APP_NAME = "Primi Motors — Backend"
APP_VERSION = "0.3.0"

# Raíz del paquete app/
BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

app = FastAPI(
    title=APP_NAME,
    version=APP_VERSION,
    description="Backend web de Primi Motors (ML, stock, publicaciones).",
)

# -------- Sesión --------
# SESSION_SECRET debe venir de env vars en Render. Si no está, generamos uno
# EFÍMERO (las sesiones se invalidan en cada reinicio). En producción real
# SIEMPRE definirlo como env var para que las sesiones sobrevivan redeploys.
_session_secret = os.environ.get("SESSION_SECRET")
if not _session_secret:
    import secrets as _secrets
    _session_secret = _secrets.token_urlsafe(32)
    # No imprimimos el secret por log (no queremos que quede en los logs de Render).

app.add_middleware(
    SessionMiddleware,
    secret_key=_session_secret,
    session_cookie="primi_session",
    max_age=7 * 24 * 3600,        # 7 días
    same_site="lax",
    https_only=True,              # Render sirve sobre HTTPS
)


# ===============================================================
# Endpoints públicos
# ===============================================================

@app.get("/health")
def health() -> JSONResponse:
    """Health-check para Render (200 OK si el proceso está vivo)."""
    return JSONResponse({"status": "ok", "service": APP_NAME, "version": APP_VERSION})


@app.get("/status")
def status() -> JSONResponse:
    """Info rápida del entorno (útil para debug de deploys)."""
    return JSONResponse({
        "service": APP_NAME,
        "version": APP_VERSION,
        "python": platform.python_version(),
        "platform": platform.platform(),
        "env": os.environ.get("RENDER_SERVICE_NAME", "local"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "auth_configured": bool(os.environ.get("ADMIN_USER") and os.environ.get("ADMIN_PASSWORD_HASH")),
        "db_configured": bool(os.environ.get("DATABASE_URL")),
        "db_connected": database.ping(),
    })


# ===============================================================
# Startup: crear tablas si no existen
# ===============================================================

@app.on_event("startup")
def _startup() -> None:
    """
    Al arrancar, si hay DB configurada, aseguramos que las tablas existan.
    Cuando tengamos modelos reales, init_db() los creará acá.
    Idempotente: no rompe si ya están creadas.
    """
    try:
        database.init_db()
    except Exception as e:
        # No hacemos crash del proceso por un error de DB al arranque —
        # preferimos que /health siga OK y ver el problema en /status.
        print(f"[startup] init_db falló: {e}")


# ===============================================================
# Login / logout
# ===============================================================

@app.get("/login", response_class=HTMLResponse)
def login_form(request: Request):
    """Form de login. Si ya está logueado, redirige a la home."""
    if auth.current_user(request):
        return RedirectResponse("/", status_code=303)
    # Starlette ≥0.29: request va como primer arg posicional, no en el context.
    return templates.TemplateResponse(request, "login.html", {"error": None})


@app.post("/login", response_class=HTMLResponse)
def login_submit(
    request: Request,
    user: str = Form(...),
    password: str = Form(...),
):
    """Verifica credenciales y crea sesión."""
    if auth.check_credentials(user, password):
        auth.login_session(request, user)
        return RedirectResponse("/", status_code=303)
    return templates.TemplateResponse(
        request,
        "login.html",
        {"error": "Usuario o contraseña incorrectos."},
        status_code=401,
    )


@app.post("/logout")
@app.get("/logout")
def logout(request: Request):
    auth.logout_session(request)
    return RedirectResponse("/login", status_code=303)


# ===============================================================
# Home (protegida)
# ===============================================================

@app.get("/", response_class=HTMLResponse)
def home(request: Request, user: str = Depends(auth.require_user)) -> str:
    """Landing privada. El dashboard real se arma acá más adelante."""
    return f"""
    <!doctype html>
    <html lang="es">
      <head>
        <meta charset="utf-8" />
        <title>{APP_NAME}</title>
        <style>
          body {{
            font-family: -apple-system, system-ui, Segoe UI, Roboto, sans-serif;
            background: #0b0b0b; color: #f2f2f2;
            margin: 0; min-height: 100vh;
            display: flex; align-items: center; justify-content: center;
          }}
          .card {{
            background: #141414; border: 1px solid #222;
            padding: 48px 56px; border-radius: 14px; max-width: 620px;
          }}
          h1 {{ margin: 0 0 8px; color: #ffb703; font-size: 28px; }}
          p  {{ margin: 8px 0; color: #bbb; line-height: 1.5; }}
          code {{ background: #1c1c1c; padding: 2px 6px; border-radius: 4px; color: #ffb703; }}
          .row {{ display:flex; justify-content:space-between; align-items:center; margin-top:28px; font-size:13px; }}
          a {{ color:#ffb703; text-decoration:none; }}
          a:hover {{ text-decoration:underline; }}
        </style>
      </head>
      <body>
        <div class="card">
          <h1>🚗 Primi Motors — Panel</h1>
          <p>Hola, <strong>{user}</strong>. Deploy versión <code>{APP_VERSION}</code>.</p>
          <p>Próximamente: subir Excel master, publicar lote ML, sync stock, dashboard.</p>
          <div class="row">
            <span>admin.primimotors.com.ar</span>
            <a href="/logout">Cerrar sesión</a>
          </div>
        </div>
      </body>
    </html>
    """
