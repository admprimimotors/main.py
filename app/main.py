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

from fastapi import Depends, FastAPI, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session as DbSession
from starlette.middleware.sessions import SessionMiddleware

from . import auth, catalogo, database
from .database import get_db

APP_NAME = "Primi Motors — Backend"
APP_VERSION = "0.6.0"

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
        "db_tables": database.count_tables(),
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
# Home (dashboard)
# ===============================================================

@app.get("/", response_class=HTMLResponse)
def home(request: Request, user: str = Depends(auth.require_user)):
    """Landing privada — dashboard con métricas (placeholder hasta tener data)."""
    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {"user": user, "active": "home", "version": APP_VERSION},
    )


# ===============================================================
# Catálogo — listado, upload de Excel master, template
# ===============================================================

@app.get("/catalogo", response_class=HTMLResponse)
def catalogo_view(
    request: Request,
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
    q: str = "",
    page: int = 1,
):
    """Listado paginado de productos con buscador."""
    productos, total = catalogo.list_productos(db, search=q, page=page)
    flash = request.session.pop("flash", None)
    return templates.TemplateResponse(
        request,
        "catalogo.html",
        {
            "user": user,
            "active": "catalogo",
            "version": APP_VERSION,
            "productos": productos,
            "total": total,
            "search": q,
            "page": page,
            "page_size": catalogo.PAGE_SIZE,
            "flash": flash,
        },
    )


@app.post("/catalogo/upload")
async def catalogo_upload(
    request: Request,
    archivo: UploadFile = File(...),
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
):
    """Recibe el Excel master, lo procesa y guarda flash con el resultado."""
    fname = (archivo.filename or "").lower()
    if not fname.endswith((".xlsx", ".xls")):
        request.session["flash"] = {
            "type": "error",
            "msg": "El archivo debe ser .xlsx o .xls",
        }
        return RedirectResponse("/catalogo", status_code=303)

    # Wrap completo: si algo explota inesperado, mostramos el error en pantalla
    # en vez de devolver un 500 mudo.
    try:
        file_bytes = await archivo.read()
        result = catalogo.process_excel_upload(db, file_bytes)
    except Exception as e:
        # Logueamos a stderr (Render → Logs) y pasamos un mensaje al usuario.
        import traceback
        traceback.print_exc()
        try:
            db.rollback()
        except Exception:
            pass
        request.session["flash"] = {
            "type": "error",
            "msg": f"Error inesperado: {type(e).__name__}: {e}",
        }
        return RedirectResponse("/catalogo", status_code=303)

    if result.ok:
        msg = (
            f"✓ {result.productos_total} productos procesados "
            f"({result.productos_insertados} nuevos, {result.productos_actualizados} actualizados)"
        )
        if result.compats_creadas:
            msg += f", {result.compats_creadas} compatibilidades"
            if result.vehiculos_creados:
                msg += f" ({result.vehiculos_creados} vehículos nuevos)"
        request.session["flash"] = {"type": "success", "msg": msg}
    else:
        msg = (
            f"Procesado con errores. "
            f"Productos: {result.productos_total}, compats: {result.compats_creadas}. "
            f"Errores: {' · '.join(result.errores[:5])}"
        )
        if len(result.errores) > 5:
            msg += f" (+{len(result.errores) - 5} más)"
        request.session["flash"] = {"type": "warning", "msg": msg}

    return RedirectResponse("/catalogo", status_code=303)


@app.get("/catalogo/template")
def catalogo_template(user: str = Depends(auth.require_user)):
    """Descarga un Excel template con las hojas y headers canónicos."""
    excel_bytes = catalogo.generate_template()
    return Response(
        content=excel_bytes,
        media_type=(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ),
        headers={
            "Content-Disposition": 'attachment; filename="primi_motors_template.xlsx"'
        },
    )


# ===============================================================
# Stubs — secciones todavía sin construir
# ===============================================================
# Cada feature real va a reemplazar uno de estos handlers cuando esté lista.
# El objetivo del stub es que el sidebar funcione end-to-end desde el día 1
# (clickeás cualquier sección y te lleva a una página coherente).

_STUBS = [
    # "catalogo" ya no es stub — vive en su propio módulo (app/catalogo.py + rutas más abajo)
    ("stock", "Stock",
     "Niveles de stock por SKU, alertas de bajo stock y reactivación "
     "de ítems pausados en Mercado Libre."),
    ("publicaciones", "Publicaciones ML",
     "Estado de los ítems publicados en Mercado Libre — pausar, "
     "republicar y ver estadísticas de cada uno."),
    ("precios", "Precios",
     "Cambios masivos de precio, márgenes por categoría y manejo "
     "de listas de precios."),
    ("clientes", "Clientes",
     "Historial de compras, remitos y notas de crédito por cliente."),
    ("mensajes", "Mensajes ML",
     "Preguntas de compradores en Mercado Libre y respuestas "
     "automáticas inteligentes."),
    ("config", "Configuración",
     "Tokens de Mercado Libre, ajustes del sistema y gestión "
     "de usuarios del panel."),
]


def _make_stub_handler(slug: str, name: str, desc: str):
    """Factory: arma un handler para una sección stub."""
    def _handler(request: Request, user: str = Depends(auth.require_user)):
        return templates.TemplateResponse(
            request,
            "stub.html",
            {
                "user": user,
                "active": slug,
                "version": APP_VERSION,
                "section_name": name,
                "section_desc": desc,
            },
        )
    _handler.__name__ = f"stub_{slug}"
    return _handler


for _slug, _name, _desc in _STUBS:
    app.get(f"/{_slug}", response_class=HTMLResponse)(
        _make_stub_handler(_slug, _name, _desc)
    )
