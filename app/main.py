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
from typing import Optional

from fastapi import Depends, FastAPI, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session as DbSession
from starlette.middleware.sessions import SessionMiddleware

from . import auth, catalogo, database, ml_client, precios, stock, storage
from .database import get_db

APP_NAME = "Primi Motors — Backend"
APP_VERSION = "0.20.0"

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
    r2_var_names = (
        "R2_ACCOUNT_ID",
        "R2_ACCESS_KEY_ID",
        "R2_SECRET_ACCESS_KEY",
        "R2_BUCKET",
        "R2_PUBLIC_URL",
    )
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
        "r2_configured": storage.is_configured(),
        "r2_vars_detected": [k for k in r2_var_names if os.environ.get(k)],
        "r2_vars_missing": [k for k in r2_var_names if not os.environ.get(k)],
        "ml_configured": ml_client.is_configured(),
        "ml_write_enabled": ml_client.is_write_enabled(),
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
    vinculadas: str = "",
    categoria: str = "",
    marca: str = "",
    rentabilidad: str = "",
):
    """Listado paginado de productos con buscador y filtros."""
    productos, total = catalogo.list_productos(
        db,
        search=q,
        page=page,
        vinculadas=vinculadas,
        categoria=categoria,
        marca=marca,
        rentabilidad=rentabilidad,
    )
    categorias_disponibles = catalogo.list_categorias(db)
    marcas_disponibles = catalogo.list_marcas(db)
    placeholders_pendientes = catalogo.count_placeholders_pendientes(db)
    flash = request.session.pop("flash", None)
    # Guardar la URL relativa (path+query) para que el detalle del producto y
    # los endpoints bulk tengan adónde volver. Path+query (no la URL absoluta)
    # para que sirva como redirect target sin problemas de scheme/host.
    relative_url = request.url.path
    if request.url.query:
        relative_url += "?" + request.url.query
    request.session["last_catalogo_url"] = relative_url
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
            "vinculadas": vinculadas,
            "categoria": categoria,
            "marca": marca,
            "rentabilidad": rentabilidad,
            "categorias_disponibles": categorias_disponibles,
            "marcas_disponibles": marcas_disponibles,
            "placeholders_pendientes": placeholders_pendientes,
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


# IMPORTANTE: esta ruta va DESPUÉS de /upload y /template porque {sku} captura
# cualquier path. Si la ponemos antes, se come a las dos rutas específicas.
@app.get("/catalogo/{sku}", response_class=HTMLResponse)
def catalogo_detail(
    request: Request,
    sku: str,
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
):
    """Vista detalle de un producto individual."""
    detail = catalogo.get_producto_detail(db, sku)
    if detail is None:
        # SKU no existe — redirigimos al listado con un flash de error
        request.session["flash"] = {
            "type": "error",
            "msg": f"No se encontró el producto con SKU '{sku}'.",
        }
        return RedirectResponse("/catalogo", status_code=303)
    flash = request.session.pop("flash", None)
    rentabilidad = precios.analyze_rentabilidad_ml(
        precio_costo=detail["precio_costo"],
        precio_final=detail["precio_final"],
        envio_fijo_producto=detail.get("ml_envio_fijo"),
        impuestos_pct_producto=detail.get("ml_impuestos_pct"),
        comision_pct_producto=detail.get("ml_comision_pct"),
    )
    back_url = request.session.get("last_catalogo_url") or "/catalogo"
    return templates.TemplateResponse(
        request,
        "producto.html",
        {
            "user": user,
            "active": "catalogo",
            "version": APP_VERSION,
            "producto": detail,
            "flash": flash,
            "r2_configured": storage.is_configured(),
            "ml_configured": ml_client.is_configured(),
            "ml_write_enabled": ml_client.is_write_enabled(),
            "rentabilidad": rentabilidad,
            "back_url": back_url,
        },
    )


# ---------------------------------------------------------------
# Fotos del producto: subir + eliminar
# ---------------------------------------------------------------

@app.post("/catalogo/{sku}/fotos")
async def catalogo_foto_upload(
    request: Request,
    sku: str,
    archivos: list[UploadFile] = File(...),
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
):
    """Sube una o más fotos a R2 y las asocia al producto."""
    if not storage.is_configured():
        request.session["flash"] = {
            "type": "error",
            "msg": "Storage R2 no está configurado. Cargá las env vars en Render.",
        }
        return RedirectResponse(f"/catalogo/{sku}", status_code=303)

    subidas = 0
    errores: list[str] = []
    for archivo in archivos:
        if not archivo.filename:
            continue
        try:
            file_bytes = await archivo.read()
            ok, msg = catalogo.add_foto(db, sku, file_bytes, archivo.filename)
            if ok:
                subidas += 1
            else:
                errores.append(f"{archivo.filename}: {msg}")
        except Exception as e:
            import traceback
            traceback.print_exc()
            errores.append(f"{archivo.filename}: {type(e).__name__}: {e}")

    if subidas and not errores:
        request.session["flash"] = {
            "type": "success",
            "msg": f"✓ {subidas} foto{'' if subidas == 1 else 's'} subida{'' if subidas == 1 else 's'} correctamente.",
        }
    elif subidas:
        request.session["flash"] = {
            "type": "warning",
            "msg": (
                f"{subidas} subidas, {len(errores)} con error: "
                + " · ".join(errores[:3])
                + (f" (+{len(errores) - 3} más)" if len(errores) > 3 else "")
            ),
        }
    else:
        request.session["flash"] = {
            "type": "error",
            "msg": "No se pudo subir ninguna foto: " + " · ".join(errores[:3]),
        }

    return RedirectResponse(f"/catalogo/{sku}", status_code=303)


@app.post("/catalogo/{sku}/fotos/{foto_id}/delete")
def catalogo_foto_delete(
    request: Request,
    sku: str,
    foto_id: int,
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
):
    """Elimina una foto del producto (de R2 y de la DB)."""
    ok, msg = catalogo.delete_foto(db, foto_id)
    request.session["flash"] = {
        "type": "success" if ok else "error",
        "msg": msg,
    }
    return RedirectResponse(f"/catalogo/{sku}", status_code=303)


# ---------------------------------------------------------------
# Mercado Libre — bulk linkeo + sync individual (read-only)
# ---------------------------------------------------------------

# IMPORTANTE: estas rutas van ANTES de /catalogo/{sku} en el archivo, pero
# como FastAPI matchea por exactitud antes que por param, /catalogo/ml-link/upload
# y /catalogo/ml-link/template no se ven afectadas por /catalogo/{sku}.

@app.post("/catalogo/ml-link/upload")
async def catalogo_ml_link_upload(
    request: Request,
    archivo: UploadFile = File(...),
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
):
    """
    Bulk linkeo: SKU → ML_Item_ID via Excel.
    Por default crea placeholders para SKUs que aún no están en el catálogo.
    """
    fname = (archivo.filename or "").lower()
    if not fname.endswith((".xlsx", ".xls")):
        request.session["flash"] = {
            "type": "error",
            "msg": "El archivo debe ser .xlsx o .xls",
        }
        return RedirectResponse("/catalogo", status_code=303)

    try:
        file_bytes = await archivo.read()
        result = catalogo.process_ml_link_upload(
            db, file_bytes, crear_faltantes=True
        )
    except Exception as e:
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

    # Armar mensaje con todos los conteos
    parts = []
    if result.vinculados:
        parts.append(f"{result.vinculados} vinculaciones")
    if result.creados_placeholder:
        parts.append(f"{result.creados_placeholder} placeholders creados")
    if result.sin_cambio:
        parts.append(f"{result.sin_cambio} sin cambios")
    summary = " · ".join(parts) if parts else "ningún cambio"

    if result.ok:
        request.session["flash"] = {
            "type": "success",
            "msg": f"✓ Linkeo OK — {summary}.",
        }
    else:
        msg = (
            f"Linkeo con errores — {summary}. "
            f"{len(result.errores)} errores: "
            + " · ".join(result.errores[:5])
        )
        if len(result.errores) > 5:
            msg += f" (+{len(result.errores) - 5} más)"
        request.session["flash"] = {
            "type": "warning" if (result.vinculados or result.creados_placeholder) else "error",
            "msg": msg,
        }

    return RedirectResponse("/catalogo", status_code=303)


# ---------------------------------------------------------------
# Bulk operations: hidratar y push a ML, con selección por checkbox
# o "todos los que matchean los filtros activos"
# ---------------------------------------------------------------

# Caps conservadores para evitar timeouts de Render (~100s por request).
HIDRATAR_CAP = 5     # cada hidratación toma 10-15s por las fotos
PUSH_CAP = 50        # cada push es ~500ms


def _back_to_catalogo(request: Request) -> str:
    """Para endpoints bulk: volver a la última URL del catálogo (con filtros y página)."""
    return request.session.get("last_catalogo_url") or "/catalogo"


def _resolver_skus_bulk(
    db: DbSession,
    *,
    skus_form: list[str],
    modo: str,
    cap: int,
    only_linked: bool,
    filtro_q: str,
    filtro_vinculadas: str,
    filtro_categoria: str,
    filtro_marca: str,
) -> list[str]:
    """
    Resuelve la lista efectiva de SKUs a procesar según el modo:
      - "seleccionados": usa los SKUs del form, capeados
      - "matching": ignora el form, busca en DB los más antiguos que
        matchean los filtros del listado
    """
    if modo == "matching":
        return catalogo.skus_oldest_matching(
            db,
            search=filtro_q,
            vinculadas=filtro_vinculadas,
            categoria=filtro_categoria,
            marca=filtro_marca,
            limit=cap,
            only_linked=only_linked,
        )
    return [s for s in skus_form if s][:cap]


@app.post("/catalogo/bulk/hidratar-pendientes/batch")
def catalogo_bulk_hidratar_batch(
    request: Request,
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
):
    """
    Endpoint JSON que el frontend llama en loop para hidratar todos los placeholders.
    Cada batch procesa hasta HIDRATAR_CAP productos y devuelve {processed, remaining, done}.

    El JS del browser sigue llamando este endpoint hasta que `done=true`. Esto evita
    el timeout de Render manteniendo cada request individual en ~25-75s.
    """
    try:
        result = catalogo.hidratar_batch_placeholders(db, limit=HIDRATAR_CAP)
    except Exception as e:
        import traceback
        traceback.print_exc()
        try:
            db.rollback()
        except Exception:
            pass
        return JSONResponse(
            {"processed": 0, "remaining": 0, "done": True,
             "errors": [f"{type(e).__name__}: {e}"], "skus_done": []},
            status_code=500,
        )
    return JSONResponse(result)


@app.post("/catalogo/bulk/editar")
def catalogo_bulk_editar(
    request: Request,
    skus: list[str] = Form(default=[]),
    campo: str = Form(...),
    valor: str = Form(default=""),
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
):
    """
    Aplica un valor uniforme a un campo (categoria/marca/moneda/activo) sobre
    los SKUs seleccionados.
    """
    try:
        aplicados, errores = catalogo.bulk_edit_skus(db, skus, campo, valor)
    except Exception as e:
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
        return RedirectResponse(_back_to_catalogo(request), status_code=303)

    if errores and not aplicados:
        request.session["flash"] = {"type": "error", "msg": " · ".join(errores[:3])}
    elif errores:
        request.session["flash"] = {
            "type": "warning",
            "msg": (
                f"{aplicados} actualizados · {len(errores)} errores: "
                + " · ".join(errores[:3])
            ),
        }
    else:
        valor_display = valor if valor else "(vacío)"
        request.session["flash"] = {
            "type": "success",
            "msg": f"✓ {aplicados} producto{'' if aplicados == 1 else 's'} actualizado{'' if aplicados == 1 else 's'} · {campo} = {valor_display}",
        }

    return RedirectResponse(_back_to_catalogo(request), status_code=303)


@app.post("/catalogo/bulk/push")
def catalogo_bulk_push(
    request: Request,
    skus: list[str] = Form(default=[]),
    modo: str = Form(default="seleccionados"),
    filtro_q: str = Form(default=""),
    filtro_vinculadas: str = Form(default=""),
    filtro_categoria: str = Form(default=""),
    filtro_marca: str = Form(default=""),
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
):
    """Pushea a ML stock + precio del DB local de los SKUs seleccionados (cap PUSH_CAP)."""
    if not ml_client.is_write_enabled():
        request.session["flash"] = {
            "type": "error",
            "msg": "Write sync deshabilitado. Seteá ML_SYNC_WRITE_ENABLED=true en Render.",
        }
        return RedirectResponse(_back_to_catalogo(request), status_code=303)

    target_skus = _resolver_skus_bulk(
        db,
        skus_form=skus,
        modo=modo,
        cap=PUSH_CAP,
        only_linked=True,
        filtro_q=filtro_q,
        filtro_vinculadas=filtro_vinculadas,
        filtro_categoria=filtro_categoria,
        filtro_marca=filtro_marca,
    )

    if not target_skus:
        request.session["flash"] = {
            "type": "warning",
            "msg": "Ningún SKU para pushear.",
        }
        return RedirectResponse(_back_to_catalogo(request), status_code=303)

    ok = 0
    errores: list[str] = []
    for sku in target_skus:
        try:
            success, msg = catalogo.push_to_ml(
                db, sku, push_stock=True, push_price=True
            )
        except Exception as e:
            success = False
            msg = f"{type(e).__name__}: {e}"
        if success:
            ok += 1
        else:
            errores.append(f"{sku}: {msg}")

    total = len(target_skus)
    if ok == total:
        msg = f"✓ {ok} productos pusheados a ML (stock + precio)."
        flash_type = "success"
    elif ok:
        msg = (
            f"{ok}/{total} OK · {len(errores)} con error: "
            + " · ".join(errores[:3])
        )
        flash_type = "warning"
    else:
        msg = "Ninguno se pusheó: " + " · ".join(errores[:3])
        flash_type = "error"

    if total == PUSH_CAP and modo == "matching":
        msg += " · Quedan más para pushear — repetí el botón."

    request.session["flash"] = {"type": flash_type, "msg": msg}
    return RedirectResponse(_back_to_catalogo(request), status_code=303)


@app.post("/catalogo/ml-link/sync-batch")
def catalogo_ml_sync_batch(
    request: Request,
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
):
    """
    Sincroniza desde ML los 50 productos vinculados con sync más antiguo.
    Ideal para procesar lotes grandes recién linkeados sin tener que clickear
    sync individual en cada uno.
    """
    BATCH = 50
    try:
        ok, total, errores = catalogo.bulk_sync_oldest(db, limit=BATCH)
    except Exception as e:
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
        return RedirectResponse(_back_to_catalogo(request), status_code=303)

    if total == 0:
        request.session["flash"] = {
            "type": "success",
            "msg": "No hay más productos vinculados pendientes de sync. Todo al día.",
        }
    elif ok == total:
        request.session["flash"] = {
            "type": "success",
            "msg": f"✓ {ok} productos sincronizados desde ML. Si quedan más, volvé a apretar.",
        }
    else:
        msg = (
            f"{ok}/{total} OK · {len(errores)} errores: "
            + " · ".join(errores[:3])
        )
        if len(errores) > 3:
            msg += f" (+{len(errores) - 3} más)"
        request.session["flash"] = {
            "type": "warning" if ok else "error",
            "msg": msg,
        }

    return RedirectResponse(_back_to_catalogo(request), status_code=303)


@app.get("/catalogo/ml-link/template")
def catalogo_ml_link_template(user: str = Depends(auth.require_user)):
    """Excel template para el bulk linkeo (3 columnas: SKU, ML_Item_ID, ML_Permalink)."""
    excel_bytes = catalogo.generate_ml_link_template()
    return Response(
        content=excel_bytes,
        media_type=(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ),
        headers={
            "Content-Disposition": 'attachment; filename="primi_motors_ml_link_template.xlsx"'
        },
    )


@app.post("/catalogo/{sku}/ml-sync")
def catalogo_ml_sync(
    request: Request,
    sku: str,
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
):
    """Sync READ-ONLY desde ML para un producto: pulla precio/stock/status."""
    try:
        ok, msg = catalogo.sync_producto_from_ml(db, sku)
    except Exception as e:
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
        return RedirectResponse(f"/catalogo/{sku}", status_code=303)

    request.session["flash"] = {
        "type": "success" if ok else "error",
        "msg": msg,
    }
    return RedirectResponse(f"/catalogo/{sku}", status_code=303)


# ===============================================================
# Stock — resumen, listado de stock bajo, bulk update
# ===============================================================

@app.get("/stock", response_class=HTMLResponse)
def stock_view(
    request: Request,
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
):
    """Página principal de stock: summary + low-stock + upload form."""
    summary = stock.get_summary(db)
    low_stock_list = stock.list_low_stock(db, threshold=summary["low_threshold"])
    flash = request.session.pop("flash", None)
    return templates.TemplateResponse(
        request,
        "stock.html",
        {
            "user": user,
            "active": "stock",
            "version": APP_VERSION,
            "summary": summary,
            "low_stock_list": low_stock_list,
            "flash": flash,
        },
    )


@app.post("/stock/upload")
async def stock_upload(
    request: Request,
    archivo: UploadFile = File(...),
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
):
    """Recibe un Excel simplificado (SKU + Stock_Actual) y actualiza solo el stock."""
    fname = (archivo.filename or "").lower()
    if not fname.endswith((".xlsx", ".xls")):
        request.session["flash"] = {
            "type": "error",
            "msg": "El archivo debe ser .xlsx o .xls",
        }
        return RedirectResponse("/stock", status_code=303)

    try:
        file_bytes = await archivo.read()
        result = stock.process_stock_upload(db, file_bytes)
    except Exception as e:
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
        return RedirectResponse("/stock", status_code=303)

    if result.ok:
        request.session["flash"] = {
            "type": "success",
            "msg": f"✓ {result.actualizados} producto{'' if result.actualizados == 1 else 's'} actualizado{'' if result.actualizados == 1 else 's'}.",
        }
    else:
        msg = (
            f"Procesado con errores. "
            f"Actualizados: {result.actualizados}. "
            f"Errores: {' · '.join(result.errores[:5])}"
        )
        if len(result.errores) > 5:
            msg += f" (+{len(result.errores) - 5} más)"
        request.session["flash"] = {
            "type": "warning" if result.actualizados else "error",
            "msg": msg,
        }

    return RedirectResponse("/stock", status_code=303)


@app.get("/stock/template")
def stock_template(user: str = Depends(auth.require_user)):
    """Excel template simplificado para el upload masivo."""
    excel_bytes = stock.generate_stock_template()
    return Response(
        content=excel_bytes,
        media_type=(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ),
        headers={
            "Content-Disposition": 'attachment; filename="primi_motors_stock_template.xlsx"'
        },
    )


@app.post("/catalogo/{sku}/stock")
def catalogo_stock_update(
    request: Request,
    sku: str,
    stock_value: int = Form(..., alias="stock"),
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
):
    """
    Ajuste rápido de stock desde la vista detalle (set absoluto, +1 o -1).

    Si ML_SYNC_WRITE_ENABLED está activo y el producto está vinculado a ML,
    además del update local intenta pushear el stock nuevo a la publicación.
    El éxito/fracaso del push se concatena al flash. El cambio local NO se
    revierte si el push falla (intencional — preservamos la intención del usuario).
    """
    ok, msg = stock.update_stock(db, sku, stock_value)

    # Auto-push a ML si está habilitado y el local update fue OK
    if ok and ml_client.is_write_enabled():
        try:
            push_ok, push_msg = catalogo.push_to_ml(
                db, sku, push_stock=True, push_price=False
            )
        except Exception as e:
            push_ok = False
            push_msg = f"ML push falló: {type(e).__name__}: {e}"
        msg = f"{msg} · {push_msg}"
        # Si el local OK pero el push falló, mensaje queda warning (no error puro)
        flash_type = "success" if push_ok else "warning"
    else:
        flash_type = "success" if ok else "error"

    request.session["flash"] = {"type": flash_type, "msg": msg}
    return RedirectResponse(f"/catalogo/{sku}", status_code=303)


@app.get("/catalogo/{sku}/editar", response_class=HTMLResponse)
def catalogo_editar_form(
    request: Request,
    sku: str,
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
):
    """Form de edición de campos básicos del producto."""
    detail = catalogo.get_producto_detail(db, sku)
    if detail is None:
        request.session["flash"] = {
            "type": "error",
            "msg": f"No se encontró el producto con SKU '{sku}'.",
        }
        return RedirectResponse("/catalogo", status_code=303)
    return templates.TemplateResponse(
        request,
        "producto_editar.html",
        {
            "user": user,
            "active": "catalogo",
            "version": APP_VERSION,
            "producto": detail,
            "categorias_disponibles": catalogo.list_categorias(db),
            "marcas_disponibles": catalogo.list_marcas(db),
            "ml_write_enabled": ml_client.is_write_enabled(),
            "back_url": request.session.get("last_catalogo_url") or "/catalogo",
        },
    )


@app.post("/catalogo/{sku}/editar")
def catalogo_editar_save(
    request: Request,
    sku: str,
    titulo: str = Form(...),
    descripcion: str = Form(default=""),
    categoria: str = Form(default=""),
    marca: str = Form(default=""),
    precio_costo: str = Form(default=""),
    precio_final: str = Form(default=""),
    moneda: str = Form(default="ARS"),
    activo: str = Form(default=""),
    ml_envio_fijo: str = Form(default=""),
    ml_impuestos_pct: str = Form(default=""),
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
):
    """Guarda cambios del form. Si cambió precio y write sync activo, pushea a ML."""
    from decimal import Decimal, InvalidOperation

    def _to_dec(s: str):
        s = (s or "").strip().replace(",", ".")
        if not s:
            return None
        try:
            return Decimal(s)
        except (InvalidOperation, ValueError):
            return None

    activo_bool = activo.strip().lower() in ("on", "true", "1", "yes")

    try:
        ok, msg, cambios = catalogo.update_producto_basic(
            db,
            sku,
            titulo=titulo,
            descripcion=descripcion,
            categoria=categoria,
            marca=marca,
            precio_costo=_to_dec(precio_costo),
            precio_final=_to_dec(precio_final),
            moneda=moneda,
            activo=activo_bool,
            ml_envio_fijo=_to_dec(ml_envio_fijo),
            ml_impuestos_pct=_to_dec(ml_impuestos_pct),
            update_envio=True,
            update_impuestos=True,
        )
    except Exception as e:
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
        return RedirectResponse(f"/catalogo/{sku}/editar", status_code=303)

    if not ok:
        request.session["flash"] = {"type": "error", "msg": msg}
        return RedirectResponse(f"/catalogo/{sku}/editar", status_code=303)

    # Auto-push a ML si está habilitado y hay cambios pusheables (precio o descripción).
    push_price = "precio_final" in cambios
    push_description = "descripcion" in cambios
    if (push_price or push_description) and ml_client.is_write_enabled():
        try:
            push_ok, push_msg = catalogo.push_to_ml(
                db, sku,
                push_stock=False,
                push_price=push_price,
                push_description=push_description,
            )
        except Exception as e:
            push_ok = False
            push_msg = f"ML push falló: {type(e).__name__}: {e}"
        msg = f"{msg} · {push_msg}"
        flash_type = "success" if push_ok else "warning"
    else:
        flash_type = "success"

    request.session["flash"] = {"type": flash_type, "msg": msg}
    return RedirectResponse(f"/catalogo/{sku}", status_code=303)


@app.post("/catalogo/{sku}/ml-push")
def catalogo_ml_push(
    request: Request,
    sku: str,
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
):
    """
    Push manual del producto entero (stock + precio + descripción + atributos del
    DB local) a la publicación de ML. Útil después de updates bulk donde no
    auto-pusheamos.
    """
    try:
        ok, msg = catalogo.push_to_ml(
            db, sku,
            push_stock=True,
            push_price=True,
            push_description=True,
            push_attributes=True,
        )
    except Exception as e:
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
        return RedirectResponse(f"/catalogo/{sku}", status_code=303)

    request.session["flash"] = {
        "type": "success" if ok else "error",
        "msg": msg,
    }
    return RedirectResponse(f"/catalogo/{sku}", status_code=303)


# ===============================================================
# Precios — cambios masivos por fórmula + Excel solo precios
# ===============================================================

# Defaults para el form (usados también para repoblar después de un POST)
_PRECIOS_FORM_DEFAULTS = {
    "operacion": "porc_inc",
    "valor": "",
    "target": "final",
    "redondeo": 0,
    "search": "",
    "categoria": "",
    "marca": "",
    "vinculadas": "",
}


def _precios_render(
    request: Request,
    user: str,
    db: DbSession,
    form: dict,
    preview: Optional[dict] = None,
):
    """Helper: renderiza precios.html con form + preview opcional."""
    return templates.TemplateResponse(
        request,
        "precios.html",
        {
            "user": user,
            "active": "precios",
            "version": APP_VERSION,
            "operaciones": precios.OPERACIONES,
            "targets": precios.TARGETS,
            "redondeos": precios.REDONDEOS,
            "categorias_disponibles": catalogo.list_categorias(db),
            "marcas_disponibles": catalogo.list_marcas(db),
            "form": form,
            "preview": preview,
            "flash": request.session.pop("flash", None),
        },
    )


@app.get("/precios", response_class=HTMLResponse)
def precios_view(
    request: Request,
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
):
    """Página principal del módulo Precios — form vacío, sin preview."""
    return _precios_render(request, user, db, form=dict(_PRECIOS_FORM_DEFAULTS))


@app.post("/precios/preview", response_class=HTMLResponse)
def precios_preview(
    request: Request,
    operacion: str = Form(...),
    valor: str = Form(...),
    target: str = Form(...),
    redondeo: str = Form(default="0"),
    search: str = Form(default=""),
    categoria: str = Form(default=""),
    marca: str = Form(default=""),
    vinculadas: str = Form(default=""),
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
):
    """Calcula los cambios sin aplicarlos y muestra preview."""
    from decimal import Decimal, InvalidOperation

    form = {
        "operacion": operacion,
        "valor": valor,
        "target": target,
        "redondeo": int(redondeo) if redondeo.isdigit() else 0,
        "search": search,
        "categoria": categoria,
        "marca": marca,
        "vinculadas": vinculadas,
    }

    try:
        valor_dec = Decimal(valor.strip().replace(",", "."))
    except (InvalidOperation, ValueError, AttributeError):
        request.session["flash"] = {
            "type": "error",
            "msg": "Valor inválido — tiene que ser un número.",
        }
        return _precios_render(request, user, db, form=form)

    try:
        preview_obj = precios.compute_precio_changes(
            db,
            operacion=operacion,
            valor=valor_dec,
            target=target,
            redondeo=form["redondeo"],
            search=search,
            categoria=categoria,
            marca=marca,
            vinculadas=vinculadas,
            return_preview=True,
        )
    except Exception as e:
        import traceback
        traceback.print_exc()
        request.session["flash"] = {
            "type": "error",
            "msg": f"Error calculando preview: {type(e).__name__}: {e}",
        }
        return _precios_render(request, user, db, form=form)

    return _precios_render(request, user, db, form=form, preview=preview_obj)


@app.post("/precios/apply")
def precios_apply(
    request: Request,
    operacion: str = Form(...),
    valor: str = Form(...),
    target: str = Form(...),
    redondeo: str = Form(default="0"),
    search: str = Form(default=""),
    categoria: str = Form(default=""),
    marca: str = Form(default=""),
    vinculadas: str = Form(default=""),
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
):
    """
    Re-computa los cambios con los mismos parámetros y los aplica.
    Re-computar (vs guardar la lista del preview) garantiza coherencia
    si la DB cambió entre preview y apply (race condition mínima).
    """
    from decimal import Decimal, InvalidOperation

    try:
        valor_dec = Decimal(valor.strip().replace(",", "."))
    except (InvalidOperation, ValueError, AttributeError):
        request.session["flash"] = {
            "type": "error",
            "msg": "Valor inválido en apply.",
        }
        return RedirectResponse("/precios", status_code=303)

    redondeo_int = int(redondeo) if redondeo.isdigit() else 0

    try:
        changes = precios.compute_precio_changes(
            db,
            operacion=operacion,
            valor=valor_dec,
            target=target,
            redondeo=redondeo_int,
            search=search,
            categoria=categoria,
            marca=marca,
            vinculadas=vinculadas,
        )
        aplicados = precios.apply_precio_changes(db, changes)
    except Exception as e:
        import traceback
        traceback.print_exc()
        try:
            db.rollback()
        except Exception:
            pass
        request.session["flash"] = {
            "type": "error",
            "msg": f"Error aplicando cambios: {type(e).__name__}: {e}",
        }
        return RedirectResponse("/precios", status_code=303)

    request.session["flash"] = {
        "type": "success",
        "msg": (
            f"✓ {aplicados} producto{'' if aplicados == 1 else 's'} actualizado{'' if aplicados == 1 else 's'} "
            f"con {len(changes)} cambio{'' if len(changes) == 1 else 's'} de precio. "
            "Para sincronizar con ML, andá a /catalogo y usá ↑ Push masivo."
        ),
    }
    return RedirectResponse("/precios", status_code=303)


@app.post("/precios/upload")
async def precios_upload(
    request: Request,
    archivo: UploadFile = File(...),
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
):
    """Excel solo precios: SKU + Precio_Costo y/o Precio_Final."""
    fname = (archivo.filename or "").lower()
    if not fname.endswith((".xlsx", ".xls")):
        request.session["flash"] = {
            "type": "error",
            "msg": "El archivo debe ser .xlsx o .xls",
        }
        return RedirectResponse("/precios", status_code=303)

    try:
        file_bytes = await archivo.read()
        result = precios.process_precios_upload(db, file_bytes)
    except Exception as e:
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
        return RedirectResponse("/precios", status_code=303)

    if result.ok:
        request.session["flash"] = {
            "type": "success",
            "msg": f"✓ {result.actualizados} producto{'' if result.actualizados == 1 else 's'} actualizado{'' if result.actualizados == 1 else 's'}.",
        }
    else:
        msg = (
            f"Procesado con errores. Actualizados: {result.actualizados}. "
            f"{len(result.errores)} errores: " + " · ".join(result.errores[:5])
        )
        if len(result.errores) > 5:
            msg += f" (+{len(result.errores) - 5} más)"
        request.session["flash"] = {
            "type": "warning" if result.actualizados else "error",
            "msg": msg,
        }
    return RedirectResponse("/precios", status_code=303)


@app.get("/precios/template")
def precios_template(user: str = Depends(auth.require_user)):
    """Excel template del módulo Precios."""
    excel_bytes = precios.generate_precios_template()
    return Response(
        content=excel_bytes,
        media_type=(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ),
        headers={
            "Content-Disposition": 'attachment; filename="primi_motors_precios_template.xlsx"'
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
    # "stock" ya no es stub — vive en app/stock.py + rutas dedicadas
    # "precios" ya no es stub — vive en app/precios.py + rutas dedicadas
    ("publicaciones", "Publicaciones ML",
     "Estado de los ítems publicados en Mercado Libre — pausar, "
     "republicar y ver estadísticas de cada uno."),
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
