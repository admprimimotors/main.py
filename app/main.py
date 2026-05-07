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
from sqlalchemy import select as select_
from sqlalchemy.orm import Session as DbSession
from starlette.middleware.sessions import SessionMiddleware

from . import (
    auth,
    catalogo,
    clientes,
    database,
    ml_client,
    ml_publisher,
    notas_credito,
    precios,
    remitos,
    stock,
    storage,
)
from .database import get_db

APP_NAME = "Primi Motors — Backend"
APP_VERSION = "0.27.1"

# Raíz del paquete app/
BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

# Helpers globales para que los templates puedan llamarlos directo
# (sin tener que pasarlos en cada context).
templates.env.globals["format_cuit"] = clientes.format_cuit_display

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


# ---------------------------------------------------------------
# Editor de ficha técnica (CRUD completo sobre el JSONB)
# ---------------------------------------------------------------

@app.get("/catalogo/{sku}/ficha", response_class=HTMLResponse)
def catalogo_ficha_form(
    request: Request,
    sku: str,
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
):
    """Form de edición de la ficha técnica (atributos JSONB del producto)."""
    prod = db.execute(
        select_(catalogo.Producto).where(catalogo.Producto.sku == sku)
    ).scalar_one_or_none()
    if prod is None:
        request.session["flash"] = {
            "type": "error",
            "msg": f"No se encontró el producto con SKU '{sku}'.",
        }
        return RedirectResponse("/catalogo", status_code=303)

    ml_keys = catalogo.keys_linkeadas_a_ml(prod)
    ficha = prod.ficha_tecnica or {}
    # Orden estable: primero las linkeadas a ML (que son las "importantes"),
    # luego las locales, ambas alfabéticas dentro de cada grupo.
    entries = [
        {"key": k, "value": v if v is not None else "", "is_ml": k in ml_keys}
        for k, v in sorted(ficha.items(), key=lambda kv: (kv[0] not in ml_keys, kv[0]))
    ]
    flash = request.session.pop("flash", None)
    return templates.TemplateResponse(
        request,
        "producto_ficha_editar.html",
        {
            "user": user,
            "active": "catalogo",
            "version": APP_VERSION,
            "producto": {
                "sku": prod.sku,
                "titulo": prod.titulo,
            },
            "entries": entries,
            "ml_write_enabled": ml_client.is_write_enabled(),
            "flash": flash,
        },
    )


@app.post("/catalogo/{sku}/ficha")
async def catalogo_ficha_save(
    request: Request,
    sku: str,
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
):
    """
    Guarda la ficha técnica completa. Parsea pares paralelos
    `ficha_key[]` / `ficha_value[]`, normaliza las keys, descarta vacíos,
    y persiste. Si hay write-sync ML activo y alguna key cambiada está
    linkeada a un atributo ML, hace push de atributos.
    """
    form = await request.form()
    keys = form.getlist("ficha_key")
    values = form.getlist("ficha_value")

    # Construir dict normalizado descartando vacíos
    nueva_ficha: dict[str, str] = {}
    for raw_k, raw_v in zip(keys, values):
        norm = catalogo._norm_attr_key(raw_k or "")
        val = (raw_v or "").strip()
        if not norm or not val:
            continue
        # La última ocurrencia gana en caso de duplicado tras normalizar
        nueva_ficha[norm] = val

    # Necesitamos detectar las keys ML-linkeadas ANTES de guardar
    prod = db.execute(
        select_(catalogo.Producto).where(catalogo.Producto.sku == sku)
    ).scalar_one_or_none()
    if prod is None:
        request.session["flash"] = {
            "type": "error",
            "msg": f"No existe el SKU '{sku}'.",
        }
        return RedirectResponse("/catalogo", status_code=303)
    ml_keys = catalogo.keys_linkeadas_a_ml(prod)

    try:
        ok, msg, cambios = catalogo.update_ficha_tecnica(db, sku, nueva_ficha)
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
        return RedirectResponse(f"/catalogo/{sku}/ficha", status_code=303)

    if not ok:
        request.session["flash"] = {"type": "error", "msg": msg}
        return RedirectResponse(f"/catalogo/{sku}/ficha", status_code=303)

    # Push a ML si hay atributos linkeados que cambiaron
    push_attrs = bool(cambios & ml_keys)
    if push_attrs and ml_client.is_write_enabled():
        try:
            push_ok, push_msg = catalogo.push_to_ml(
                db, sku,
                push_stock=False,
                push_price=False,
                push_description=False,
                push_attributes=True,
            )
        except Exception as e:
            push_ok = False
            push_msg = f"ML push falló: {type(e).__name__}: {e}"
        msg = f"{msg} · {push_msg}"
        flash_type = "success" if push_ok else "warning"
    elif push_attrs and not ml_client.is_write_enabled():
        msg = f"{msg} · ⚠ Hay {len(cambios & ml_keys)} atributo(s) ML modificado(s) que no se pushearon (ML_WRITE_ENABLED=false)."
        flash_type = "warning"
    else:
        flash_type = "success"

    request.session["flash"] = {"type": flash_type, "msg": msg}
    return RedirectResponse(f"/catalogo/{sku}", status_code=303)


# ===============================================================
# Publicación de productos NUEVOS a Mercado Libre (POST /items)
# ===============================================================

@app.get("/api/ml/category-search")
def api_ml_category_search(
    q: str = "",
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
):
    """
    Buscador de categorías ML para el form de publicar.
    Estrategia:
      1. domain_discovery (sugiere categorías exactas para "qué categoría es esto")
      2. Si vacío: search de items + agrupar category_id por frecuencia
      3. Para cada category_id resultante, traer name + path_from_root para
         que el dropdown muestre "Accesorios › ... › Camisas de Motor".
    """
    query = (q or "").strip()
    if len(query) < 2:
        return JSONResponse({"results": []})

    # Paso 1: domain_discovery
    candidates = ml_client.domain_discovery_search(db, query, limit=8)
    seen_cats: dict[str, dict] = {}
    for c in candidates:
        cat_id = c.get("category_id")
        if not cat_id:
            continue
        if cat_id not in seen_cats:
            seen_cats[cat_id] = {
                "category_id": cat_id,
                "category_name": c.get("category_name") or "",
                "domain_name": c.get("domain_name") or "",
                "votes": 0,
                "source": "discovery",
            }

    # Paso 2: si discovery no trajo nada, fallback a search de items
    if not seen_cats:
        items = ml_client.search_items(db, query, limit=15)
        for item in items:
            cat_id = item.get("category_id")
            if not cat_id:
                continue
            if cat_id not in seen_cats:
                seen_cats[cat_id] = {
                    "category_id": cat_id,
                    "category_name": "",
                    "domain_name": "",
                    "votes": 0,
                    "source": "search",
                }
            seen_cats[cat_id]["votes"] += 1

    # Paso 3: enriquecer con name + path_from_root para cada categoría
    enriched = []
    for cat_id, info in seen_cats.items():
        cat_full = ml_client.get_category(db, cat_id) or {}
        name = info["category_name"] or cat_full.get("name") or cat_id
        path = cat_full.get("path_from_root") or []
        path_str = " › ".join(p.get("name", "") for p in path) if path else ""
        enriched.append({
            "category_id": cat_id,
            "name": name,
            "path": path_str,
            "domain_name": info.get("domain_name", ""),
            "votes": info["votes"],
            "source": info["source"],
        })

    # Orden: votes desc (los más frecuentes en search arriba), luego nombre
    enriched.sort(key=lambda x: (-x["votes"], x["name"]))

    return JSONResponse({"results": enriched[:10]})



@app.get("/catalogo/{sku}/publicar", response_class=HTMLResponse)
def catalogo_publicar_form(
    request: Request,
    sku: str,
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
):
    """
    Preflight de publicación: muestra categoría predicha + alternativas,
    estado de readiness (qué falta), atributos requeridos por la categoría.
    """
    prod = db.execute(
        select_(catalogo.Producto).where(catalogo.Producto.sku == sku)
    ).scalar_one_or_none()
    if prod is None:
        request.session["flash"] = {
            "type": "error",
            "msg": f"No se encontró el producto con SKU '{sku}'.",
        }
        return RedirectResponse("/catalogo", status_code=303)

    if prod.ml_item_id:
        request.session["flash"] = {
            "type": "warning",
            "msg": (
                f"Este producto ya está publicado en ML como {prod.ml_item_id}. "
                "Para actualizarlo usá Push a ML."
            ),
        }
        return RedirectResponse(f"/catalogo/{sku}", status_code=303)

    # Resolver categoría: mapping guardado, o predicción del título
    ml_cat_id, ml_cat_name, candidatos = ml_publisher.get_or_predict_ml_category(
        db,
        nuestra_categoria=prod.categoria,
        titulo=prod.titulo or "",
    )

    cat_attrs = ml_client.get_category_attributes(db, ml_cat_id) if ml_cat_id else []
    req_attrs = ml_publisher.required_attributes(cat_attrs)
    problems = ml_publisher.validate_ready(
        prod, ml_category_id=ml_cat_id, required_attrs=req_attrs
    )

    flash = request.session.pop("flash", None)
    return templates.TemplateResponse(
        request,
        "producto_publicar.html",
        {
            "user": user,
            "active": "catalogo",
            "version": APP_VERSION,
            "producto": prod,
            "ml_category_id": ml_cat_id,
            "ml_category_name": ml_cat_name,
            "candidatos": candidatos,
            "required_attrs": req_attrs,
            "problems": problems,
            "ready_to_publish": len(problems) == 0,
            "ml_write_enabled": ml_client.is_write_enabled(),
            "default_listing_type": ml_publisher.DEFAULT_LISTING_TYPE,
            "default_initial_status": ml_publisher.DEFAULT_INITIAL_STATUS,
            "free_shipping_min": ml_publisher.FREE_SHIPPING_MIN,
            "flex_enabled": ml_publisher.FLEX_ENABLED,
            "flash": flash,
        },
    )


@app.post("/catalogo/{sku}/publicar")
def catalogo_publicar_save(
    request: Request,
    sku: str,
    ml_category_id: str = Form(default=""),
    confirmar_categoria: str = Form(default=""),
    listing_type_id: str = Form(default=""),
    initial_status: str = Form(default=""),
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
):
    """
    Crea la publicación. Si el usuario eligió/confirmó una categoría distinta a
    la predicha, la guardamos en `categoria_ml_mapping` para futuros productos
    de la misma categoría interna.
    """
    cat_id = (ml_category_id or "").strip() or None
    listing = (listing_type_id or "").strip() or None
    status = (initial_status or "").strip() or None

    # Si pidieron confirmar el mapping, lo grabamos antes de publicar
    if cat_id and confirmar_categoria.lower() in ("on", "true", "1", "yes"):
        prod = db.execute(
            select_(catalogo.Producto).where(catalogo.Producto.sku == sku)
        ).scalar_one_or_none()
        if prod and prod.categoria:
            try:
                # Buscamos el nombre de categoría para guardar legible
                cat_info = ml_client.get_category(db, cat_id)
                ml_publisher.confirm_categoria_mapping(
                    db,
                    nuestra_categoria=prod.categoria,
                    ml_category_id=cat_id,
                    ml_category_name=(cat_info or {}).get("name"),
                    confirmado=True,
                )
            except Exception:
                pass  # No bloqueamos la publicación si el cache falla

    try:
        ok, msg, item_id = ml_publisher.create_publication(
            db, sku,
            ml_category_id_override=cat_id,
            listing_type_id=listing,
            initial_status=status,
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
        return RedirectResponse(f"/catalogo/{sku}/publicar", status_code=303)

    request.session["flash"] = {
        "type": "success" if ok else "error",
        "msg": msg,
    }
    if ok:
        return RedirectResponse(f"/catalogo/{sku}", status_code=303)
    return RedirectResponse(f"/catalogo/{sku}/publicar", status_code=303)


# --- Vista masiva (top-level path para no chocar con /catalogo/{sku}) ---

@app.get("/publicar", response_class=HTMLResponse)
def publicar_masivo_form(
    request: Request,
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
):
    """
    Lista de productos sin publicar en ML, con preflight de readiness para
    cada uno. Permite publicar individualmente o masivo.
    """
    productos = ml_publisher.get_publishable_products(db, limit=500)

    # Para el listado masivo solo chequeamos lo barato (campos básicos + foto).
    # El check de categoría/atributos requeridos se hace en el preflight individual
    # — sería costoso pegarle a ML por cada producto acá.
    rows = []
    for prod in productos:
        problems = []
        if not (prod.titulo or "").strip():
            problems.append("título")
        if prod.precio_final is None or prod.precio_final <= 0:
            problems.append("precio")
        if (prod.stock_actual or 0) <= 0:
            problems.append("stock")
        if not prod.fotos:
            problems.append("fotos")
        rows.append({
            "sku": prod.sku,
            "titulo": prod.titulo,
            "categoria": prod.categoria,
            "marca": prod.marca,
            "precio": prod.precio_final,
            "stock": prod.stock_actual,
            "foto_url": prod.fotos[0].url if prod.fotos else None,
            "problems_quick": problems,
            "ready_quick": len(problems) == 0,
        })

    flash = request.session.pop("flash", None)
    return templates.TemplateResponse(
        request,
        "publicar_masivo.html",
        {
            "user": user,
            "active": "publicar",
            "version": APP_VERSION,
            "rows": rows,
            "total": len(rows),
            "ml_write_enabled": ml_client.is_write_enabled(),
            "flash": flash,
        },
    )


@app.post("/publicar")
async def publicar_masivo_run(
    request: Request,
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
):
    """
    Ejecuta la publicación masiva sobre los SKUs marcados con checkbox.
    Body: ficha_sku[]=...
    """
    form = await request.form()
    skus = [s.strip() for s in form.getlist("sku") if (s or "").strip()]
    if not skus:
        request.session["flash"] = {
            "type": "warning",
            "msg": "No seleccionaste ningún producto.",
        }
        return RedirectResponse("/publicar", status_code=303)

    try:
        summary = ml_publisher.bulk_create(db, skus, dry_run=False)
    except Exception as e:
        import traceback
        traceback.print_exc()
        try:
            db.rollback()
        except Exception:
            pass
        request.session["flash"] = {
            "type": "error",
            "msg": f"Error inesperado en publicación masiva: {type(e).__name__}: {e}",
        }
        return RedirectResponse("/publicar", status_code=303)

    n_ok = len(summary["ok"])
    n_fail = len(summary["fail"])
    n_skip = len(summary["skipped"])
    if n_ok and not n_fail:
        flash_type = "success"
    elif n_ok:
        flash_type = "warning"
    else:
        flash_type = "error"

    parts = [f"{n_ok} publicado{'s' if n_ok != 1 else ''}"]
    if n_fail:
        parts.append(f"{n_fail} con error")
    if n_skip:
        parts.append(f"{n_skip} salteado{'s' if n_skip != 1 else ''}")

    msg = "Publicación masiva: " + " · ".join(parts) + "."
    if n_fail:
        # Mostramos los primeros 3 errores para no inundar el flash
        errores_breve = "; ".join(
            f"{f['sku']}: {f['msg'][:80]}" for f in summary["fail"][:3]
        )
        msg += f" Errores: {errores_breve}"
        if n_fail > 3:
            msg += f" (+ {n_fail - 3} más)"

    request.session["flash"] = {"type": flash_type, "msg": msg}
    return RedirectResponse("/publicar", status_code=303)


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
# Clientes — listado + CRUD + upload masivo
# ===============================================================
# IMPORTANTE: las rutas estáticas (/nuevo, /upload, /template) van ANTES que
# /{cliente_id:int} para que FastAPI las matchee primero.

@app.get("/clientes", response_class=HTMLResponse)
def clientes_view(
    request: Request,
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
    q: str = "",
    provincia: str = "",
    condicion_iva: str = "",
    incluir_archivados: str = "",
    page: int = 1,
):
    """Listado paginado de clientes con filtros."""
    incluir = incluir_archivados.strip().lower() in ("on", "1", "true", "yes")
    cli_list, total = clientes.list_clientes(
        db,
        search=q,
        provincia=provincia,
        condicion_iva=condicion_iva,
        incluir_archivados=incluir,
        page=page,
    )
    flash = request.session.pop("flash", None)
    # Guardar URL para back navigation desde detalle/editar
    relative_url = request.url.path
    if request.url.query:
        relative_url += "?" + request.url.query
    request.session["last_clientes_url"] = relative_url

    return templates.TemplateResponse(
        request,
        "clientes.html",
        {
            "user": user,
            "active": "clientes",
            "version": APP_VERSION,
            "clientes": cli_list,
            "total": total,
            "search": q,
            "provincia": provincia,
            "condicion_iva": condicion_iva,
            "incluir_archivados": incluir,
            "page": page,
            "page_size": clientes.PAGE_SIZE,
            "flash": flash,
            "provincias_disponibles": clientes.list_provincias(db),
            "condiciones_iva": clientes.CONDICIONES_IVA,
        },
    )


@app.get("/clientes/nuevo", response_class=HTMLResponse)
def clientes_nuevo_form(
    request: Request,
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
):
    """Form para crear un nuevo cliente."""
    flash = request.session.pop("flash", None)
    # Si hay form data preservada (después de un error), usarla
    form = request.session.pop("cliente_form_draft", None) or {"activo": True}
    return templates.TemplateResponse(
        request,
        "cliente_editar.html",
        {
            "user": user,
            "active": "clientes",
            "version": APP_VERSION,
            "cliente": None,
            "form": form,
            "flash": flash,
            "condiciones_iva": clientes.CONDICIONES_IVA,
            "provincias_ar": clientes.PROVINCIAS_AR,
            "provincias_disponibles": clientes.list_provincias(db),
            "localidades_disponibles": [],  # vacío en form de creación
        },
    )


@app.post("/clientes/nuevo")
def clientes_nuevo_save(
    request: Request,
    razon_social: str = Form(...),
    nombre_comercial: str = Form(default=""),
    cuit_dni: str = Form(default=""),
    condicion_iva: str = Form(default=""),
    direccion: str = Form(default=""),
    localidad: str = Form(default=""),
    provincia: str = Form(default=""),
    codigo_postal: str = Form(default=""),
    telefono: str = Form(default=""),
    email: str = Form(default=""),
    notas: str = Form(default=""),
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
):
    """Crea un cliente nuevo."""
    cli, msg = clientes.create_cliente(
        db,
        razon_social=razon_social,
        nombre_comercial=nombre_comercial,
        cuit_dni=cuit_dni,
        condicion_iva=condicion_iva,
        direccion=direccion,
        localidad=localidad,
        provincia=provincia,
        codigo_postal=codigo_postal,
        telefono=telefono,
        email=email,
        notas=notas,
        activo=True,
    )
    if cli is None:
        # Error: preservar form data y volver al form
        request.session["cliente_form_draft"] = {
            "razon_social": razon_social,
            "nombre_comercial": nombre_comercial,
            "cuit_dni": cuit_dni,
            "condicion_iva": condicion_iva,
            "direccion": direccion,
            "localidad": localidad,
            "provincia": provincia,
            "codigo_postal": codigo_postal,
            "telefono": telefono,
            "email": email,
            "notas": notas,
            "activo": True,
        }
        request.session["flash"] = {"type": "error", "msg": msg}
        return RedirectResponse("/clientes/nuevo", status_code=303)
    request.session["flash"] = {"type": "success", "msg": msg}
    return RedirectResponse(f"/clientes/{cli.id}", status_code=303)


@app.post("/clientes/upload")
async def clientes_upload(
    request: Request,
    archivo: UploadFile = File(...),
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
):
    """Excel/CSV masivo de clientes."""
    fname = (archivo.filename or "").lower()
    if not fname.endswith((".xlsx", ".xls")):
        request.session["flash"] = {
            "type": "error",
            "msg": "El archivo debe ser .xlsx o .xls",
        }
        return RedirectResponse("/clientes", status_code=303)

    try:
        file_bytes = await archivo.read()
        result = clientes.process_clientes_upload(db, file_bytes)
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
        return RedirectResponse("/clientes", status_code=303)

    parts = []
    if result.creados:
        parts.append(f"{result.creados} creados")
    if result.actualizados:
        parts.append(f"{result.actualizados} actualizados")
    if result.sin_cambios:
        parts.append(f"{result.sin_cambios} sin cambios")
    summary = " · ".join(parts) if parts else "ningún cambio"

    if result.ok:
        request.session["flash"] = {"type": "success", "msg": f"✓ Upload OK — {summary}."}
    else:
        msg = (
            f"Procesado con errores — {summary}. "
            f"{len(result.errores)} errores: " + " · ".join(result.errores[:5])
        )
        if len(result.errores) > 5:
            msg += f" (+{len(result.errores) - 5} más)"
        request.session["flash"] = {
            "type": "warning" if (result.creados or result.actualizados) else "error",
            "msg": msg,
        }
    return RedirectResponse("/clientes", status_code=303)


@app.get("/clientes/template")
def clientes_template(user: str = Depends(auth.require_user)):
    """Excel template de clientes."""
    excel_bytes = clientes.generate_clientes_template()
    return Response(
        content=excel_bytes,
        media_type=(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ),
        headers={
            "Content-Disposition": 'attachment; filename="primi_motors_clientes_template.xlsx"'
        },
    )


# Rutas con {cliente_id} — DESPUÉS de las estáticas. El converter :int garantiza
# que /clientes/nuevo, /clientes/upload, etc. no caigan acá por confusión.

@app.get("/clientes/{cliente_id:int}", response_class=HTMLResponse)
def cliente_detail(
    request: Request,
    cliente_id: int,
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
):
    """Detalle de un cliente."""
    cli = clientes.get_cliente(db, cliente_id)
    if cli is None:
        request.session["flash"] = {
            "type": "error",
            "msg": f"No existe cliente ID {cliente_id}.",
        }
        return RedirectResponse("/clientes", status_code=303)
    flash = request.session.pop("flash", None)
    back_url = request.session.get("last_clientes_url") or "/clientes"
    return templates.TemplateResponse(
        request,
        "cliente.html",
        {
            "user": user,
            "active": "clientes",
            "version": APP_VERSION,
            "cliente": cli,
            "flash": flash,
            "back_url": back_url,
        },
    )


@app.get("/clientes/{cliente_id:int}/editar", response_class=HTMLResponse)
def cliente_editar_form(
    request: Request,
    cliente_id: int,
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
):
    """Form para editar un cliente existente."""
    try:
        cli = clientes.get_cliente(db, cliente_id)
        if cli is None:
            request.session["flash"] = {
                "type": "error",
                "msg": f"No existe cliente ID {cliente_id}.",
            }
            return RedirectResponse("/clientes", status_code=303)
        flash = request.session.pop("flash", None)
        form = request.session.pop("cliente_form_draft", None) or {
            "razon_social": cli.razon_social,
            "nombre_comercial": cli.nombre_comercial,
            "cuit_dni": clientes.format_cuit_display(cli.cuit_dni) if cli.cuit_dni else "",
            "condicion_iva": cli.condicion_iva,
            "direccion": cli.direccion,
            "localidad": cli.localidad,
            "provincia": cli.provincia,
            "codigo_postal": cli.codigo_postal,
            "telefono": cli.telefono,
            "email": cli.email,
            "notas": cli.notas,
            "activo": cli.activo,
        }
        return templates.TemplateResponse(
            request,
            "cliente_editar.html",
            {
                "user": user,
                "active": "clientes",
                "version": APP_VERSION,
                "cliente": cli,
                "form": form,
                "flash": flash,
                "condiciones_iva": clientes.CONDICIONES_IVA,
                "provincias_ar": clientes.PROVINCIAS_AR,
                "provincias_disponibles": clientes.list_provincias(db),
                "localidades_disponibles": [],
            },
        )
    except Exception as e:
        # Loguear traceback completo a stderr (Render captura)
        import traceback
        traceback.print_exc()
        # Mostrar el error específico al usuario en vez de un 500 mudo
        request.session["flash"] = {
            "type": "error",
            "msg": f"Error al abrir el editor del cliente: {type(e).__name__}: {e}",
        }
        return RedirectResponse(f"/clientes/{cliente_id}", status_code=303)


@app.post("/clientes/{cliente_id:int}/editar")
def cliente_editar_save(
    request: Request,
    cliente_id: int,
    razon_social: str = Form(...),
    nombre_comercial: str = Form(default=""),
    cuit_dni: str = Form(default=""),
    condicion_iva: str = Form(default=""),
    direccion: str = Form(default=""),
    localidad: str = Form(default=""),
    provincia: str = Form(default=""),
    codigo_postal: str = Form(default=""),
    telefono: str = Form(default=""),
    email: str = Form(default=""),
    notas: str = Form(default=""),
    activo: str = Form(default=""),
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
):
    """Guarda cambios en un cliente."""
    activo_bool = activo.strip().lower() in ("on", "true", "1", "yes")
    ok, msg = clientes.update_cliente(
        db, cliente_id,
        razon_social=razon_social,
        nombre_comercial=nombre_comercial,
        cuit_dni=cuit_dni,
        condicion_iva=condicion_iva,
        direccion=direccion,
        localidad=localidad,
        provincia=provincia,
        codigo_postal=codigo_postal,
        telefono=telefono,
        email=email,
        notas=notas,
        activo=activo_bool,
    )
    request.session["flash"] = {"type": "success" if ok else "error", "msg": msg}
    if not ok:
        return RedirectResponse(f"/clientes/{cliente_id}/editar", status_code=303)
    return RedirectResponse(f"/clientes/{cliente_id}", status_code=303)


@app.post("/clientes/{cliente_id:int}/archivar")
def cliente_archivar(
    request: Request,
    cliente_id: int,
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
):
    ok, msg = clientes.archivar_cliente(db, cliente_id)
    request.session["flash"] = {"type": "success" if ok else "error", "msg": msg}
    return RedirectResponse(f"/clientes/{cliente_id}", status_code=303)


@app.post("/clientes/{cliente_id:int}/reactivar")
def cliente_reactivar(
    request: Request,
    cliente_id: int,
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
):
    ok, msg = clientes.reactivar_cliente(db, cliente_id)
    request.session["flash"] = {"type": "success" if ok else "error", "msg": msg}
    return RedirectResponse(f"/clientes/{cliente_id}", status_code=303)


@app.post("/clientes/{cliente_id:int}/eliminar")
def cliente_eliminar(
    request: Request,
    cliente_id: int,
    confirmacion: str = Form(default=""),
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
):
    """
    DELETE definitivo del cliente. Requiere que el usuario haya tipeado
    el texto de confirmación en el form (defensa contra clicks accidentales).
    """
    if confirmacion.strip().upper() != "ELIMINAR":
        request.session["flash"] = {
            "type": "error",
            "msg": "Para eliminar definitivamente tenés que escribir ELIMINAR en el campo de confirmación.",
        }
        return RedirectResponse(f"/clientes/{cliente_id}", status_code=303)
    ok, msg = clientes.eliminar_cliente(db, cliente_id)
    request.session["flash"] = {"type": "success" if ok else "error", "msg": msg}
    return RedirectResponse("/clientes", status_code=303)


# ===============================================================
# API JSON para autocomplete de productos en formularios de remitos/NC
# ===============================================================

@app.get("/api/productos/lookup")
def api_producto_lookup(
    sku: str,
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
):
    """Lookup por SKU exacto. Devuelve datos básicos para autocompletar forms."""
    from .models import Producto
    prod = db.execute(
        select_(Producto).where(Producto.sku == sku.strip())
    ).scalar_one_or_none() if sku.strip() else None
    if prod is None:
        return JSONResponse({"error": "not_found"}, status_code=404)
    return JSONResponse({
        "id": prod.id,
        "sku": prod.sku,
        "titulo": prod.titulo,
        "precio_final": float(prod.precio_final) if prod.precio_final is not None else 0,
        "stock_actual": prod.stock_actual,
    })


# ===============================================================
# Helpers compartidos por remitos y NC
# ===============================================================

def _parse_items_from_form(
    item_sku: list[str],
    item_descripcion: list[str],
    item_cantidad: list[str],
    item_precio: list[str],
    item_descuento: list[str],
) -> list[remitos.ItemInput]:
    """Convierte las listas paralelas del form en una lista de ItemInput."""
    items: list[remitos.ItemInput] = []
    n = max(
        len(item_sku), len(item_descripcion),
        len(item_cantidad), len(item_precio), len(item_descuento),
    )
    for i in range(n):
        desc = (item_descripcion[i] if i < len(item_descripcion) else "").strip()
        if not desc:
            continue  # skip filas vacías
        try:
            cant = int(item_cantidad[i]) if i < len(item_cantidad) else 0
        except (ValueError, TypeError):
            cant = 0
        if cant <= 0:
            continue
        from decimal import Decimal as _D
        try:
            precio = _D(str(item_precio[i] if i < len(item_precio) else "0").replace(",", "."))
        except Exception:
            precio = _D("0")
        try:
            desc_pc = _D(str(item_descuento[i] if i < len(item_descuento) else "0").replace(",", "."))
        except Exception:
            desc_pc = _D("0")
        sku = (item_sku[i] if i < len(item_sku) else "").strip() or None
        items.append(remitos.ItemInput(
            descripcion=desc,
            cantidad=cant,
            precio_unitario=precio,
            descuento_porc=desc_pc,
            sku=sku,
        ))
    return items


def _list_clientes_for_form(db) -> list:
    """Lista todos los clientes activos, ordenados por razón social, para los selects."""
    cli_list, _ = clientes.list_clientes(db, page=1, incluir_archivados=False)
    # Si hay más de 50, traer todos sin paginar
    from .models import Cliente as _Cli
    return list(db.execute(
        select_(_Cli).where(_Cli.activo == True).order_by(_Cli.razon_social.asc())  # noqa: E712
    ).scalars().all())


# ===============================================================
# Remitos
# ===============================================================

@app.get("/remitos", response_class=HTMLResponse)
def remitos_view(
    request: Request,
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
    q: str = "",
    estado: str = "",
    page: int = 1,
):
    try:
        rem_list, total = remitos.list_remitos(db, search=q, estado=estado, page=page)
        flash = request.session.pop("flash", None)
        return templates.TemplateResponse(
            request,
            "remitos.html",
            {
                "user": user,
                "active": "remitos",
                "version": APP_VERSION,
                "remitos": rem_list,
                "total": total,
                "search": q,
                "estado": estado,
                "page": page,
                "page_size": remitos.PAGE_SIZE,
                "flash": flash,
            },
        )
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        traceback.print_exc()
        return HTMLResponse(
            f"<pre style='padding:20px;background:#0b0b0b;color:#fca5a5;font-family:monospace;white-space:pre-wrap;'>"
            f"Error en /remitos (v{APP_VERSION}):\n\n"
            f"{type(e).__name__}: {e}\n\n{tb}</pre>",
            status_code=500,
        )


@app.get("/remitos/nuevo", response_class=HTMLResponse)
def remitos_nuevo_form(
    request: Request,
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
):
    flash = request.session.pop("flash", None)
    form = request.session.pop("remito_form_draft", None) or {}
    return templates.TemplateResponse(
        request,
        "remito_nuevo.html",
        {
            "user": user,
            "active": "remitos",
            "version": APP_VERSION,
            "clientes": _list_clientes_for_form(db),
            "proximo_numero": remitos.next_remito_numero(db),
            "form": form,
            "flash": flash,
        },
    )


@app.post("/remitos/nuevo")
def remitos_nuevo_save(
    request: Request,
    cliente_id: int = Form(...),
    condicion_venta: str = Form(default=""),
    forma_pago: str = Form(default=""),
    descuento_general: str = Form(default="0"),
    observaciones: str = Form(default=""),
    item_sku: list[str] = Form(default=[]),
    item_descripcion: list[str] = Form(default=[]),
    item_cantidad: list[str] = Form(default=[]),
    item_precio: list[str] = Form(default=[]),
    item_descuento: list[str] = Form(default=[]),
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
):
    items = _parse_items_from_form(
        item_sku, item_descripcion, item_cantidad, item_precio, item_descuento
    )
    if not items:
        request.session["flash"] = {
            "type": "error",
            "msg": "El remito necesita al menos un item con descripción y cantidad > 0.",
        }
        return RedirectResponse("/remitos/nuevo", status_code=303)

    from decimal import Decimal as _D
    try:
        desc_gen_dec = _D(str(descuento_general).replace(",", "."))
    except Exception:
        desc_gen_dec = _D("0")

    try:
        remito = remitos.crear_remito(
            db, cliente_id, items,
            condicion_venta=condicion_venta,
            forma_pago=forma_pago,
            descuento_general=desc_gen_dec,
            observaciones=observaciones,
        )
    except remitos.StockInsuficienteError as e:
        request.session["flash"] = {
            "type": "error",
            "msg": f"Stock insuficiente para SKU {e.sku}: hay {e.disponible}, se piden {e.pedido}. Ajustá la cantidad o el SKU.",
        }
        return RedirectResponse("/remitos/nuevo", status_code=303)
    except remitos.RemitoError as e:
        request.session["flash"] = {"type": "error", "msg": str(e)}
        return RedirectResponse("/remitos/nuevo", status_code=303)
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
        return RedirectResponse("/remitos/nuevo", status_code=303)

    request.session["flash"] = {
        "type": "success",
        "msg": f"✓ Remito {remito.numero} creado. Stock descontado correctamente.",
    }
    return RedirectResponse(f"/remitos/{remito.id}", status_code=303)


@app.get("/remitos/{remito_id:int}", response_class=HTMLResponse)
def remito_detail(
    request: Request,
    remito_id: int,
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
):
    remito = remitos.get_remito(db, remito_id)
    if remito is None:
        request.session["flash"] = {
            "type": "error",
            "msg": f"No existe el remito ID {remito_id}.",
        }
        return RedirectResponse("/remitos", status_code=303)
    flash = request.session.pop("flash", None)
    return templates.TemplateResponse(
        request,
        "remito.html",
        {
            "user": user,
            "active": "remitos",
            "version": APP_VERSION,
            "remito": remito,
            "flash": flash,
        },
    )


@app.post("/remitos/{remito_id:int}/anular")
def remito_anular(
    request: Request,
    remito_id: int,
    motivo: str = Form(default=""),
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
):
    """Anular remito → genera NC automática con los mismos items + redirige a la NC."""
    try:
        ok, msg, nc_id = remitos.anular_remito(db, remito_id, motivo=motivo)
    except Exception as e:
        import traceback
        traceback.print_exc()
        try:
            db.rollback()
        except Exception:
            pass
        request.session["flash"] = {
            "type": "error",
            "msg": f"Error al anular: {type(e).__name__}: {e}",
        }
        return RedirectResponse(f"/remitos/{remito_id}", status_code=303)

    request.session["flash"] = {"type": "success" if ok else "error", "msg": msg}
    if ok and nc_id:
        return RedirectResponse(f"/notas-credito/{nc_id}", status_code=303)
    return RedirectResponse(f"/remitos/{remito_id}", status_code=303)


@app.get("/remitos/{remito_id:int}/pdf")
def remito_pdf(
    remito_id: int,
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
):
    """Devuelve el PDF del remito como descarga inline."""
    from . import pdf_generator
    remito = remitos.get_remito(db, remito_id)
    if remito is None:
        return JSONResponse({"error": "not_found"}, status_code=404)
    pdf_bytes = pdf_generator.generate_remito_pdf(remito)
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={
            "Content-Disposition": f'inline; filename="remito_{remito.numero:07d}.pdf"',
        },
    )


@app.get("/notas-credito/{nc_id:int}/pdf")
def nc_pdf(
    nc_id: int,
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
):
    """Devuelve el PDF de la NC como descarga inline."""
    from . import pdf_generator
    nc = notas_credito.get_nc(db, nc_id)
    if nc is None:
        return JSONResponse({"error": "not_found"}, status_code=404)
    pdf_bytes = pdf_generator.generate_nc_pdf(nc)
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={
            "Content-Disposition": f'inline; filename="nc_{nc.numero:07d}.pdf"',
        },
    )


# ===============================================================
# Notas de Crédito
# ===============================================================

@app.get("/notas-credito", response_class=HTMLResponse)
def ncs_view(
    request: Request,
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
    q: str = "",
    estado: str = "",
    page: int = 1,
):
    try:
        nc_list, total = notas_credito.list_ncs(db, search=q, estado=estado, page=page)
        flash = request.session.pop("flash", None)
        return templates.TemplateResponse(
            request,
            "notas_credito.html",
            {
                "user": user,
                "active": "notas_credito",
                "version": APP_VERSION,
                "ncs": nc_list,
                "total": total,
                "search": q,
                "estado": estado,
                "page": page,
                "page_size": notas_credito.PAGE_SIZE,
                "flash": flash,
            },
        )
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        traceback.print_exc()
        return HTMLResponse(
            f"<pre style='padding:20px;background:#0b0b0b;color:#fca5a5;font-family:monospace;white-space:pre-wrap;'>"
            f"Error en /notas-credito (v{APP_VERSION}):\n\n"
            f"{type(e).__name__}: {e}\n\n{tb}</pre>",
            status_code=500,
        )


@app.get("/notas-credito/nuevo", response_class=HTMLResponse)
def ncs_nuevo_form(
    request: Request,
    from_remito_id: Optional[int] = None,
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
):
    """
    Form para crear una NC. Si se pasa ?from_remito_id=X, pre-carga el cliente
    + items del remito de origen para no tener que tipear de nuevo.
    """
    flash = request.session.pop("flash", None)
    form = request.session.pop("nc_form_draft", None) or {}
    items_preload: list[dict] = []

    if from_remito_id and not form:
        # Pre-cargar desde el remito si lo piden y no hay un draft previo
        remito = remitos.get_remito(db, from_remito_id)
        if remito is not None:
            form = {
                "cliente_id": remito.cliente_id,
                "remito_origen_id": remito.id,
                "motivo": "Devolución",
                "descuento_general": float(remito.descuento_general or 0),
                "observaciones": f"NC sobre remito Nº {remito.numero}.",
            }
            for it in remito.items:
                items_preload.append({
                    "sku": it.sku or "",
                    "descripcion": it.descripcion,
                    "cantidad": it.cantidad,
                    "precio_unitario": float(it.precio_unitario or 0),
                    "descuento_porc": float(it.descuento_porc or 0),
                })
        else:
            request.session["flash"] = {
                "type": "warning",
                "msg": f"No se encontró el remito ID {from_remito_id} para pre-cargar.",
            }

    return templates.TemplateResponse(
        request,
        "nota_credito_nuevo.html",
        {
            "user": user,
            "active": "notas_credito",
            "version": APP_VERSION,
            "clientes": _list_clientes_for_form(db),
            "proximo_numero": notas_credito.next_nc_numero(db),
            "motivos": notas_credito.MOTIVOS_NC,
            "form": form,
            "items_preload": items_preload,
            "flash": flash,
        },
    )


@app.post("/notas-credito/nuevo")
def ncs_nuevo_save(
    request: Request,
    cliente_id: int = Form(...),
    motivo: str = Form(default=""),
    remito_origen_id: str = Form(default=""),
    descuento_general: str = Form(default="0"),
    observaciones: str = Form(default=""),
    item_sku: list[str] = Form(default=[]),
    item_descripcion: list[str] = Form(default=[]),
    item_cantidad: list[str] = Form(default=[]),
    item_precio: list[str] = Form(default=[]),
    item_descuento: list[str] = Form(default=[]),
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
):
    items = _parse_items_from_form(
        item_sku, item_descripcion, item_cantidad, item_precio, item_descuento
    )
    if not items:
        request.session["flash"] = {
            "type": "error",
            "msg": "La NC necesita al menos un item con descripción y cantidad > 0.",
        }
        return RedirectResponse("/notas-credito/nuevo", status_code=303)

    from decimal import Decimal as _D
    try:
        desc_gen_dec = _D(str(descuento_general).replace(",", "."))
    except Exception:
        desc_gen_dec = _D("0")

    remito_orig: Optional[int] = None
    rstr = (remito_origen_id or "").strip()
    if rstr:
        try:
            remito_orig = int(rstr)
        except ValueError:
            request.session["flash"] = {
                "type": "error",
                "msg": "El ID del remito de origen debe ser un número entero.",
            }
            return RedirectResponse("/notas-credito/nuevo", status_code=303)

    try:
        nc = notas_credito.crear_nc(
            db, cliente_id, items,
            motivo=motivo,
            remito_origen_id=remito_orig,
            descuento_general=desc_gen_dec,
            observaciones=observaciones,
        )
    except remitos.RemitoError as e:
        request.session["flash"] = {"type": "error", "msg": str(e)}
        return RedirectResponse("/notas-credito/nuevo", status_code=303)
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
        return RedirectResponse("/notas-credito/nuevo", status_code=303)

    request.session["flash"] = {
        "type": "success",
        "msg": f"✓ NC {nc.numero} creada. Stock reincorporado correctamente.",
    }
    return RedirectResponse(f"/notas-credito/{nc.id}", status_code=303)


@app.get("/notas-credito/{nc_id:int}", response_class=HTMLResponse)
def nc_detail(
    request: Request,
    nc_id: int,
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
):
    nc = notas_credito.get_nc(db, nc_id)
    if nc is None:
        request.session["flash"] = {
            "type": "error",
            "msg": f"No existe la NC ID {nc_id}.",
        }
        return RedirectResponse("/notas-credito", status_code=303)
    flash = request.session.pop("flash", None)
    return templates.TemplateResponse(
        request,
        "nota_credito.html",
        {
            "user": user,
            "active": "notas_credito",
            "version": APP_VERSION,
            "nc": nc,
            "flash": flash,
        },
    )


@app.post("/notas-credito/{nc_id:int}/anular")
def nc_anular(
    request: Request,
    nc_id: int,
    motivo: str = Form(default=""),
    user: str = Depends(auth.require_user),
    db: DbSession = Depends(get_db),
):
    ok, msg = notas_credito.anular_nc(db, nc_id, motivo=motivo)
    request.session["flash"] = {"type": "success" if ok else "error", "msg": msg}
    return RedirectResponse(f"/notas-credito/{nc_id}", status_code=303)


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
    # "clientes" ya no es stub — vive en app/clientes.py + rutas dedicadas
    ("publicaciones", "Publicaciones ML",
     "Estado de los ítems publicados en Mercado Libre — pausar, "
     "republicar y ver estadísticas de cada uno."),
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
