"""
app/main.py
===========
FastAPI — backend web de Primi Motors.

Este es el MVP de despliegue para Render:
  - `/`         → landing mínima (HTML inline, confirma que está vivo).
  - `/health`   → endpoint JSON para health-check de Render.
  - `/status`   → info básica del entorno (versión Python, env, etc.).

Próximos pasos (a medida que vayamos migrando):
  - `/auth/login`         → login simple usuario/clave (admin Primi Motors).
  - `/upload/excel`       → subir Excel master (Catálogo / Compatibilidades / Stock).
  - `/upload/fotos`       → subir fotos por SKU (se almacenan en R2/B2).
  - `/ml/sync-stock`      → botón "sincronizar stock con ML".
  - `/ml/publicar-lote`   → publicar SKUs en batch con diagnóstico previo.
  - `/dashboard`          → SPA con estética Primi Motors.

IMPORTANTE — variables de entorno en Render:
  - `ML_CLIENT_ID`, `ML_CLIENT_SECRET`, `ML_REFRESH_TOKEN`
  - `DATABASE_URL`  (cuando migremos a Postgres)
  - `R2_ACCESS_KEY`, `R2_SECRET_KEY`, `R2_BUCKET`  (cuando sumemos storage)
  - `ADMIN_USER`, `ADMIN_PASSWORD_HASH`

Hoy el backend NO toca la DB ni ML — es solo el "hola mundo" para confirmar
que el pipeline GitHub → Render → admin.primimotors.com.ar funciona.
"""

from __future__ import annotations

import os
import platform
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

APP_NAME = "Primi Motors — Backend"
APP_VERSION = "0.1.0"

app = FastAPI(
    title=APP_NAME,
    version=APP_VERSION,
    description="Backend web del sistema Primi Motors (Mercado Libre, stock, publicaciones).",
)


@app.get("/", response_class=HTMLResponse)
def home() -> str:
    """Landing mínima. Confirma visualmente que el deploy quedó vivo."""
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
            padding: 48px 56px; border-radius: 14px; max-width: 560px;
          }}
          h1 {{ margin: 0 0 8px; color: #ffb703; font-size: 28px; }}
          p  {{ margin: 8px 0; color: #bbb; line-height: 1.5; }}
          code {{ background: #1c1c1c; padding: 2px 6px; border-radius: 4px; color: #ffb703; }}
        </style>
      </head>
      <body>
        <div class="card">
          <h1>🚗 Primi Motors — Backend</h1>
          <p>Versión <code>{APP_VERSION}</code> — deploy OK.</p>
          <p>Endpoints: <code>/health</code> · <code>/status</code></p>
          <p style="margin-top:24px; font-size:13px; color:#666;">
            Sistema en construcción. Próximamente: panel de administración,
            upload de catálogo, sincronización con Mercado Libre.
          </p>
        </div>
      </body>
    </html>
    """


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
    })
