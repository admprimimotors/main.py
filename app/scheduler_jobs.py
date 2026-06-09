"""
app/scheduler_jobs.py
=====================
Auto-sync periódico de PUBLICACIONES ML → Postgres con APScheduler.

Por qué existe
--------------
La app ya auto-sincroniza ÓRDENES cada 15 min (lazy, on-request). Pero las
PUBLICACIONES no tenían auto-sync: el snapshot (estado/stock/precio) quedaba
viejo hasta que alguien apretaba el botón manual. Resultado real: el panel
controlaba con datos del 14/05 mientras ML seguía cambiando (drift invisible).

Este módulo arranca un BackgroundScheduler dentro del proceso web. Como Render
Starter NO duerme (sin cold starts), el scheduler corre confiable. Cada N
minutos refresca un lote de las publicaciones MÁS desactualizadas
(`catalogo.bulk_sync_oldest`, oldest-first). Con los valores por defecto el
catálogo completo (~1.650 vinculadas) se refresca en MENOS de 1 hora.

Es read-only sobre ML (solo GET /items/{id}) — NO toca precios ni stock en ML.
Solo actualiza el snapshot local (productos.ml_* y producto_publicaciones_ml.*).

Config (env vars opcionales)
----------------------------
  ML_PUBS_SYNC_ENABLED       (default "1")  -> "0" desactiva el job
  ML_PUBS_SYNC_INTERVAL_MIN  (default 10)   -> cada cuántos minutos corre
  ML_PUBS_SYNC_BATCH         (default 300)  -> cuántas publicaciones por corrida

  Default: 300 cada 10 min = 1.800/h ≥ 1.650 vinculadas -> ciclo < 1 h.
  Si ML devuelve muchos 429 (rate limit), bajá el batch o subí el intervalo.

Nota de despliegue
------------------
Render corre 1 solo worker (startCommand sin --workers), así que hay UN
scheduler. Si algún día se escala a varios workers, cada uno levantaría su
scheduler y duplicaría el sync: en ese caso mover el job a un worker dedicado
o gatearlo por índice de worker.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

from apscheduler.schedulers.background import BackgroundScheduler

from . import catalogo, database, ml_client

_scheduler: BackgroundScheduler | None = None

_ENABLED = os.environ.get("ML_PUBS_SYNC_ENABLED", "1") != "0"
_INTERVAL_MIN = int(os.environ.get("ML_PUBS_SYNC_INTERVAL_MIN", "10"))
_BATCH = int(os.environ.get("ML_PUBS_SYNC_BATCH", "300"))


def _sync_publicaciones_job() -> None:
    """
    Refresca un lote de las publicaciones más desactualizadas.
    Fail-safe: nunca levanta excepción hacia el scheduler (si ML o la DB
    fallan, loguea y sigue; la próxima corrida reintenta).
    """
    if database.SessionLocal is None or not ml_client.is_configured():
        return
    started = datetime.now(timezone.utc)
    db = database.SessionLocal()
    try:
        ok, total, errores = catalogo.bulk_sync_oldest(db, limit=_BATCH)
        db.commit()
        dur = (datetime.now(timezone.utc) - started).total_seconds()
        msg = f"[pubs-sync] {ok}/{total} ok en {dur:.0f}s"
        if errores:
            msg += f" · {len(errores)} con error (ej: {errores[0]})"
        print(msg, flush=True)
    except Exception:
        import traceback
        traceback.print_exc()
        try:
            db.rollback()
        except Exception:
            pass
    finally:
        db.close()


def start() -> None:
    """
    Arranca el scheduler una sola vez. Idempotente. Llamado desde el startup
    de FastAPI. Si falta DATABASE_URL o está deshabilitado por env, no hace nada.
    """
    global _scheduler
    if not _ENABLED:
        print("[pubs-sync] deshabilitado (ML_PUBS_SYNC_ENABLED=0)", flush=True)
        return
    if _scheduler is not None:
        return
    if database.SessionLocal is None:
        print("[pubs-sync] sin DATABASE_URL — scheduler no arranca", flush=True)
        return

    sch = BackgroundScheduler(timezone="UTC", daemon=True)
    sch.add_job(
        _sync_publicaciones_job,
        trigger="interval",
        minutes=_INTERVAL_MIN,
        max_instances=1,                              # nunca dos corridas en paralelo
        coalesce=True,                                # si se acumulan, corre una sola
        next_run_time=datetime.now(timezone.utc),     # primera corrida al arrancar
        id="pubs_sync",
        replace_existing=True,
    )
    sch.start()
    _scheduler = sch
    print(
        f"[pubs-sync] scheduler ON · cada {_INTERVAL_MIN} min · lote {_BATCH} "
        f"(ciclo completo < 1 h)",
        flush=True,
    )


def shutdown() -> None:
    """Frena el scheduler (para el shutdown de FastAPI). Idempotente."""
    global _scheduler
    if _scheduler is not None:
        try:
            _scheduler.shutdown(wait=False)
        except Exception:
            pass
        _scheduler = None
