"""
Paquete `app` — backend web de Primi Motors (FastAPI).

Esto es el punto de entrada para el deploy en Render. La idea:
  - La lógica "core" (SQLite + ML API + publicaciones) vive en los módulos
    que ya existen en la raíz (`db.py`, `ml/`, etc.) y se va migrando
    incremental a este paquete.
  - `app/main.py` expone los endpoints HTTP (health, auth, upload de
    Excel/fotos, endpoints REST para el dashboard).
  - Render ejecuta: `uvicorn app.main:app --host 0.0.0.0 --port $PORT`
"""
