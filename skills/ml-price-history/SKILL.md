---
name: ml-price-history
description: Registra y consulta historial de cambios de precio de publicaciones de Mercado Libre. Úsala cuando el usuario quiera saber cómo varió el precio de un item, ver cambios recientes en el catálogo, o configurar tracking automático. Trigger frases como "histórico de precios", "cuándo cambié el precio", "qué precios subí esta semana", "tracking de precios".
---

# Skill: ML Price History — Tracking de cambios de precio

ML API pública NO expone histórico de cambios de precio: solo el precio actual.
Esta skill resuelve eso con snapshots locales. Cada corrida captura el precio
de las 1000+ publicaciones activas y, comparando con la última corrida,
identifica los cambios. Acumulando snapshots se construye el histórico.

## Cómo funciona

1. Una tabla local `ml_price_snapshots` en `data/primi_motors.db` guarda
   cada snapshot con `(ml_item_id, price, captured_at, is_change)`.
2. El script `track_precios.py` corre periódicamente:
   - Lista todas las publicaciones activas.
   - Para cada item, trae el precio actual desde ML.
   - Compara contra el último snapshot del mismo item.
   - Inserta una fila nueva. Si el precio cambió, `is_change=1`.
3. Consultando `WHERE ml_item_id = '...' AND is_change = 1 ORDER BY captured_at`
   se obtiene el historial de cambios reales.

## Schema de la tabla

```sql
CREATE TABLE ml_price_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ml_item_id TEXT NOT NULL,
    title TEXT, sku TEXT,
    price REAL NOT NULL,
    base_price REAL,
    original_price REAL,
    currency TEXT,
    status TEXT,
    available_quantity INTEGER,
    sold_quantity INTEGER,
    ml_price_id TEXT,
    captured_at TEXT NOT NULL,
    is_change BOOLEAN NOT NULL DEFAULT 0
);
```

## Comandos

**Capturar snapshot diario** (guarda TODOS los precios, marca cambios):
```bash
python3 skills/ml-price-history/track_precios.py
```

**Capturar SOLO cambios** (no inserta filas si no hay cambio — ahorra espacio):
```bash
python3 skills/ml-price-history/track_precios.py --changed-only
```

## Consultas útiles

**Historial de un item específico:**
```sql
SELECT captured_at, price,
       LAG(price) OVER (ORDER BY captured_at) AS price_anterior
FROM ml_price_snapshots
WHERE ml_item_id = 'MLA1671394509'
ORDER BY captured_at DESC;
```

**Solo los cambios (no snapshots sin variación):**
```sql
SELECT captured_at, price, sku
FROM ml_price_snapshots
WHERE ml_item_id = 'MLA1671394509' AND is_change = 1
ORDER BY captured_at DESC;
```

**Items con más subas/bajas en los últimos N días:**
```sql
SELECT ml_item_id, title, COUNT(*) AS cambios
FROM ml_price_snapshots
WHERE is_change = 1
  AND captured_at >= datetime('now', '-30 days')
GROUP BY ml_item_id ORDER BY cambios DESC LIMIT 20;
```

## Tracking automático (recomendado)

Programar la skill para correr cada día a las 8am usando el sistema de
scheduled tasks de Cowork. Una vez por día es suficiente — más frecuente
solo da más resolución temporal pero ocupa más espacio en la DB.

## Limitaciones

- Solo registra desde el día que se activó. Cambios anteriores se pierden
  (ML no los expone).
- Requiere correr el script periódicamente — sin tracking activo no hay datos.
- Funciona solo con items activos. Items pausados o cerrados quedan
  congelados en su último snapshot conocido.

## Resultado de la primera corrida (baseline)

- **2026-05-27**: 1.039 publicaciones activas, todos los precios capturados
  como baseline. Próximas corridas detectarán cambios.
