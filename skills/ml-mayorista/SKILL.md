---
name: ml-mayorista
description: Aplica precios mayoristas escalonados a publicaciones de Mercado Libre usando el editor masivo. Úsala cuando el usuario quiera configurar "Precios mayoristas" (Precio mayorista 1/2/3) en sus publicaciones de ML, agregar descuentos por cantidad, o repetir el proceso cuando subió mercadería nueva. Trigger frases como "aplicar mayoristas", "precios por cantidad", "descuento por volumen", "subí stock nuevo, regenerá los mayoristas".
---

# Skill: ML Mayoristas — Precios escalonados por cantidad

Esta skill toma el Excel del **editor masivo de Mercado Libre** (descargado desde
https://www.mercadolibre.com.ar/publicaciones/editor-masivo) y completa las
columnas de precios mayoristas según escalones configurables por stock.

## Por qué existe

La feature "Precios por cantidad" del API público de ML (`/items/$ID/prices/standard/quantity`)
está restringida a sellers B2B "selected" o al rubro neumáticos automotrices.
El usuario PRIMI MOTORS no califica en ninguno, pero el editor masivo SÍ acepta
estos cambios. La skill automatiza completar el Excel para subirlo a mano.

## Inputs

- Archivo Excel descargado del editor masivo de ML (formato típico:
  `Publicaciones-YYYY_MM_DD-HH_MM.xlsx`)
- Hoja "Publicaciones" con columnas estándar:
  - `ITEM_ID` (col 2), `TITLE` (col 6), `STOCK_FLEX` (col 8), `PRICE` (col 9),
    `CURRENCY_ID` (col 10)
  - `TIERED_PRICING_PRICE_1` (col 11), `TIERED_PRICING_QUANTITY_1` (col 12)
  - `TIERED_PRICING_PRICE_2` (col 13), `TIERED_PRICING_QUANTITY_2` (col 14)
  - `TIERED_PRICING_PRICE_3` (col 15), `TIERED_PRICING_QUANTITY_3` (col 16)
  - Hasta 5 escalones disponibles.

## Configuración estándar de PRIMI MOTORS

| Escalón | Cantidad mínima | Descuento |
|---|---|---|
| Mayorista 1 | 2 unidades | 6% |
| Mayorista 2 | 4 unidades | 10% |
| Mayorista 3 | 6 unidades | 15% |

Estas reglas viven en `config.json` (en la carpeta de la skill) y se pueden
modificar sin tocar el script.

## Flujo

1. **Borrar mayoristas previos** en todas las filas para empezar desde cero
   (evita inconsistencias si el usuario corrió la skill antes con otros valores).
2. **Para cada fila con `STOCK_FLEX ≥ 2`**:
   - Aplicar Mayorista 1 si stock ≥ 2: `price × (1 - desc1) / 2u`
   - Aplicar Mayorista 2 si stock ≥ 4: `price × (1 - desc2) / 4u`
   - Aplicar Mayorista 3 si stock ≥ 6: `price × (1 - desc3) / 6u`
3. Las filas con `STOCK_FLEX < 2`, sin precio, o sin stock quedan vacías en
   los mayoristas.
4. Guardar el archivo modificado preservando hojas, encabezados, validaciones.
5. Resumen: cuántas filas tocó por escalón.

## Cómo invocar

```python
python3 skills/ml-mayorista/aplicar_mayoristas.py \
  --input "uploads/Publicaciones-2026_05_26-12_18.xlsx" \
  --output "Publicaciones_CON_MAYORISTA.xlsx"
```

O directamente al asistente: *"Acá te paso el editor masivo de ML, aplicá los
escalones mayoristas como siempre."*

## Salida esperada

- Archivo Excel modificado, mismo formato.
- Resumen en consola:
  - Filas con M1 (≥2u), M2 (≥4u), M3 (≥6u)
  - Filas sin tocar (stock<2, sin precio)
  - Muestra de 6 filas con los 3 escalones aplicados
- El usuario sube ese archivo manualmente desde el editor masivo de ML.

## Reglas clave

1. **Nunca tocar el precio base (`PRICE` col 9)**. Solo modificar columnas 11-20.
2. **Borrar antes de escribir**: limpiar M1-M5 en todas las filas para evitar
   estados mezclados de corridas previas.
3. **Preservar formato**: copiar el archivo de input con `cat` (no `shutil.copy`
   en algunos casos, porque heredas permisos read-only del directorio uploads).
4. **No filtrar filas**: el archivo de salida debe tener exactamente las mismas
   filas que el input. ML rechaza el upload si faltan filas.
5. **Cantidad mínima obligatoria**: si pones un precio mayorista, también tenés
   que poner su cantidad mínima en la columna siguiente. Si pones una sin la otra,
   ML lo rechaza.

## Modificar la configuración

Para cambiar los escalones, editar `config.json`:

```json
{
  "tiers": [
    {"min_qty": 2, "discount_pct": 6},
    {"min_qty": 4, "discount_pct": 10},
    {"min_qty": 6, "discount_pct": 15}
  ],
  "currency": "ARS"
}
```

Máximo 5 tiers (ML soporta hasta 5). El script lee este archivo en runtime.

## Errores comunes

- **PermissionError al guardar**: el archivo está abierto en Excel o OneDrive lo
  está sincronizando. Pedir al usuario que lo cierre.
- **Filas con precio en formato `''` (string vacío)**: el script las trata como
  `None` y las ignora.
- **Stock como float**: ML a veces exporta `5.0` en lugar de `5`. Tratar como int.

## Cuándo NO usar esta skill

- Para subir precios mayoristas via API directa (no funciona para PRIMI sin B2B).
- Para promociones globales tipo "todo 10% off" — eso es otro endpoint
  (`/seller-promotions/promotions` con `promotion_type=VOLUME`).
- Para cambiar el precio base de los productos — usar `update-precio-base` (TODO).
