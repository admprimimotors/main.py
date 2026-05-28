---
name: ml-compatibilidades
description: Carga compatibilidades de vehículos (marca/modelo/año) a publicaciones de Mercado Libre que están inactivas por tag "incomplete_compatibilities". Úsala cuando el usuario tiene publicaciones marcadas como "Inactivas para revisar - No indica los vehículos compatibles", o quiere cargar compatibilidades en lote a su catálogo automotriz.
---

# Skill: ML Compatibilidades — Carga masiva de vehículos compatibles

Resuelve la situación donde ML inactiva publicaciones de autopartes porque no
tienen vehículos compatibles cargados. Detecta marca + modelos del título usando
la DB ACARA, busca product_ids reales en el catálogo de ML, y los carga via API.

## Flujo

1. **Listar items afectados**: scan via `/users/{uid}/items/search?tags=incomplete_compatibilities`
2. **Para cada item**:
   - Detectar marca + modelos en el título cruzando contra `data/vehiculos-acara.json`
   - Buscar product_ids en `/products/search?domain_id=MLA-CARS_AND_VANS&q=marca+modelo`
   - POST a `/user-products/{user_product_id}/compatibilities` con:
     ```json
     {
       "domain_id": "MLA-CARS_AND_VANS",
       "products": [{"id": "MLA8709787"}, {"id": "MLA8706736"}]
     }
     ```
3. ML reprocesa automáticamente y reactiva las publicaciones (puede tardar
   minutos/horas en propagar)

## Endpoint clave descubierto

- **NO usar** `POST /items/{id}/compatibilities` (rechaza con "Item has User Product compatibilities")
- **SÍ usar** `POST /user-products/{user_product_id}/compatibilities`
- `user_product_id` está disponible en `item.user_product_id` (formato `MLAUXXXXXXXX`)
- Mínimo 2 product_ids por publicación para que ML reactive

## Estructura del body

```json
{
  "domain_id": "MLA-CARS_AND_VANS",   // dominio del vehículo compatible (no del item)
  "products": [
    {"id": "MLA8709787"},
    {"id": "MLA8706736"}
  ]
}
```

Alternativas (no probadas exhaustivamente):
- `products_families`: especificar BRAND + MODEL value_ids para asociar todos los años
- `universal: true`: marca como compatible con todo (no recomendado)

## Limitaciones conocidas

- **Máximo 200 productos por request**
- **Rate limit**: 100 requests/min por app_id
- **Items con marcas no-automotrices** (Cummins, Perkins, MWM, Deutz, Bedford,
  Indenor, Tensa, Yanmar) no se procesan: son motores estacionarios sin
  vehículo en MLA-CARS_AND_VANS. El usuario debe:
  - Cambiar título para incluir marca + modelo vehicular, o
  - Recategorizar a "Maquinaria Agrícola/Industrial", o
  - Dejar pausadas

## Configuración

`config.json` define:
- `domain_id_default`: `"MLA-CARS_AND_VANS"`
- `min_products_per_item`: 2 (mínimo ML exige)
- `max_products_per_item`: 5 (para no saturar)
- `marcas_no_vehiculares`: lista de marcas a ignorar

## Reglas de detección de marca+modelo

1. Buscar alias de marca en el título (CHEVR, CHEV, GM → CHEVROLET, etc.)
2. Si la marca encontrada está en `marcas_no_vehiculares`, descartar item
3. Si hay marca vehicular, buscar todos los modelos de esa marca que aparecen
   en el título contra ACARA
4. Si no hay marca explícita pero hay modelos identificables, deducir la marca
   del modelo (ej: "Golf" → VW, "Etios" → Toyota)
5. Si solo hay marca sin modelos en el título, usar `FALLBACK_MODELOS`
   (modelos populares de esa marca)

## Tasa de éxito típica

En PRIMI MOTORS (rubro pistones/aros/camisas Argentina):
- 384 items con `incomplete_compatibilities` detectados
- 311 (81%) detectados y cargados automáticamente
- 73 (19%) requieren revisión manual (motores estacionarios)

## Cómo invocar

```bash
python3 skills/ml-compatibilidades/cargar_compatibilidades.py [--dry-run]
```

O al asistente: *"cargar las compatibilidades a las publicaciones inactivas"*

## Salida

- `preview_compatibilidades.xlsx` con 2 hojas:
  - Hoja 1: items listos (con product_ids)
  - Hoja 2: items para revisión manual
- `compat_state.json` con estado de la corrida (OK/ERROR por item_id)
- Reporte final: cantidad de compatibilidades creadas, items reactivados

## Cuándo NO usar

- Para crear publicaciones nuevas con compatibilidades preconfiguradas
  (eso requiere include compatibilities en el POST de creación del item)
- Para items en categorías no-automotrices
- Cuando los items son catalog_listing (esos usan compatibilidades del
  catalog product, no se cargan al user_product)
