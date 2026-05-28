# Base de vehículos de referencia (ACARA)

Este archivo es una **extensión** de la skill `experto-repuestos`. Vive en la
carpeta del proyecto (`data/`) y debe consultarse junto a `vehiculos-acara.json`
cuando se procesan catálogos de repuestos.

## Ubicación

- `data/vehiculos-acara.json` — DB estructurada `{marca: [modelos]}`
- `data/vehiculos-acara.csv` — versión plana con `marca, modelo`

Generadas a partir de la Guía Oficial de Precios ACARA (384 páginas).
Contiene 97 marcas y 1.340 modelos.

## Cuándo y cómo usarla

### Modo A — Procesar catálogo

Antes de escribir filas en `base-repuestos.xlsx`, **siempre** consultar esta DB
para resolver `marca_vehiculo` y `modelo_vehiculo`:

1. Verificar que la marca extraída exista en la DB. Normalizar si hace falta
   (`VW` → `VOLKSWAGEN`, `MB` → `MERCEDES-BENZ`, `CITROEN` → `CITROËN`).
2. Para cada candidato a modelo, chequear contra `db[marca]`:
   - ¿Aparece tal cual? → mantenerlo.
   - ¿No aparece y parece modelo? → mantener + anotar en `observaciones`:
     "modelo no validado contra ACARA".
   - ¿Es claramente otra cosa (cilindrada, código de motor, medida)? → moverlo
     al campo correcto.

### Reglas clave: descomponer líneas compactas

Catálogos como **PERSAN** usan formatos del tipo `Onix - Prisma 1,4 NF 8V`.
La descomposición correcta:

| Texto           | Campo destino       | Interpretación      |
|-----------------|---------------------|---------------------|
| `Onix - Prisma` | `modelo_vehiculo`   | Modelos compatibles |
| `1,4`           | `cilindrada`        | 1.4 L               |
| `NF`            | `tipo_combustible`  | Nafta               |
| `8V`            | `cantidad_valvulas` | 8 válvulas          |

**Abreviaturas argentinas a reconocer:**

- Combustible: `NF`/`N`/`Nafta`/`Gas.` → `Nafta`. `DSL`/`D`/`Diesel`/`Diésel`/
  `TDI`/`CRDi`/`HDi`/`JTD` → `Diésel`. `GNC` → `GNC`.
- Válvulas: `8V`, `16V`, `12V`, `24V` → número directo.
- Cilindrada: formato `1,4` (coma como decimal) = `1.4` L. En cc: `1598` →
  redondeo comercial `1.6`.
- Transmisión / trim (NO va en motor): `MT`, `AT`, `MT6`, `5MT`, `S-Tronic`,
  `TCT`, `CVT`, `DSG`.

### Caso real (corrección a aplicar a las filas mal cargadas)

Si una fila viene con:
- `modelo_vehiculo`: `1,4 NF 8V ...`
- `tipo_motor`: `Onix - Prisma`
- `cilindrada`: `A confirmar`

El mapeo correcto (consultando ACARA → Chevrolet tiene `Onix` y `Prisma`):
- `modelo_vehiculo`: `Onix, Prisma`
- `marca_vehiculo`: `Chevrolet`
- `tipo_motor`: `A confirmar` (el código no figura)
- `cilindrada`: `1.4`
- `cantidad_valvulas`: `8`
- `tipo_combustible`: `Nafta`

### Modo B — Fichas para publicar

Si una fila tiene `modelo_vehiculo = A confirmar` (catálogo trae solo marca +
motor), usar la DB ACARA para proponer modelos compatibles con esa marca +
cilindrada + combustible. Mostrar al usuario para confirmar antes de publicar.

## Carga programática

```python
import json
db = json.load(open('data/vehiculos-acara.json', 'r', encoding='utf-8'))
# db['CHEVROLET'] = ['Agile', 'Astra II', 'Aveo', 'Camaro', ...]

def modelo_oficial(marca, modelo):
    return modelo in db.get(marca.upper(), [])
```

## Mantenimiento

- Regenerar corriendo el parser ACARA sobre versión nueva del PDF (`acara_precios.pdf`).
- Si falta una marca/modelo, editar `vehiculos-acara.json` a mano.
- Marca en MAYÚSCULAS, modelo en capitalización oficial (`Onix`, `Hilux`,
  `Corolla Cross`).
