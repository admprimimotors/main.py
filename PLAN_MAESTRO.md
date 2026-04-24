# PLAN MAESTRO — Sistema Integral Primi Motors

*Última actualización: 17/04/2026*

---

## 1. Visión del proyecto

Construir un **sistema de gestión integral para Primi Motors** que corra localmente en la PC de Federico, con Mercado Libre como uno de los canales principales pero no el único. El sistema es la fuente de verdad del negocio: inventario, clientes, remitos, precios y respuestas automáticas conviven en una sola aplicación, y ML se sincroniza a partir de ese núcleo.

**Principio rector:** el stock de la PC manda. ML se actualiza desde ahí.

---

## 2. Datos de la empresa

| Campo | Valor |
|---|---|
| Nombre comercial | Primi Motors |
| Dueño | Federico Ignacio Primi (33 años) |
| Socia / responsable fiscal | Tatiana Marcela Montes |
| Email | adm.primimotors@gmail.com |
| Dirección | Calle 15 4971, Berazategui |
| CP / Provincia | 1884 · Buenos Aires |
| CUIT | 23-37354799-9 |
| Rubro | Venta de repuestos y autopartes (toda Argentina) |
| Canal principal | Mercado Libre |
| Antigüedad | 6 años |

### Especialidad histórica (motor)
Pistones · Blocks · Tapas de cilindro · Árboles de levas · Piezas del interior del motor — para vehículos livianos, pesados, maquinaria agrícola y autoelevadores.

### Rubros en incorporación
Embragues · Suspensión · Tren delantero · Frenos.

---

## 3. Arquitectura técnica

- **Lenguaje:** Python 3.11+
- **Modalidad:** aplicación local en la PC de Federico (sin servidor externo)
- **Base de datos:** SQLite (archivo local único, fácil de respaldar)
- **Interfaz:** web local (corre en `localhost`, se abre en el navegador) — a definir stack (posible FastAPI + HTMX o similar)
- **Scheduler:** APScheduler para tareas programadas (polling ML, respuestas nocturnas, etc.)
- **PDF:** ReportLab o WeasyPrint para remitos
- **Logs:** archivo rotativo + visible desde la UI

### Autenticación Mercado Libre
- OAuth 2.0 — flujo Authorization Code + Refresh Token
- Autenticación manual una sola vez; después renovación automática
- **Polling** cada X minutos (sin webhooks)
- Credenciales en `.env` local (no se sube a ningún lado)

---

## 4. Credenciales y accesos

### App de Mercado Libre Developers
- **Nombre:** PrimiMotors Automation
- **Client ID / App ID:** `3131016753852536`
- **Client Secret:** guardado en `.env` (no se escribe acá por seguridad)
- **Redirect URI:** `https://www.google.com`
- **Site ID:** `MLA` (Argentina)

### Permisos habilitados
Usuarios (R/W) · Comunicaciones pre/post venta (R/W) · Publicación y sincronización (R/W) · Métricas (R) · Venta y envíos (R/W)

### Sin acceso a
Publicidad · Facturación · Promociones/cupones (AFIP lo maneja Federico por afuera)

---

## 5. Fuentes de datos actuales

### Google Sheet "Remitos"
- **File ID:** `1Z4FgNnSWV2Ke7tcbZaMpoSYfI-9sNM5eMXvr4pBVIbk`
- **URL:** https://docs.google.com/spreadsheets/d/1Z4FgNnSWV2Ke7tcbZaMpoSYfI-9sNM5eMXvr4pBVIbk/edit
- **Solapas relevantes:**
  - `Clientes` → base de clientes (nombre, dirección, provincia/CP, teléfono, CUIT, mail)
  - `BaseDatosRemitos` → histórico de remitos emitidos
  - `BaseDatosNC` → histórico de notas de crédito
  - `Index` → plantilla visual de remito (`A1:I39`) y NC (`O1:W39`)

### Numeración actual
- Último remito observado: **1549** (abril 2026)
- Último nota de crédito: ~**202**
- La app continúa la numeración desde donde esté.

### Inventario
- Actualmente: carga desde archivos de proveedores (Excel o PDF)
- Identificador único: **código SKU del producto**
- Se necesita: categorización + búsqueda ágil

### Catálogos para publicaciones
- Federico los sube a la app (formato: Excel, PDF, Word)
- De ahí se extraen: títulos, descripciones, fichas técnicas, fotos, precios

---

## 6. Módulos de la aplicación

### 6.1 Inventario
- Carga inicial y actualización (manual o por subida de Excel/PDF de proveedor)
- Identificador principal: **SKU**
- Categorización y búsqueda ágil
- Sincronización automática con ML: pausa al agotarse, reactiva al reponer
- Registro de movimientos (auditoría)

### 6.2 Publicaciones ML
- Lectura de catálogos proporcionados por Federico
- Generación automática de: títulos SEO-optimizados, descripciones, fichas técnicas
- Asignación de fotos
- Publicación masiva ordenada
- Armado de **compatibilidades vehiculares** (crítico en repuestos)
- Validación pre-publicación

### 6.3 Precios
- Reglas definibles (ej: costo + 40%; precio de lista con X% descuento)
- Actualización masiva ante cambios de costos
- Aplica tanto al inventario local como a publicaciones ML

### 6.4 Respuestas automáticas ML
- Lectura periódica de preguntas nuevas
- Responde usando información de catálogos
- Clasificador: responde sola las simples, guarda las complejas para respuesta manual
- Horarios configurables (típicamente nocturno)

### 6.5 Gestión de Remitos y Notas de Crédito
- Emisión de remitos a: clientes cargados, clientes nuevos, o consumidor final sin nombre
- Ítems: del inventario (descuenta stock + sincroniza con ML) o escritos libres
- **Al cargar un producto libre, el sistema ofrece guardarlo en el inventario** (con opción de sumarlo a stock y/o subirlo a ML)
- Descuento especial por cliente (campo en ficha de cliente) + descuento global del remito
- **Sin espacio para firma en el PDF**
- PDF descargable + registro en la app con total e ítems
- Notas de crédito: seleccionar uno o varios ítems del remito original → suma stock de vuelta
- Facturación AFIP: **NO** (la hacen Federico y Tatiana por afuera, por ahora)
- Impresión física: **NO** por ahora (solo PDF)

### 6.6 Dashboard
- Ventas totales · ganancia · ventas ML · ventas por remito
- Productos más vendidos · stock valorizado · comisiones ML
- Alertas de reputación ML y stock crítico

---

## 7. Roadmap por fases

| Fase | Módulo | Duración estimada | Estado |
|---|---|---|---|
| 0 | Fundación: OAuth, BD, logs, estructura | 1 semana | 🟡 En curso |
| 1 | Inventario + sync básico con ML | 2 semanas | ⚪ Pendiente |
| 2 | Clientes + Remitos + Notas de Crédito | 2 semanas | ⚪ Pendiente |
| 3 | Publicaciones ML masivas | 3 semanas | ⚪ Pendiente |
| 4 | Precios (reglas + actualización masiva) | 1 semana | ⚪ Pendiente |
| 5 | Respuestas automáticas | 2 semanas | ⚪ Pendiente |
| 6 | Dashboard | 1-2 semanas | ⚪ Pendiente |

Total estimado: ~12-14 semanas iterando.

---

## 8. Decisiones tomadas (acordadas)

- ✅ Sistema local, no cloud, una sola PC
- ✅ Python + SQLite + UI web local
- ✅ OAuth manual una vez, después automático
- ✅ Polling, no webhooks
- ✅ Stock de la PC = fuente de verdad; ML se sincroniza desde acá
- ✅ Remito continúa la numeración actual (próximo: 1550)
- ✅ Nota de crédito continúa su numeración (próxima: 203)
- ✅ Sin espacio para firma en PDF
- ✅ Sin facturación AFIP (lo hacen por afuera)
- ✅ Solo PDF, sin impresión física por ahora
- ✅ Productos libres en remitos → ofrecer guardarlos en inventario
- ✅ Descuento por defecto por cliente + descuento global por remito
- ✅ Logo oficial disponible (Primi Motors — pistones y cigüeñal con texto azul/rojo)

---

## 9. Parámetros del negocio (respondidos)

- ✅ **Publicaciones activas hoy:** 74 (cada una con su SKU)
- ✅ **Total en ML (activas + inactivas):** +1.400
- ✅ **Productos planeados para cargar:** ~5.000 más con el sistema
- ✅ **Moneda de compra:** pesos argentinos (no hay que ajustar por USD)
- ✅ **Horario atención manual:** 8:00 a 18:30 hora Argentina (el bot de respuestas automáticas trabaja fuera de ese horario)

## 9b. Pendientes

- Subir el PNG del logo al proyecto (`assets/logo_primi.png`) para embeberlo en PDFs
- Definir margen por defecto para el módulo Precios (cuando lleguemos a Fase 4)
- Confirmar si hay una o varias cuentas ML (asumo una por ahora)

---

## 10. Estructura del proyecto (a construir)

```
primi-motors-ml/
├── .env                    # credenciales (no versionado)
├── .env.example            # template sin secretos
├── .gitignore
├── requirements.txt
├── README.md
├── PLAN_MAESTRO.md         # este archivo
├── config.py               # carga de variables de entorno
├── db.py                   # conexión y esquema SQLite
├── logger.py               # sistema de logs
├── assets/
│   └── logo_primi.png
├── ml/                     # todo lo relacionado con Mercado Libre
│   ├── auth.py             # OAuth + refresh
│   ├── client.py           # wrapper con auto-retry y rate-limit
│   ├── stock_sync.py
│   ├── publicaciones.py
│   ├── precios.py
│   └── respuestas.py
├── inventory/
│   ├── models.py
│   ├── importer.py         # carga desde Excel/PDF
│   └── search.py
├── clientes/
│   └── models.py
├── remitos/
│   ├── models.py
│   ├── pdf_generator.py
│   └── numbering.py
├── dashboard/
│   └── app.py              # UI local (FastAPI)
├── scheduler/
│   └── jobs.py             # tareas programadas
└── data/
    ├── primi_motors.db     # SQLite
    └── logs/
```

---

## 11. Cómo retomar una sesión

> Pegar al inicio de la nueva conversación:
>
> "Hola, soy Federico de Primi Motors. Continuamos el proyecto del sistema integral. Leé `/sessions/youthful-bold-faraday/mnt/outputs/primi-motors-ml/PLAN_MAESTRO.md` para ponerte al día."
