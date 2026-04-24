# CONTEXTO COMPLETO — Proyecto Primi Motors

*Sesión del 17/04/2026 con Claude Opus 4.7*
*Documento actualizado: contiene TODO lo hablado hasta ahora. Sirve para retomar la conversación desde cero en cualquier momento.*

---

## 📌 Cómo retomar una sesión futura

Al iniciar una nueva conversación, pegar este mensaje:

> "Hola, soy Federico de Primi Motors. Continuamos el proyecto del sistema integral de gestión. Leé los archivos `PLAN_MAESTRO.md` y `CONTEXTO_COMPLETO.md` en la carpeta `primi-motors-ml` para ponerte al día, y retomamos desde donde quedamos."

---

## 1. DATOS DEL NEGOCIO

- **Nombre comercial:** Primi Motors
- **Dueño:** Federico Ignacio Primi (33 años, argentino)
- **Socia / responsable fiscal:** Tatiana Marcela Montes (pareja y compañera de trabajo)
- **Email:** adm.primimotors@gmail.com
- **Dirección fiscal:** Calle 15 4971, Berazategui
- **Código Postal / Provincia:** 1884 · Buenos Aires
- **CUIT:** 23-37354799-9
- **Rubro:** Venta de repuestos y autopartes para autos — toda Argentina
- **Canal principal:** Mercado Libre
- **Antigüedad:** 6 años en el rubro
- **Logo:** disponible (pistones y cigüeñal con texto "PRIMI MOTORS" en azul y rojo sobre fondo negro)

### Productos — especialidad histórica (motor)
Pistones · Blocks de motor · Tapas de cilindro · Árboles de levas · Piezas del interior del motor en general.
Para: vehículos livianos, pesados, maquinaria agrícola, autoelevadores.

### Rubros nuevos que están incorporando
Embragues · Suspensión · Tren delantero · Frenos.

---

## 2. OBJETIVO DEL PROYECTO

Construir un **sistema integral de gestión** para Primi Motors que corra localmente en la PC de Federico (sin servidor externo), con Mercado Libre como canal principal pero no único.

### Principio rector
**El stock de la PC es la fuente de verdad. Mercado Libre se sincroniza desde ahí.**

El sistema centraliza inventario, clientes, remitos, precios y respuestas en una sola aplicación. ML es un consumidor de ese núcleo, igual que los remitos directos.

---

## 3. ARQUITECTURA TÉCNICA DECIDIDA

- **Lenguaje:** Python 3.11+
- **Modalidad:** aplicación local en la PC de Federico
- **Base de datos:** SQLite (archivo único, fácil de respaldar)
- **Interfaz:** web local (corre en `localhost`, se abre en navegador — stack a definir, probablemente FastAPI + HTMX o similar)
- **Scheduler:** APScheduler para tareas programadas
- **PDF:** ReportLab o WeasyPrint (para remitos y notas de crédito)
- **Logs:** archivo rotativo + visible desde la UI
- **Autenticación ML:** OAuth 2.0 (Authorization Code + Refresh Token, una sola autenticación manual)
- **Modo de operación con ML:** polling cada X minutos (NO webhooks)
- **Credenciales:** en archivo `.env` local, protegido por `.gitignore`

---

## 4. APP DE MERCADO LIBRE DEVELOPERS

**Portal:** developers.mercadolibre.com.ar
**Cuenta ML:** PRIMI

### Configuración de la app "PrimiMotors Automation"
- **Client ID / App ID:** `3131016753852536`
- **Client Secret:** guardado en `.env` local (sensible, no se transcribe)
- **Redirect URI:** `https://www.google.com`
- **Notifications Callback:** placeholder — no se usa porque vamos por polling
- **PKCE:** No

### Flujos OAuth habilitados
Authorization Code · Client Credentials · Refresh Token

### Permisos configurados

| Permiso | Nivel |
|---|---|
| Usuarios | Lectura y escritura |
| Comunicaciones pre y post ventas | Lectura y escritura |
| Publicación y sincronización | Lectura y escritura |
| Métricas del negocio | Solo lectura |
| Venta y envíos de un producto | Lectura y escritura |
| Publicidad de un producto | Sin acceso |
| Facturación de una venta | Sin acceso |
| Promociones, cupones y descuentos | Sin acceso |

### Tópicos de notificación
Todos desmarcados (se usa polling, no webhooks).

---

## 5. FUENTES DE DATOS ACTUALES

### Google Sheet "Remitos"
- **File ID:** `1Z4FgNnSWV2Ke7tcbZaMpoSYfI-9sNM5eMXvr4pBVIbk`
- **URL:** https://docs.google.com/spreadsheets/d/1Z4FgNnSWV2Ke7tcbZaMpoSYfI-9sNM5eMXvr4pBVIbk/edit
- **Solapas relevantes:**
  - `Clientes` → base de clientes (nombre, dirección, provincia/CP, teléfono, CUIT, mail)
  - `BaseDatosRemitos` → histórico de remitos emitidos
  - `BaseDatosNC` → histórico de notas de crédito
  - `Index` → plantilla visual actual de remito (`A1:I39`) y nota de crédito (`O1:W39`)

### Numeración actual (observada en el Sheet)
- Último remito: **1549** (abril 2026) → próximo será el **1550**
- Último nota de crédito: aprox **202** → próxima será la **203**
- La app debe continuar desde estos números.

### Inventario
- Hoy: archivos de proveedores (Excel o PDF) cargados manualmente
- Identificador único: **código SKU del producto**
- Necesidad: categorización + búsqueda ágil

### Catálogos para publicaciones
- Federico los sube a la app (Excel, PDF, Word)
- De ahí se extrae: títulos, descripciones, fichas técnicas, fotos, precios

---

## 6. MÓDULOS DE LA APLICACIÓN (6 en total)

### 6.1 — Inventario
- Carga inicial y actualización (manual o por subida de archivo Excel/PDF de proveedor)
- Identificación por **SKU**
- Categorización y búsqueda ágil
- Sincronización automática con ML: pausa al agotarse, reactiva al reponer stock
- Registro de movimientos (auditoría)

### 6.2 — Publicaciones ML (masivas)
- Lectura de catálogos que sube Federico
- Generación automática de: títulos optimizados SEO, descripciones, fichas técnicas
- Asignación de fotos
- Publicación masiva y ordenada
- **Compatibilidades vehiculares** armadas automáticamente (marca / modelo / año / motor) — crítico en repuestos
- Categorización ML correcta por producto
- Validación pre-publicación para evitar penalizaciones

### 6.3 — Precios
- Reglas de negocio definidas por Federico (ej: costo + 40%, precio lista con X% de descuento)
- Actualización masiva cuando cambian costos
- Aplica tanto al inventario local como a publicaciones ML
- Historial de cambios

### 6.4 — Respuestas automáticas ML
- Lectura periódica de preguntas en ML (polling)
- Responde con info de los catálogos, fichas técnicas y base de conocimiento
- Clasificador: responde sola las simples, deriva las complejas a respuesta manual
- Horarios configurables (típicamente nocturno)
- Cola de preguntas pendientes de respuesta manual

### 6.5 — Gestión de Remitos y Notas de Crédito
- Emisión de remitos a: clientes existentes, clientes nuevos, o consumidor final sin nombre
- Ítems del remito: del inventario (descuenta stock + sincroniza con ML) o escritos libres
- **Productos libres: al finalizar el remito, el sistema ofrece guardarlos en el inventario** (con opción de sumarles stock o subirlos a ML)
- Descuento por defecto por cliente (campo en ficha) + descuento global del remito
- **PDF sin espacio para firma**
- PDF descargable + registro en la app (total, ítems, cliente, fecha)
- Notas de crédito: selección parcial o total de ítems del remito original → devuelve stock al inventario y lo sincroniza con ML
- **Facturación AFIP:** NO (lo manejan Federico y Tatiana por afuera, por ahora)
- **Impresión física:** NO por ahora, solo PDF

### 6.6 — Dashboard
- Ventas totales · ganancia · ventas ML · ventas por remito
- Productos más vendidos · stock valorizado · comisiones ML pagadas
- Alertas de reputación ML y de stock crítico

---

## 7. DECISIONES TOMADAS Y ACORDADAS

- ✅ Sistema local, no cloud, una sola PC
- ✅ Python + SQLite + UI web local
- ✅ OAuth manual una sola vez, luego renovación automática
- ✅ Polling para leer ML (no webhooks)
- ✅ Stock de la PC es fuente de verdad; ML se sincroniza desde ahí
- ✅ Remitos siguen numeración existente del Sheet (próximo: 1550)
- ✅ Notas de crédito siguen numeración existente (próxima: 203)
- ✅ Sin espacio para firma en PDF
- ✅ Sin facturación AFIP en esta app (lo hacen por afuera)
- ✅ Solo PDF, sin impresión física
- ✅ Productos libres en remitos → ofrecer guardarlos en inventario
- ✅ Descuento por defecto por cliente + descuento global del remito
- ✅ Logo oficial disponible (pendiente subirlo al proyecto)
- ✅ Base de clientes existente en Google Sheet (se va a importar)

---

## 8. PARÁMETROS DEL NEGOCIO (respondidos el 17/04/2026)

- ✅ **Publicaciones activas en ML:** 74 (cada una con SKU)
- ✅ **Total de publicaciones en ML (incluyendo inactivas):** más de 1.400
- ✅ **Planificado agregar:** ~5.000 productos adicionales con el sistema
- ✅ **Moneda de compra:** pesos argentinos (no se necesita ajuste por USD en precios)
- ✅ **Horario atención manual:** 8:00 a 18:30 hora Argentina
- ✅ **Horario bot automático:** fuera del rango manual (18:30 a 8:00 del día siguiente)

### Pendientes menores
- Subir el PNG del logo al proyecto (pendiente de upload real)
- Definir margen por defecto del módulo Precios (cuando lleguemos a Fase 4)
- Confirmar si hay una sola cuenta ML o varias

---

## 9. ROADMAP POR FASES

| Fase | Módulo | Duración estimada | Estado actual |
|---|---|---|---|
| 0 | Fundación: OAuth, BD, logs, estructura | ~1 semana | 🟡 En curso |
| 1 | Inventario + sync básico con ML | ~2 semanas | ⚪ Pendiente |
| 2 | Clientes + Remitos + Notas de Crédito | ~2 semanas | ⚪ Pendiente |
| 3 | Publicaciones ML masivas | ~3 semanas | ⚪ Pendiente |
| 4 | Precios (reglas + actualización masiva) | ~1 semana | ⚪ Pendiente |
| 5 | Respuestas automáticas ML | ~2 semanas | ⚪ Pendiente |
| 6 | Dashboard | ~1-2 semanas | ⚪ Pendiente |

Total estimado: ~12-14 semanas iterando.

---

## 10. ESTRUCTURA DEL PROYECTO (a construir)

```
primi-motors-ml/
├── .env                    # credenciales reales (no versionado)
├── .env.example            # template sin secretos
├── .gitignore
├── requirements.txt
├── README.md
├── PLAN_MAESTRO.md         # plan estructurado del proyecto
├── CONTEXTO_COMPLETO.md    # este archivo — contexto total de la sesión
├── config.py
├── db.py
├── logger.py
├── assets/
│   └── logo_primi.png      # (pendiente de subir)
├── ml/
│   ├── auth.py
│   ├── client.py
│   ├── stock_sync.py
│   ├── publicaciones.py
│   ├── precios.py
│   └── respuestas.py
├── inventory/
│   ├── models.py
│   ├── importer.py
│   └── search.py
├── clientes/
│   └── models.py
├── remitos/
│   ├── models.py
│   ├── pdf_generator.py
│   └── numbering.py
├── dashboard/
│   └── app.py
├── scheduler/
│   └── jobs.py
└── data/
    ├── primi_motors.db
    └── logs/
```

---

## 11. CRONOLOGÍA DE LA CONVERSACIÓN

### Sesión previa (con Claude Sonnet)
- Se definió el negocio, los productos, el objetivo (automatización ML)
- Se creó la app "PrimiMotors Automation" en developers.mercadolibre.com.ar
- Se configuraron permisos, flujos OAuth, redirect URI
- Se dejó planteado: 4 módulos iniciales (stock, publicaciones, precios, respuestas)
- Se generó el archivo `PRIMI_MOTORS_contexto_proyecto.md` como puente entre sesiones

### Sesión actual con Opus (17/04/2026)
1. Federico inició la sesión pasando el contexto de Sonnet.
2. Opus leyó el contexto, propuso mapa más amplio del sistema (6+ áreas) y formuló preguntas clave para planificar correctamente.
3. Federico pasó el **Client ID** (`3131016753852536`) y Opus explicó dónde encontrar el Client Secret en ML Developers.
4. Federico pasó el **Client Secret** (guardado en `.env`). Opus empezó a crear la estructura del proyecto.
5. Federico pausó el coding y pidió ampliar la visión: "hagamos la mejor app del planeta".
6. Opus presentó mapa completo con 9 áreas funcionales, preguntas clave y roadmap en fases.
7. Federico agregó **módulo 5: gestión de remitos y notas de crédito** con especificaciones detalladas.
8. Opus integró el módulo, destacó implicancias (stock multi-canal), propuso cambios al roadmap y planteó preguntas específicas del módulo.
9. Federico envió:
   - **Logo oficial** (visualizado en el chat)
   - Respuestas a las preguntas de remitos: existe Google Sheet "Remitos" con clientes, remitos históricos, NC y plantilla; solo remito (no factura AFIP); productos libres → ofrecer guardar en inventario; solo PDF sin impresión; sin espacio para firma.
   - Reformulación final de los 6 módulos de la app.
10. Opus buscó el Google Sheet en Drive, lo encontró, extrajo datos fiscales de la empresa (CUIT, dirección) y numeración actual (1549 / 202).
11. Opus creó los archivos base del proyecto: `.env`, `.env.example`, `.gitignore`, `README.md`, `PLAN_MAESTRO.md` y lista de tareas (Fases 0 a 6).
12. Federico pidió guardar todo lo hablado en un archivo de cowork → este documento.

---

## 12. PRÓXIMOS PASOS INMEDIATOS

1. **Federico:**
   - Subir el PNG del logo para embeberlo en PDFs
   - Responder las preguntas pendientes de la sección 8
2. **Opus (al retomar):**
   - Programar módulo OAuth completo (primer token + refresh automático)
   - Definir esquema SQLite inicial
   - Configurar logging y scheduler base
3. **Ambos:**
   - Una vez que Fase 0 esté cerrada, arrancar Fase 1 (Inventario)

---

*Fin del contexto completo — documento vivo, actualizable en cada sesión.*
