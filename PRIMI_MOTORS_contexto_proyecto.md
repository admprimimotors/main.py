# PRIMI MOTORS — Contexto completo del proyecto
*Resumen de sesión para continuar con Claude Opus*

---

## 1. DATOS DEL NEGOCIO

- **Nombre comercial:** Primi Motors
- **Dueño:** Federico Ignacio Primi (33 años, argentino)
- **Socia/responsable fiscal:** Tatiana Marcela Montes (pareja y compañera de trabajo)
- **Email:** adm.primimotors@gmail.com
- **Rubro:** Venta de repuestos y autopartes para autos — toda Argentina
- **Canales de venta:** Principalmente Mercado Libre
- **Antigüedad:** 6 años en el rubro

### Productos actuales (especialidad histórica — motor):
- Pistones
- Blocks de motor
- Tapas de cilindro
- Árboles de levas
- Piezas del interior del motor en general
- Para: vehículos livianos, pesados, maquinaria agrícola, autoelevadores

### Rubros nuevos que están incorporando:
- Embragues
- Suspensión
- Tren delantero
- Frenos

---

## 2. OBJETIVO PRINCIPAL DEL PROYECTO

Construir un **sistema de automatización de Mercado Libre** que corra en la computadora de Federico (local, sin servidor externo), usando la API oficial de ML (MELI API).

### Prioridades del sistema (en orden):
1. **Automatización de stock** — sincronizar inventario real con ML; pausar cuando no hay stock, reactivar cuando vuelve
2. **Publicaciones automáticas masivas** — subir productos desde catálogos (con título optimizado, descripción, ficha técnica, fotos, precio)
3. **Gestión de precios** — actualización masiva según reglas de negocio (costo + margen)
4. **Respuestas automáticas a preguntas** — en horarios nocturnos, usando IA para responder con info de los catálogos; las preguntas complejas se guardan para respuesta manual

---

## 3. ARQUITECTURA DECIDIDA

- **Lenguaje:** Python
- **Modalidad:** Script local que corre en la PC de Federico
- **Autenticación ML:** OAuth 2.0 con Authorization Code + Refresh Token (una sola autenticación manual, después se renueva solo)
- **Modo de operación:** Polling (el sistema consulta la API cada X minutos — NO usa webhooks)
- **Módulos planificados:**
  - Módulo 1: Stock
  - Módulo 2: Publicaciones
  - Módulo 3: Precios
  - Módulo 4: Respuestas automáticas

---

## 4. CONFIGURACIÓN DE LA APP EN MERCADO LIBRE DEVELOPERS

**Portal:** developers.mercadolibre.com.ar  
**Cuenta ML:** PRIMI

### App creada con la siguiente configuración:
- **Nombre:** PrimiMotors Automation
- **Redirect URI:** `https://www.google.com`
- **Notifications Callback URL:** `https://webhook.site/3cf542f0-aa20-482f-b261-ca165dd424bc` *(placeholder, no se usa)*

### Flujos OAuth habilitados:
- ✅ Authorization Code
- ✅ Client Credentials
- ✅ Refresh Token

### PKCE: NO

### Negocio: Mercado Libre ✅

### Permisos configurados:
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

### Tópicos: todos desmarcados (se usa polling, no webhooks)

---

## 5. PENDIENTE — PRÓXIMOS PASOS

### Inmediato:
- [ ] Federico debe pasar el **App ID** y el **Secret Key** de la app creada en ML Developers
- [ ] Con esos datos, programar el módulo de autenticación OAuth (obtener el primer Access Token + Refresh Token)

### Después:
- [ ] Subir los catálogos de productos (en el formato que los tenga: Excel, PDF, Word)
- [ ] Definir las reglas de precios (margen sobre costo, etc.)
- [ ] Programar Módulo 1: Stock
- [ ] Programar Módulo 2: Publicaciones
- [ ] Programar Módulo 3: Precios
- [ ] Programar Módulo 4: Respuestas automáticas

---

## 6. CÓMO CONTINUAR CON CLAUDE OPUS

Al iniciar la nueva conversación, pegá este mensaje al principio:

> "Hola, soy Federico de Primi Motors. Te paso el contexto completo de nuestro proyecto para que puedas continuar desde donde lo dejamos. [pegar contenido de este archivo]"

Luego tené a mano el **App ID** y **Secret Key** de la app de ML para pegarlo cuando Claude te lo pida.

---

*Archivo generado el 17/04/2026*
