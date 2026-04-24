# Primi Motors — Sistema de Gestión Integral

Aplicación local para la gestión integral del negocio de Primi Motors, con Mercado Libre como canal principal.

## Módulos

1. **Inventario** — stock real sincronizado con ML
2. **Publicaciones ML** — carga masiva optimizada
3. **Precios** — reglas de negocio y actualización masiva
4. **Respuestas automáticas** — Q&A nocturno con IA
5. **Remitos y Notas de Crédito** — con generación de PDF
6. **Dashboard** — métricas del negocio

## Documentación

Ver [PLAN_MAESTRO.md](./PLAN_MAESTRO.md) para el plan completo del proyecto.

## Requisitos

- Python 3.11+
- Cuenta de Mercado Libre con app creada en [developers.mercadolibre.com.ar](https://developers.mercadolibre.com.ar)

## Instalación (en desarrollo)

```bash
# 1. Clonar / copiar el proyecto
# 2. Crear entorno virtual
python -m venv venv
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

# 3. Instalar dependencias
pip install -r requirements.txt

# 4. Configurar credenciales
# Copiar .env.example a .env y completar los valores
cp .env.example .env

# 5. (Próximo) ejecutar flujo inicial de OAuth
python -m ml.auth init
```
