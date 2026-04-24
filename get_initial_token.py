"""
get_initial_token.py
====================
Script interactivo para obtener los primeros tokens OAuth de Mercado Libre.

Se ejecuta UNA sola vez al principio. Después, el refresh_token se renueva solo
cada vez que el sistema lo necesita.

USO:
    1. Asegurate de tener el archivo .env configurado con ML_CLIENT_ID y ML_CLIENT_SECRET.
    2. Corré: python get_initial_token.py
    3. Se abre una URL en el navegador — logueate en ML con la cuenta PRIMI y autorizá la app.
    4. Vas a ser redirigido a google.com con una URL larga tipo:
         https://www.google.com/?code=TG-xxxxxxxxx&state=primi-motors
    5. Copiá TODA esa URL y pegala en la terminal cuando te lo pida.
    6. Listo — los tokens quedan guardados en data/tokens.json.
"""

from __future__ import annotations

import webbrowser

from ml.auth import (
    construir_url_autorizacion,
    intercambiar_code_por_tokens,
    extraer_code_de_url,
    probar_conexion,
    cargar_tokens,
)


def main() -> None:
    print("=" * 70)
    print("   PRIMI MOTORS — Autenticación inicial con Mercado Libre")
    print("=" * 70)
    print()

    # Chequear si ya hay tokens
    existentes = cargar_tokens()
    if existentes is not None:
        print(f"⚠ Ya hay tokens guardados para user_id={existentes.user_id}.")
        rta = input("  ¿Querés sobrescribirlos y hacer login de nuevo? (s/N): ").strip().lower()
        if rta != "s":
            print("Cancelado. Se mantienen los tokens existentes.")
            return

    # Paso 1: construir y mostrar URL de autorización
    url = construir_url_autorizacion()
    print("PASO 1 — Autorización en Mercado Libre")
    print("-" * 70)
    print("Voy a abrir esta URL en tu navegador:")
    print()
    print(f"  {url}")
    print()
    print("Si no se abre automáticamente, copiala y pegala en el navegador.")
    print("Logueate con la cuenta PRIMI y hacé clic en 'Autorizar'.")
    print()
    input("Presioná ENTER cuando estés listo para abrirla...")

    try:
        webbrowser.open(url)
    except Exception:
        pass

    print()
    print("PASO 2 — Pegá la URL de vuelta")
    print("-" * 70)
    print("Después de autorizar, Mercado Libre te redirige a una URL que")
    print("empieza con https://www.google.com/?code=TG-... ")
    print()
    print("Copiá toda la barra de direcciones del navegador y pegala abajo:")
    print()
    url_redirigida = input("URL pegada aquí: ").strip()

    if not url_redirigida:
        print("❌ No pegaste nada. Abortando.")
        return

    # Paso 3: extraer code y intercambiar por tokens
    try:
        code = extraer_code_de_url(url_redirigida)
        print(f"✓ Code detectado: {code[:15]}...")
    except ValueError as e:
        print(f"❌ Error extrayendo el code: {e}")
        return

    print()
    print("PASO 3 — Intercambiando código por tokens...")
    print("-" * 70)
    try:
        tokens = intercambiar_code_por_tokens(code)
    except Exception as e:
        print(f"❌ Error al obtener tokens: {e}")
        return

    print()
    print(f"✓ Tokens obtenidos para user_id={tokens.user_id}")
    print(f"  Scope: {tokens.scope}")
    print(f"  Expira en ~6 horas (se renueva solo después)")
    print()

    # Paso 4: probar conexión
    print("PASO 4 — Probando conexión con la API de Mercado Libre...")
    print("-" * 70)
    try:
        datos = probar_conexion()
        print(f"✓ Conexión OK.")
        print(f"  Usuario: {datos.get('nickname')}")
        print(f"  Nombre: {datos.get('first_name', '')} {datos.get('last_name', '')}")
        print(f"  Email:  {datos.get('email')}")
        print(f"  Site:   {datos.get('site_id')}")
        print(f"  Tipo:   {datos.get('user_type')}")
    except Exception as e:
        print(f"⚠ Tokens guardados pero falló el test de conexión: {e}")
        return

    print()
    print("=" * 70)
    print("  ✅ LISTO — La autenticación quedó funcionando.")
    print("=" * 70)
    print()
    print("A partir de ahora, el sistema renueva los tokens solo.")
    print("Ya podés arrancar a usar los otros módulos.")


if __name__ == "__main__":
    main()
