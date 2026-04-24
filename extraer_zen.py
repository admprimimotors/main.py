"""
extraer_zen.py
==============
CLI para extraer fichas técnicas + fotos del PDF de ZEN
(ImpulsoresdePartida.pdf).

Uso:
    (venv) PS > python extraer_zen.py                          # todo, guarda DB+fotos
    (venv) PS > python extraer_zen.py --dry-run                # no escribe nada, solo reporta
    (venv) PS > python extraer_zen.py --max 10                 # solo primeros 10 productos (test)
    (venv) PS > python extraer_zen.py --sin-fotos              # solo ficha técnica, no PNGs
    (venv) PS > python extraer_zen.py --pagina-inicio 284 --pagina-fin 290

El PDF por defecto se busca en:
    /sessions/youthful-bold-faraday/mnt/uploads/ImpulsoresdePartida.pdf
o se puede pasar con --pdf <ruta>.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from logger import get_logger
from zen_pdf import extractor as zpe

log = get_logger(__name__)


def _linea(c: str = "=", n: int = 72) -> str:
    return c * n


def main() -> int:
    p = argparse.ArgumentParser(
        description="Extrae ficha técnica + fotos del PDF ZEN hacia la DB local."
    )
    p.add_argument("--pdf", type=Path, default=zpe.PDF_PATH_DEFAULT,
                   help=f"Ruta al PDF (default: {zpe.PDF_PATH_DEFAULT})")
    p.add_argument("--pagina-inicio", type=int, default=zpe.PAGINA_INICIO_CATALOGO,
                   help=f"Primera página 1-indexed (default: {zpe.PAGINA_INICIO_CATALOGO})")
    p.add_argument("--pagina-fin", type=int, default=None,
                   help=f"Última página (default: {zpe.PAGINA_FIN_CATALOGO_DEFAULT})")
    p.add_argument("--max", dest="max_productos", type=int, default=None,
                   help="Corta después de N productos (útil para tests).")
    p.add_argument("--sin-fotos", action="store_true",
                   help="No extrae fotos, solo ficha técnica.")
    p.add_argument("--dry-run", action="store_true",
                   help="No escribe en DB ni guarda fotos — solo cuenta y reporta.")
    p.add_argument("--fotos-dir", type=Path, default=zpe.FOTOS_DIR,
                   help=f"Directorio destino para fotos (default: {zpe.FOTOS_DIR}).")
    args = p.parse_args()

    print(_linea())
    print("  EXTRACTOR ZEN — ficha técnica + fotos")
    print(_linea())
    print(f"  PDF           : {args.pdf}")
    print(f"  Páginas       : {args.pagina_inicio} → {args.pagina_fin or zpe.PAGINA_FIN_CATALOGO_DEFAULT}")
    print(f"  Máx. productos: {args.max_productos or 'sin límite'}")
    print(f"  Extraer fotos : {'sí' if not args.sin_fotos else 'no'}")
    print(f"  Dry-run       : {'sí (nada se persiste)' if args.dry_run else 'no'}")
    print(f"  Fotos dir     : {args.fotos_dir}")
    print(_linea())
    print()

    if not args.pdf.exists():
        print(f"✗ No existe el PDF en {args.pdf}")
        print()
        print("  Soluciones:")
        print("    a) Copiá 'ImpulsoresdePartida.pdf' a la carpeta data/ del proyecto.")
        print("    b) O pasá la ruta explícita con --pdf \"C:\\ruta\\al\\archivo.pdf\"")
        return 1

    resultado = zpe.extraer(
        pdf_path=args.pdf,
        pagina_inicio=args.pagina_inicio,
        pagina_fin=args.pagina_fin,
        max_productos=args.max_productos,
        extraer_fotos=not args.sin_fotos,
        guardar_db=not args.dry_run,
        dry_run=args.dry_run,
        fotos_dir=args.fotos_dir,
    )

    print(_linea())
    print("  RESULTADO")
    print(_linea())
    print(f"  Páginas procesadas            : {resultado.paginas_procesadas}")
    print(f"  Productos vistos              : {resultado.productos_vistos}")
    print(f"  Fichas técnicas persistidas   : {resultado.fichas_creadas}")
    print(f"  Fotos guardadas               : {resultado.fotos_guardadas}")
    print(f"  SKUs no encontrados en DB     : {len(resultado.skus_no_encontrados_en_db)}")
    if resultado.skus_no_encontrados_en_db:
        muestra = resultado.skus_no_encontrados_en_db[:20]
        print(f"    muestra (primeros 20): {', '.join(muestra)}")
        if len(resultado.skus_no_encontrados_en_db) > 20:
            print(f"    ... +{len(resultado.skus_no_encontrados_en_db) - 20} más")
    print(f"  Errores                       : {len(resultado.errores)}")
    for e in resultado.errores[:10]:
        print(f"    - {e}")
    if len(resultado.errores) > 10:
        print(f"    ... +{len(resultado.errores) - 10} más")
    print(_linea())

    if args.dry_run:
        print("  Dry-run: ningún dato fue persistido en DB ni en disco.")
    else:
        print("  Listo. Fichas en tabla `fichas_tecnicas`, fotos en disco.")
    print(_linea())

    return 0


if __name__ == "__main__":
    sys.exit(main())
