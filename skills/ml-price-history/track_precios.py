#!/usr/bin/env python3
"""
Captura snapshots de precio de TODAS las publicaciones activas de ML y los
guarda en la tabla ml_price_snapshots. Diferencial: solo marca `is_change=1`
si el precio cambió respecto al último snapshot del mismo item.

Uso:
    python3 track_precios.py [--all|--changed-only]
    --all (default): guarda snapshot de TODOS los items (uso normal diario).
    --changed-only: guarda solo los items donde el precio cambió.
"""
from __future__ import annotations
import argparse
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path

BASE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(BASE))
from ml import client

DB_PATH = BASE / 'data' / 'primi_motors.db'

def asegurar_tabla(conn):
    """Crea la tabla si no existe (idempotente)."""
    conn.execute("""
    CREATE TABLE IF NOT EXISTS ml_price_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ml_item_id TEXT NOT NULL,
        title TEXT, sku TEXT,
        price REAL NOT NULL, base_price REAL, original_price REAL,
        currency TEXT, status TEXT,
        available_quantity INTEGER, sold_quantity INTEGER,
        ml_price_id TEXT, captured_at TEXT NOT NULL,
        is_change BOOLEAN NOT NULL DEFAULT 0
    )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS ix_snap_item ON ml_price_snapshots(ml_item_id, captured_at DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS ix_snap_change ON ml_price_snapshots(ml_item_id, is_change)")

def listar_activos(user_id):
    ids = []
    scroll_id = None
    while True:
        params = {'search_type':'scan','limit':100,'status':'active'}
        if scroll_id: params['scroll_id']=scroll_id
        r = client.get(f'/users/{user_id}/items/search', params=params)
        batch = r.get('results',[]) or []
        if not batch: break
        ids.extend(batch)
        scroll_id = r.get('scroll_id')
        if not scroll_id: break
    return ids

def traer_metadata(item_ids):
    """Multi-get de items en lotes de 20."""
    out = {}
    attrs = 'id,title,price,base_price,original_price,currency_id,status,available_quantity,sold_quantity,seller_sku,seller_custom_field,attributes'
    for i in range(0, len(item_ids), 20):
        batch = item_ids[i:i+20]
        r = client.get('/items', params={'ids':','.join(batch),'attributes':attrs})
        for entry in r:
            if entry.get('code') == 200:
                b = entry['body']
                sku = b.get('seller_sku') or b.get('seller_custom_field')
                if not sku:
                    for a in b.get('attributes',[]):
                        if a.get('id')=='SELLER_SKU':
                            sku = a.get('value_name'); break
                out[b['id']] = {
                    'title': b.get('title'),
                    'sku': sku,
                    'price': b.get('price'),
                    'base_price': b.get('base_price'),
                    'original_price': b.get('original_price'),
                    'currency': b.get('currency_id'),
                    'status': b.get('status'),
                    'available_quantity': b.get('available_quantity'),
                    'sold_quantity': b.get('sold_quantity'),
                }
    return out

def ultimo_snapshot(conn, item_id):
    r = conn.execute("""
        SELECT price FROM ml_price_snapshots
        WHERE ml_item_id = ? ORDER BY captured_at DESC, id DESC LIMIT 1
    """, (item_id,)).fetchone()
    return r[0] if r else None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--changed-only', action='store_true',
                    help='Solo guardar items con cambio de precio.')
    ap.add_argument('--user-id', type=int, default=None)
    args = ap.parse_args()

    if args.user_id is None:
        me = client.get('/users/me')
        args.user_id = me['id']
        print(f"User: {me.get('nickname')} (ID {me['id']})")

    print("\n[1/3] Listando publicaciones activas...")
    ids = listar_activos(args.user_id)
    print(f"      {len(ids)} items")

    print("\n[2/3] Trayendo precios actuales...")
    meta = traer_metadata(ids)
    print(f"      Metadata: {len(meta)}")

    print("\n[3/3] Guardando snapshots en DB local...")
    conn = sqlite3.connect(str(DB_PATH))
    asegurar_tabla(conn)
    now = datetime.now().isoformat(timespec='seconds')

    n_guardados = 0
    n_cambios = 0
    n_nuevos = 0
    cambios_detalle = []

    for iid, m in meta.items():
        if m['price'] is None: continue
        prev = ultimo_snapshot(conn, iid)
        es_cambio = (prev is None) or (abs((m['price']) - (prev or 0)) > 0.005)
        if prev is None: n_nuevos += 1
        if es_cambio and prev is not None: n_cambios += 1

        if args.changed_only and not es_cambio:
            continue

        conn.execute("""
            INSERT INTO ml_price_snapshots
                (ml_item_id,title,sku,price,base_price,original_price,
                 currency,status,available_quantity,sold_quantity,captured_at,is_change)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (iid, m['title'], m['sku'], m['price'], m['base_price'], m['original_price'],
              m['currency'], m['status'], m['available_quantity'], m['sold_quantity'],
              now, 1 if es_cambio else 0))
        n_guardados += 1

        if es_cambio and prev is not None and len(cambios_detalle) < 10:
            cambios_detalle.append({
                'iid': iid, 'title': m['title'],
                'antes': prev, 'ahora': m['price'],
                'delta_pct': 100*(m['price']-prev)/prev if prev else 0,
            })

    conn.commit()
    conn.close()

    print(f"\n{'='*60}")
    print(f"✓ FIN — snapshot {now}")
    print(f"  Items capturados: {n_guardados}")
    print(f"  Nuevos (baseline): {n_nuevos}")
    print(f"  Con cambio vs snapshot anterior: {n_cambios}")

    if cambios_detalle:
        print(f"\n  Muestra de cambios:")
        for c in cambios_detalle:
            arrow = '↑' if c['ahora']>c['antes'] else '↓'
            print(f"    {arrow} {c['iid']} ${c['antes']:,.0f} → ${c['ahora']:,.0f} ({c['delta_pct']:+.1f}%) | {c['title'][:60]}")

if __name__ == '__main__':
    main()
