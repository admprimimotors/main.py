#!/usr/bin/env python3
"""
Carga compatibilidades de vehículos a publicaciones ML con tag
incomplete_compatibilities. Cruza títulos contra data/vehiculos-acara.json y
busca product_ids reales en el catálogo ML.

Uso:
    python3 cargar_compatibilidades.py [--dry-run] [--limit N]

Requiere:
    - ml/ módulo en el path (auth, client)
    - data/vehiculos-acara.json
"""
from __future__ import annotations
import argparse
import json
import os
import re
import sys
import time
from collections import defaultdict
from pathlib import Path

# Asegurar import del módulo ml/
BASE = Path(__file__).resolve().parents[2]  # CLAUDE P.M/
sys.path.insert(0, str(BASE))
from ml import client

SKILL_DIR = Path(__file__).resolve().parent
ACARA_PATH = BASE / 'data' / 'vehiculos-acara.json'
CONFIG_PATH = SKILL_DIR / 'config.json'
STATE_PATH = SKILL_DIR / 'state_compat.json'

ACARA = json.load(open(ACARA_PATH, encoding='utf-8'))
CONFIG = json.load(open(CONFIG_PATH, encoding='utf-8'))

# ----- Constantes -----
MARCA_ALIAS = {
    'CHEVROLET':'CHEVROLET','CHEVR':'CHEVROLET','CHEV':'CHEVROLET','CHEVY':'CHEVROLET','GM':'CHEVROLET',
    'FIAT':'FIAT','FORD':'FORD','VW':'VOLKSWAGEN','VOLKSWAGEN':'VOLKSWAGEN','AUDI':'AUDI',
    'PEUGEOT':'PEUGEOT','PEUG':'PEUGEOT','RENAULT':'RENAULT','REN':'RENAULT',
    'CITROEN':'CITROËN','CITROËN':'CITROËN','TOYOTA':'TOYOTA','HYUNDAI':'HYUNDAI',
    'NISSAN':'NISSAN','MERCEDES':'MERCEDES-BENZ','MB':'MERCEDES-BENZ','M.BENZ':'MERCEDES-BENZ',
    'ISUZU':'ISUZU','MITSUBISHI':'MITSUBISHI','MAZDA':'MAZDA','SUBARU':'SUBARU',
    'SUZUKI':'SUZUKI','SEAT':'SEAT','HONDA':'HONDA','OPEL':'OPEL','ROVER':'ROVER',
    'BAIC':'BAIC','BYD':'BYD','CHERY':'CHERY','DAEWOO':'DAEWOO','DAIHATSU':'DAIHATSU',
    'JAC':'JAC','KIA':'KIA',
}

NO_VEHICLE_BRANDS = {
    'CUMMINS','PERKINS','MWM','DEUTZ','JOHN','JOHN DEERE','YANMAR','KUBOTA','CASE',
    'INDENOR','BEDFORD','BORGWARD','IHC','MAXION','HOLSET','LOMBARDINI','LANZ',
    'NEW HOLLAND','CHALLENGER','APACHE','CLAAS','PAUNY','VALTRA','AGCO','AGRINAR',
    'BENDIX','CLAYTON','MIDLAND','WABCO','WESTINGHOUSE','TENSA','MARELLI','JOHNSON',
    'MERCURY','YAMAHA','VARGA','VM','SCANIA','VOLVO','IVECO','LIFAN','SHINERAY',
    'DACIA','LADA',
}

FALLBACK_MODELOS = {
    'CHEVROLET': ['Corsa','Astra','Aveo','Agile'],
    'FIAT': ['Palio','Uno','Siena','Duna'],
    'FORD': ['Falcon','Focus','Fiesta','Ka'],
    'VOLKSWAGEN': ['Gol','Polo','Suran','Voyage'],
    'PEUGEOT': ['208','206','405','504'],
    'RENAULT': ['Clio','Megane','9','11'],
    'CITROËN': ['C3','C4','Berlingo','Xsara'],
    'TOYOTA': ['Corolla','Hilux','Etios'],
    'HYUNDAI': ['Accent','Atos','Elantra'],
    'NISSAN': ['Tiida','Sentra'],
    'MERCEDES-BENZ': ['Sprinter','190','200'],
    'AUDI': ['A3','A4','80','90'],
    'OPEL': ['Corsa','Astra'],
    'SEAT': ['Ibiza','Cordoba'],
}

# Modelo → marca para búsqueda inversa
MODEL_TO_BRAND = {}
for marca, modelos in ACARA.items():
    for m in modelos:
        if len(m) >= 3:
            MODEL_TO_BRAND.setdefault(m.lower(), []).append((marca, m))


def detect_marca_modelos(titulo):
    """Devuelve (marca, [modelos], source). marca=None si no aplica."""
    t = titulo.upper()
    t_lower = titulo.lower()
    
    # 1) Marca explícita
    marca = None
    for alias, m in MARCA_ALIAS.items():
        if re.search(r'\b' + re.escape(alias) + r'\b', t):
            marca = m
            break
    
    # 2) Marca no-automotriz → descartar
    for nv in NO_VEHICLE_BRANDS:
        if re.search(r'\b' + re.escape(nv) + r'\b', t):
            if marca is None:
                return (None, [], 'no_vehicle_brand')
    
    # 3) Marca conocida → modelos
    if marca:
        encontrados = []
        for mdl in ACARA.get(marca, []):
            if len(mdl) < 2: continue
            if re.search(r'\b' + re.escape(mdl.lower()) + r'\b', t_lower):
                encontrados.append(mdl)
        if encontrados:
            return (marca, encontrados, 'marca+modelos_titulo')
        return (marca, FALLBACK_MODELOS.get(marca, [])[:3], 'marca+fallback')
    
    # 4) Sin marca → buscar modelos
    candidates = defaultdict(list)
    for ml, brand_list in MODEL_TO_BRAND.items():
        if len(ml) < 3: continue
        if re.search(r'\b' + re.escape(ml) + r'\b', t_lower):
            for marca_acara, mdl_orig in brand_list:
                candidates[marca_acara].append(mdl_orig)
    if candidates:
        best = max(candidates.keys(), key=lambda k: len(candidates[k]))
        return (best, list(set(candidates[best])), 'modelos_titulo')
    
    return (None, [], 'no_match')


def buscar_productos(cache, marca, modelo, max_results=2):
    """Busca product_ids en ML para marca+modelo. Usa cache."""
    key = f"{marca.lower()}|{modelo.lower()}"
    if key in cache:
        return cache[key]
    try:
        r = client.get('/products/search', params={
            'site_id':'MLA','domain_id':CONFIG['domain_id_default'],
            'q':f"{marca} {modelo}",'limit':max_results,
        })
        ids = []
        for p in r.get('results',[]):
            ba = next((a for a in p.get('attributes',[]) if a.get('id')=='BRAND'), None)
            if ba and ba.get('value_name','').lower() == marca.lower():
                ids.append({'id':p['id'],'name':p.get('name','')})
        cache[key] = ids
        return ids
    except Exception:
        cache[key] = []
        return []


def listar_items_afectados(user_id):
    """Scan items con tag incomplete_compatibilities."""
    ids = []
    scroll_id = None
    while True:
        params = {'search_type':'scan','limit':100,'tags':'incomplete_compatibilities'}
        if scroll_id: params['scroll_id'] = scroll_id
        r = client.get(f'/users/{user_id}/items/search', params=params)
        batch = r.get('results',[]) or []
        if not batch: break
        ids.extend(batch)
        scroll_id = r.get('scroll_id')
        if not scroll_id: break
    return ids


def cargar_user_product_ids(item_ids):
    """Multi-get para conseguir user_product_id de cada item."""
    upid_map = {}
    for i in range(0, len(item_ids), 20):
        batch = item_ids[i:i+20]
        r = client.get('/items', params={'ids':','.join(batch),'attributes':'id,user_product_id,title,status'})
        for entry in r:
            if entry.get('code') == 200:
                b = entry['body']
                upid_map[b['id']] = b
    return upid_map


def aplicar_compatibilidades(upid, product_ids, dry_run=False):
    if dry_run:
        return {'dry_run':True, 'count':len(product_ids)}
    body = {
        'domain_id': CONFIG['domain_id_default'],
        'products': [{'id': p['id']} for p in product_ids],
    }
    return client.post(f'/user-products/{upid}/compatibilities', json_body=body)


def cargar_state():
    if STATE_PATH.exists():
        return json.load(open(STATE_PATH))
    return {'done':{}}


def guardar_state(state):
    with open(STATE_PATH,'w',encoding='utf-8') as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--dry-run', action='store_true', help='Mostrar plan sin aplicar')
    ap.add_argument('--limit', type=int, default=None, help='Limitar a N items')
    ap.add_argument('--user-id', type=int, default=None, help='User ID ML (default: /users/me)')
    args = ap.parse_args()
    
    if args.user_id is None:
        me = client.get('/users/me')
        args.user_id = me['id']
        print(f"User: {me.get('nickname')} ({me['id']})")
    
    print("\n1) Listando items con incomplete_compatibilities...")
    ids = listar_items_afectados(args.user_id)
    print(f"   Total: {len(ids)}")
    
    print("\n2) Obteniendo detalles (titulo + user_product_id)...")
    info = cargar_user_product_ids(ids)
    print(f"   Detalles: {len(info)}")
    
    print("\n3) Detectando marca y modelos...")
    detected = []
    for iid, b in info.items():
        marca, modelos, src = detect_marca_modelos(b['title'])
        if marca and modelos:
            detected.append({
                'item_id': iid, 'title': b['title'], 'upid': b.get('user_product_id'),
                'marca': marca, 'modelos': modelos, 'source': src,
            })
    print(f"   Items con marca+modelos detectados: {len(detected)}")
    
    if args.limit:
        detected = detected[:args.limit]
    
    print(f"\n4) Buscando product_ids en catálogo ML...")
    cache = {}
    for d in detected:
        pids = []
        seen = set()
        for modelo in d['modelos'][:CONFIG['max_modelos_por_titulo']]:
            for p in buscar_productos(cache, d['marca'], modelo,
                                       max_results=CONFIG['max_products_per_item']):
                if p['id'] not in seen:
                    seen.add(p['id'])
                    pids.append(p)
            if len(pids) >= CONFIG['max_products_per_item']:
                break
        d['products'] = pids[:CONFIG['max_products_per_item']]
    
    listos = [d for d in detected if len(d['products']) >= CONFIG['min_products_per_item']]
    print(f"   Listos (≥{CONFIG['min_products_per_item']} products): {len(listos)}")
    print(f"   Cache queries únicas: {len(cache)}")
    
    if args.dry_run:
        print("\n=== DRY RUN: no se aplicará ===")
        for d in listos[:5]:
            print(f"  {d['item_id']} | {d['marca']} | {d['modelos'][:3]}")
            for p in d['products']:
                print(f"    → {p['id']}: {p['name'][:60]}")
        return
    
    print(f"\n5) Aplicando a ML...")
    state = cargar_state()
    n_ok = 0
    n_err = 0
    for d in listos:
        if d['item_id'] in state['done']:
            continue
        try:
            r = aplicar_compatibilidades(d['upid'], d['products'])
            state['done'][d['item_id']] = {'status':'OK','count':r.get('created_compatibilities_count', len(d['products']))}
            n_ok += 1
        except Exception as e:
            state['done'][d['item_id']] = {'status':'ERROR','error':str(e)[:200]}
            n_err += 1
        if (n_ok + n_err) % 50 == 0:
            guardar_state(state)
            print(f"   ...{n_ok+n_err}/{len(listos)} | OK={n_ok} ERR={n_err}")
    guardar_state(state)
    
    print(f"\n✓ FIN")
    print(f"  OK: {n_ok}")
    print(f"  ERR: {n_err}")
    print(f"  Total compatibilidades creadas: {sum(v.get('count',0) for v in state['done'].values() if v.get('status')=='OK')}")


if __name__ == '__main__':
    main()
