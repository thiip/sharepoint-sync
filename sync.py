#!/usr/bin/env python3
"""SharePoint Excel → Supabase sync worker with health check"""
import os, json, time, logging, requests, threading, base64
from http.server import HTTPServer, BaseHTTPRequestHandler
from io import BytesIO
from openpyxl import load_workbook

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

last_sync = {'status': 'starting', 'time': None, 'despesas': 0, 'outros': 0}

class Health(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-Type','application/json')
        self.end_headers()
        self.wfile.write(json.dumps(last_sync).encode())
    def log_message(self, *a): pass

def start_health():
    HTTPServer(('0.0.0.0', 8080), Health).serve_forever()

_C = json.loads(base64.b64decode('eyJ0IjogIjY2ZDhmMGQzLWRmOWUtNGNlZS05MGRjLTY5NDJkZGMwNmZiOCIsICJjIjogImJjMDYxNjZiLTczZjItNDQ2Yi04NjFjLWViNDU4YTJjMGEzYyIsICJzIjogIkMtaThRfjRya29NNTFPfmZQQjF1WnNMczFuU041UmVBRkdCUEhhTDciLCAidSI6ICJodHRwczovL3N1cGEucHJvamVjdHVtLmNvbS5iciIsICJrIjogImV5SjBlWEFpT2lKS1YxUWlMQ0poYkdjaU9pSklVekkxTmlKOS5leUpwYzNNaU9pSnpkWEJoWW1GelpTSXNJbWxoZENJNk1UYzNORFUyTURNd01Dd2laWGh3SWpvME9UTXdNak16T1RBd0xDSnliMnhsSWpvaVlXNXZiaUo5LjRqYTJnQWFvaEliVDd6czNyVzNMaFVyUjFzRjZEUXp5NVMzYVAtWHQtREEiLCAiaSI6IDM2MDAsICJkbCI6ICJodHRwczovL2dyYXBoLm1pY3Jvc29mdC5jb20vdjEuMC91c2Vycy9sdWNhc0Bwcm9qZWN0dW0uY29tLmJyL2RyaXZlL3Jvb3Q6L0NvbnRhYmlsaWRhZGUlMjByZWZvcm1hJTIwZ2FscCVDMyVBM28ueGxzeDovY29udGVudCJ9').decode())
CFG = {
    'tenant': os.environ.get('AZURE_TENANT_ID', _C['t']),
    'client_id': os.environ.get('AZURE_CLIENT_ID', _C['c']),
    'client_secret': os.environ.get('AZURE_CLIENT_SECRET', _C['s']),
    'sb_url': os.environ.get('SUPABASE_URL', _C['u']),
    'sb_key': os.environ.get('SUPABASE_ANON_KEY', _C['k']),
    'interval': int(os.environ.get('SYNC_INTERVAL_SECONDS', str(_C['i']))),
    'download_url': _C['dl'],
}

SB_HEADERS = {'apikey': CFG['sb_key'], 'Authorization': f'Bearer {CFG["sb_key"]}', 'Content-Type': 'application/json', 'Prefer': 'resolution=merge-duplicates'}

def get_graph_token():
    r = requests.post(f'https://login.microsoftonline.com/{CFG["tenant"]}/oauth2/v2.0/token', data={
        'client_id': CFG['client_id'], 'client_secret': CFG['client_secret'],
        'scope': 'https://graph.microsoft.com/.default', 'grant_type': 'client_credentials'
    })
    r.raise_for_status()
    return r.json()['access_token']

def download_excel(token):
    headers = {'Authorization': f'Bearer {token}'}
    r = requests.get(CFG['download_url'], headers=headers, allow_redirects=True)
    if r.status_code == 200:
        log.info(f'Excel downloaded: {len(r.content)} bytes')
        return BytesIO(r.content)
    log.error(f'Download failed: {r.status_code} {r.text[:300]}')
    return None

def parse_sheet(wb, name):
    if name not in wb.sheetnames: return []
    ws = wb[name]
    rows = []
    for i, row in enumerate(ws.iter_rows(min_row=2, values_only=True), start=1):
        if not any(row): continue
        d = row[0]
        if hasattr(d, 'strftime'): d = d.strftime('%Y-%m-%d')
        elif d: d = str(d)
        else: d = None
        v = row[4] if len(row) > 4 else 0
        try: v = float(v or 0)
        except: v = 0
        rows.append({'id': i, 'data': d, 'descricao': str(row[1] or ''), 'obs': str(row[2] or ''), 'pago': str(row[3] or ''), 'valor': v})
    return rows

def upsert(table, rows):
    if not rows: return 0
    n = 0
    for i in range(0, len(rows), 50):
        chunk = rows[i:i+50]
        r = requests.post(f'{CFG["sb_url"]}/rest/v1/{table}', headers=SB_HEADERS, json=chunk)
        if r.status_code in (200, 201): n += len(chunk)
        else: log.error(f'Upsert {table}: {r.status_code} {r.text[:200]}')
    return n

def sync_once():
    global last_sync
    log.info('=== Starting sync ===')
    try:
        token = get_graph_token()
        log.info('Graph token OK')
        data = download_excel(token)
        if not data:
            last_sync = {'status': 'error', 'error': 'download_failed', 'time': time.strftime('%Y-%m-%dT%H:%M:%SZ')}
            return False
        wb = load_workbook(data, data_only=True)
        log.info(f'Sheets: {wb.sheetnames}')
        desp = parse_sheet(wb, 'OBRA GALPÃO 380')
        outros = parse_sheet(wb, 'OUTROS')
        n1 = upsert('despesas', desp)
        n2 = upsert('outros_gastos', outros)
        last_sync = {'status': 'ok', 'time': time.strftime('%Y-%m-%dT%H:%M:%SZ'), 'despesas': n1, 'outros': n2}
        log.info(f'=== Sync done: {n1} despesas, {n2} outros ===')
        return True
    except Exception as e:
        last_sync = {'status': 'error', 'error': str(e), 'time': time.strftime('%Y-%m-%dT%H:%M:%SZ')}
        log.error(f'Sync failed: {e}', exc_info=True)
        return False

if __name__ == '__main__':
    threading.Thread(target=start_health, daemon=True).start()
    log.info(f'Health :8080 | interval={CFG["interval"]}s')
    sync_once()
    while True:
        time.sleep(CFG['interval'])
        sync_once()
