#!/usr/bin/env python3
"""SharePoint Excel → Supabase sync worker with health check"""
import os, json, time, logging, requests, threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from io import BytesIO
from openpyxl import load_workbook

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

last_sync = {'status': 'starting', 'time': None, 'despesas': 0, 'outros': 0}

# Health check server
class Health(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-Type','application/json')
        self.end_headers()
        self.wfile.write(json.dumps(last_sync).encode())
    def log_message(self, *a): pass

def start_health():
    HTTPServer(('0.0.0.0', 8080), Health).serve_forever()

# Azure AD / Graph
TENANT_ID = os.environ['AZURE_TENANT_ID']
CLIENT_ID = os.environ['AZURE_CLIENT_ID']
CLIENT_SECRET = os.environ['AZURE_CLIENT_SECRET']
SITE_HOST = os.environ.get('SHAREPOINT_SITE', 'projectumm-my.sharepoint.com')
USER_PATH = os.environ.get('SHAREPOINT_USER_PATH', '/personal/lucas_projectum_com_br1')
FILE_PATH = os.environ.get('SHAREPOINT_FILE', 'Documents/Contabilidade reforma galpão.xlsx')

SB_URL = os.environ['SUPABASE_URL']
SB_KEY = os.environ['SUPABASE_ANON_KEY']
SB_HEADERS = {'apikey': SB_KEY, 'Authorization': f'Bearer {SB_KEY}', 'Content-Type': 'application/json', 'Prefer': 'resolution=merge-duplicates'}
SYNC_INTERVAL = int(os.environ.get('SYNC_INTERVAL_SECONDS', '3600'))

def get_graph_token():
    r = requests.post(f'https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token', data={
        'client_id': CLIENT_ID, 'client_secret': CLIENT_SECRET,
        'scope': 'https://graph.microsoft.com/.default', 'grant_type': 'client_credentials'
    })
    r.raise_for_status()
    return r.json()['access_token']

def download_excel(token):
    headers = {'Authorization': f'Bearer {token}'}
    urls = [
        f'https://graph.microsoft.com/v1.0/sites/{SITE_HOST}:{USER_PATH}:/drive/root:/{FILE_PATH}:/content',
        f'https://graph.microsoft.com/v1.0/users/lucas@projectum.com.br/drive/root:/{FILE_PATH}:/content',
        f'https://graph.microsoft.com/v1.0/sites/{SITE_HOST},{USER_PATH}/drive/root:/{FILE_PATH}:/content',
    ]
    for url in urls:
        r = requests.get(url, headers=headers, allow_redirects=True)
        if r.status_code == 200:
            log.info(f'Excel downloaded: {len(r.content)} bytes from {url[:80]}')
            return BytesIO(r.content)
        log.warning(f'Try failed ({r.status_code}): {url[:80]}')
    log.error('All download attempts failed')
    return None

def parse_despesas(wb):
    ws = wb['OBRA GALPÃO 380']
    rows = []
    for i, row in enumerate(ws.iter_rows(min_row=2, values_only=True), start=1):
        if not any(row): continue
        data_val = row[0]
        if hasattr(data_val, 'strftime'): data_val = data_val.strftime('%Y-%m-%d')
        elif data_val: data_val = str(data_val)
        else: data_val = None
        valor = row[4] if len(row) > 4 else 0
        try: valor = float(valor or 0)
        except: valor = 0
        rows.append({'id': i, 'data': data_val, 'descricao': str(row[1] or ''), 'obs': str(row[2] or ''), 'pago': str(row[3] or ''), 'valor': valor})
    return rows

def parse_outros(wb):
    if 'OUTROS' not in wb.sheetnames: return []
    ws = wb['OUTROS']
    rows = []
    for i, row in enumerate(ws.iter_rows(min_row=2, values_only=True), start=1):
        if not any(row): continue
        data_val = row[0]
        if hasattr(data_val, 'strftime'): data_val = data_val.strftime('%Y-%m-%d')
        elif data_val: data_val = str(data_val)
        else: data_val = None
        valor = row[4] if len(row) > 4 else 0
        try: valor = float(valor or 0)
        except: valor = 0
        rows.append({'id': i, 'data': data_val, 'descricao': str(row[1] or ''), 'obs': str(row[2] or ''), 'pago': str(row[3] or ''), 'valor': valor})
    return rows

def upsert_supabase(table, rows):
    if not rows: return 0
    total = 0
    for i in range(0, len(rows), 50):
        chunk = rows[i:i+50]
        r = requests.post(f'{SB_URL}/rest/v1/{table}', headers=SB_HEADERS, json=chunk)
        if r.status_code in (200, 201): total += len(chunk)
        else: log.error(f'Upsert {table} failed: {r.status_code} {r.text[:200]}')
    return total

def sync_once():
    global last_sync
    log.info('=== Starting sync ===')
    try:
        token = get_graph_token()
        excel_data = download_excel(token)
        if not excel_data:
            last_sync = {'status': 'error', 'error': 'download_failed', 'time': time.strftime('%Y-%m-%dT%H:%M:%SZ')}
            return False
        wb = load_workbook(excel_data, data_only=True)
        despesas = parse_despesas(wb)
        outros = parse_outros(wb)
        n1 = upsert_supabase('despesas', despesas)
        n2 = upsert_supabase('outros_gastos', outros)
        last_sync = {'status': 'ok', 'time': time.strftime('%Y-%m-%dT%H:%M:%SZ'), 'despesas': n1, 'outros': n2}
        log.info(f'=== Sync complete: {n1} despesas, {n2} outros ===')
        return True
    except Exception as e:
        last_sync = {'status': 'error', 'error': str(e), 'time': time.strftime('%Y-%m-%dT%H:%M:%SZ')}
        log.error(f'Sync failed: {e}', exc_info=True)
        return False

if __name__ == '__main__':
    threading.Thread(target=start_health, daemon=True).start()
    log.info(f'Health check on :8080 | Sync interval: {SYNC_INTERVAL}s')
    sync_once()
    while True:
        time.sleep(SYNC_INTERVAL)
        sync_once()
