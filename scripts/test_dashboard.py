#!/usr/bin/env python3
"""
Test Initiation Dashboard — Bedrock Traffic Shaper
Local HTTP server serving an interactive dashboard for running validation tests.
Usage: python scripts/test_dashboard.py [--port 8080]
"""

import http.server
import json
import os
import sys
import time
import urllib.request
import urllib.error
import ssl
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add lambda layer to Python path for shared_service
layer_path = os.path.join(os.path.dirname(__file__), '..', 'infrastructure', 'lambda_layer', 'python')
sys.path.insert(0, layer_path)

import boto3
from shared_service import DynamoService
import config_loader

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

CFG = config_loader.load_config()  # Also sets AWS_DEFAULT_REGION
CLOUDFRONT_URL = CFG.get('CLOUDFRONT_URL', '').rstrip('/')
DLQ_URL = CFG.get('DLQ_URL', '')
SINGLE_TABLE_NAME = CFG.get('SINGLE_TABLE_NAME', '')
DASHBOARD_URL = CFG.get('DASHBOARD_URL', '')
AWS_REGION = CFG.get('AWS_REGION', 'us-east-1')
DEFAULT_MODEL = CFG.get('BEDROCK_MODEL_ID', 'us.amazon.nova-2-lite-v1:0')

# AWS clients
sqs = boto3.client('sqs', region_name=AWS_REGION)
dynamo_service = DynamoService(single_table_name=SINGLE_TABLE_NAME)

# SSL context for CloudFront HTTPS calls
ssl_ctx = ssl.create_default_context()

# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

def http_request(url, headers=None, method='POST', body=None, timeout=30):
    """Make an HTTP request and return (status, headers, body, elapsed_ms)."""
    req = urllib.request.Request(url, method=method, headers=headers or {})
    if body:
        req.data = json.dumps(body).encode()
        req.add_header('Content-Type', 'application/json')
    start = time.time()
    try:
        resp = urllib.request.urlopen(req, context=ssl_ctx, timeout=timeout)  # nosec B310  # nosemgrep: dynamic-urllib-use-detected  # static internal execute-api/CloudFront host, not user-controlled
        elapsed = (time.time() - start) * 1000
        resp_headers = dict(resp.getheaders())
        resp_body = resp.read().decode('utf-8', errors='replace')
        return {
            'status': resp.status,
            'headers': resp_headers,
            'body': resp_body[:4000],
            'elapsed_ms': round(elapsed, 1)
        }
    except urllib.error.HTTPError as e:
        elapsed = (time.time() - start) * 1000
        resp_body = e.read().decode('utf-8', errors='replace') if e.fp else ''
        return {
            'status': e.code,
            'headers': dict(e.headers.items()) if e.headers else {},
            'body': resp_body[:4000],
            'elapsed_ms': round(elapsed, 1)
        }
    except Exception as e:
        elapsed = (time.time() - start) * 1000
        return {
            'status': 0,
            'headers': {},
            'body': str(e),
            'elapsed_ms': round(elapsed, 1)
        }


def run_smoke_test(model_id=None):
    """Send 1 request through CloudFront → API GW → Step Functions → Bedrock."""
    model = model_id or DEFAULT_MODEL
    url = f"{CLOUDFRONT_URL}/invoke"
    return http_request(url, headers={
        'x-model-id': model,
        'Content-Type': 'application/json'
    }, body={
        'prompt': 'Say "smoke test passed" in exactly 5 words.',
        'max_tokens': 50,
        'temperature': 0.1
    })


def run_cff_no_header():
    """Send request without x-model-id — should get 400 from CFF."""
    url = f"{CLOUDFRONT_URL}/invoke"
    return http_request(url, headers={
        'Content-Type': 'application/json'
    }, body={'prompt': 'test', 'max_tokens': 10})


def run_cff_with_header(model_id=None):
    """Send request with x-model-id — should pass CFF validation."""
    model = model_id or DEFAULT_MODEL
    url = f"{CLOUDFRONT_URL}/invoke"
    return http_request(url, headers={
        'x-model-id': model,
        'Content-Type': 'application/json'
    }, body={
        'prompt': 'Say "CFF validation passed".',
        'max_tokens': 30,
        'temperature': 0.1
    })


def run_burst_test(count=5, model_id=None):
    """Send N requests in parallel through the full pipeline."""
    model = model_id or DEFAULT_MODEL
    count = min(max(int(count), 1), 50)
    results = []
    with ThreadPoolExecutor(max_workers=min(count, 20)) as pool:
        futures = {
            pool.submit(run_smoke_test, model): i
            for i in range(count)
        }
        for future in as_completed(futures):
            idx = futures[future]
            try:
                r = future.result()
                r['request_index'] = idx
                results.append(r)
            except Exception as e:
                results.append({
                    'request_index': idx,
                    'status': 0,
                    'body': str(e),
                    'elapsed_ms': 0
                })
    results.sort(key=lambda r: r.get('request_index', 0))
    return results


def get_dlq_status():
    """Get DLQ approximate message count."""
    if not DLQ_URL:
        return {'error': 'DLQ_URL not configured'}
    attrs = sqs.get_queue_attributes(
        QueueUrl=DLQ_URL,
        AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
    )['Attributes']
    return {
        'visible': int(attrs.get('ApproximateNumberOfMessages', 0)),
        'in_flight': int(attrs.get('ApproximateNumberOfMessagesNotVisible', 0))
    }


def get_queue_depth(model_id=None):
    """Get DynamoDB queue depth for a model."""
    model = model_id or DEFAULT_MODEL
    try:
        depth = dynamo_service.get_queue_depth(model)
        return {'model_id': model, 'queue_depth': depth}
    except Exception as e:
        return {'model_id': model, 'queue_depth': 0, 'error': str(e)}


def get_system_config(model_id=None):
    """Get current model config and system info."""
    model = model_id or DEFAULT_MODEL
    try:
        config = dynamo_service.get_model_config(model)
        return {
            'model_id': model,
            'burst_capacity': config.get('burst_capacity'),
            'queue_capacity': config.get('queue_capacity'),
            'rpm_limit': config.get('rpm_limit'),
            'tpm_burst_capacity': config.get('tpm_burst_capacity', 0),
            'cloudfront_url': CLOUDFRONT_URL,
            'dashboard_url': DASHBOARD_URL,
        }
    except Exception as e:
        return {'error': str(e), 'model_id': model}


# ---------------------------------------------------------------------------
# HTTP Handler
# ---------------------------------------------------------------------------

class DashboardHandler(http.server.BaseHTTPRequestHandler):

    def log_message(self, format, *args):
        # Quieter logging
        if '/api/' in args[0]:
            return
        super().log_message(format, *args)

    def _json_response(self, data, status=200):
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(data, default=str).encode())

    def _read_body(self):
        length = int(self.headers.get('Content-Length', 0))
        if length:
            return json.loads(self.rfile.read(length))
        return {}

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def do_GET(self):
        if self.path == '/' or self.path == '/index.html':
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.end_headers()
            self.wfile.write(DASHBOARD_HTML_BYTES)
        elif self.path == '/api/dlq-status':
            self._json_response(get_dlq_status())
        elif self.path.startswith('/api/queue-depth'):
            self._json_response(get_queue_depth())
        elif self.path.startswith('/api/config'):
            self._json_response(get_system_config())
        else:
            self.send_error(404)

    def do_POST(self):
        body = self._read_body()
        model_id = body.get('model_id')

        if self.path == '/api/smoke-test':
            self._json_response(run_smoke_test(model_id))
        elif self.path == '/api/cff-no-header':
            self._json_response(run_cff_no_header())
        elif self.path == '/api/cff-with-header':
            self._json_response(run_cff_with_header(model_id))
        elif self.path == '/api/burst-test':
            count = body.get('count', 5)
            self._json_response(run_burst_test(count, model_id))
        else:
            self.send_error(404)


# ---------------------------------------------------------------------------
# Dashboard HTML
# ---------------------------------------------------------------------------

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Bedrock Traffic Shaper — Test Dashboard</title>
<style>
  :root {
    --bg: #0f1117;
    --surface: #1a1d27;
    --surface2: #242836;
    --border: #2d3348;
    --text: #e4e7ef;
    --text2: #8b90a5;
    --accent: #6c8cff;
    --green: #4ade80;
    --red: #f87171;
    --yellow: #fbbf24;
    --orange: #fb923c;
  }
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body {
    font-family: 'SF Mono', 'Cascadia Code', 'JetBrains Mono', monospace;
    background: var(--bg);
    color: var(--text);
    min-height: 100vh;
    font-size: 13px;
  }
  header {
    background: var(--surface);
    border-bottom: 1px solid var(--border);
    padding: 16px 24px;
    display: flex;
    align-items: center;
    justify-content: space-between;
  }
  header h1 {
    font-size: 16px;
    font-weight: 600;
    letter-spacing: 0.5px;
  }
  header h1 span { color: var(--accent); }
  .header-links a {
    color: var(--accent);
    text-decoration: none;
    font-size: 12px;
    margin-left: 16px;
    padding: 6px 12px;
    border: 1px solid var(--border);
    border-radius: 6px;
    transition: all 0.15s;
  }
  .header-links a:hover {
    background: var(--accent);
    color: var(--bg);
  }
  .layout {
    display: grid;
    grid-template-columns: 320px 1fr;
    min-height: calc(100vh - 56px);
  }
  .sidebar {
    background: var(--surface);
    border-right: 1px solid var(--border);
    padding: 16px;
    overflow-y: auto;
  }
  .status-cards {
    display: flex;
    flex-direction: column;
    gap: 12px;
    margin-bottom: 20px;
  }
  .status-card {
    background: var(--surface2);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 12px;
  }
  .status-card .label { font-size: 10px; color: var(--text2); text-transform: uppercase; letter-spacing: 1px; }
  .status-card .value { font-size: 22px; font-weight: 700; margin-top: 4px; }
  .status-card .sub { font-size: 11px; color: var(--text2); margin-top: 2px; }
  .test-section { margin-bottom: 16px; }
  .test-section h3 {
    font-size: 11px;
    color: var(--text2);
    text-transform: uppercase;
    letter-spacing: 1px;
    margin-bottom: 8px;
    padding-bottom: 4px;
    border-bottom: 1px solid var(--border);
  }
  .test-btn {
    display: flex;
    align-items: center;
    justify-content: space-between;
    width: 100%;
    padding: 10px 12px;
    background: var(--surface2);
    border: 1px solid var(--border);
    border-radius: 6px;
    color: var(--text);
    font-family: inherit;
    font-size: 12px;
    cursor: pointer;
    margin-bottom: 6px;
    transition: all 0.15s;
    text-align: left;
  }
  .test-btn:hover { border-color: var(--accent); background: #1e2235; }
  .test-btn:disabled { opacity: 0.5; cursor: wait; }
  .test-btn .badge {
    font-size: 10px;
    padding: 2px 6px;
    border-radius: 4px;
    font-weight: 600;
  }
  .badge-get { background: #1a3a2a; color: var(--green); }
  .badge-post { background: #3a2a1a; color: var(--orange); }
  .main {
    padding: 20px;
    overflow-y: auto;
  }
  .results-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 16px;
  }
  .results-header h2 { font-size: 14px; font-weight: 600; }
  #clearBtn {
    font-family: inherit;
    font-size: 11px;
    color: var(--text2);
    background: none;
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 4px 10px;
    cursor: pointer;
  }
  #clearBtn:hover { color: var(--text); border-color: var(--text2); }
  .result-entry {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    margin-bottom: 12px;
    overflow: hidden;
  }
  .result-header {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 10px 14px;
    font-size: 12px;
    border-bottom: 1px solid var(--border);
    cursor: pointer;
    user-select: none;
  }
  .result-header:hover { background: var(--surface2); }
  .status-dot {
    width: 8px; height: 8px;
    border-radius: 50%;
    flex-shrink: 0;
  }
  .status-dot.ok { background: var(--green); }
  .status-dot.err { background: var(--red); }
  .status-dot.warn { background: var(--yellow); }
  .status-dot.loading { background: var(--accent); animation: pulse 1s infinite; }
  @keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.3; } }
  .result-header .name { font-weight: 600; flex: 1; }
  .result-header .meta { color: var(--text2); font-size: 11px; }
  .result-body {
    display: none;
    padding: 12px 14px;
    background: var(--bg);
    font-size: 11px;
    line-height: 1.6;
  }
  .result-body.open { display: block; }
  .result-body pre {
    white-space: pre-wrap;
    word-break: break-all;
    color: var(--text2);
    max-height: 300px;
    overflow-y: auto;
  }
  .result-body .field { margin-bottom: 8px; }
  .result-body .field-label { color: var(--accent); font-weight: 600; }
  .burst-summary {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(60px, 1fr));
    gap: 4px;
    margin-top: 8px;
  }
  .burst-item {
    text-align: center;
    padding: 6px 4px;
    border-radius: 4px;
    font-size: 10px;
    font-weight: 600;
  }
  .burst-item.s2xx { background: #1a3a2a; color: var(--green); }
  .burst-item.s4xx { background: #3a2a1a; color: var(--yellow); }
  .burst-item.s5xx { background: #3a1a1a; color: var(--red); }
  .burst-item.s0xx { background: #2a2a2a; color: var(--text2); }
  .config-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 4px 12px;
    font-size: 11px;
  }
  .config-grid dt { color: var(--text2); }
  .config-grid dd { color: var(--text); font-weight: 600; text-align: right; }
  .burst-input {
    display: flex; gap: 6px; align-items: center; margin-bottom: 6px;
  }
  .burst-input input {
    width: 60px;
    padding: 6px 8px;
    background: var(--bg);
    border: 1px solid var(--border);
    border-radius: 4px;
    color: var(--text);
    font-family: inherit;
    font-size: 12px;
    text-align: center;
  }
  .burst-input label { font-size: 11px; color: var(--text2); }
  .empty-state {
    text-align: center;
    padding: 60px 20px;
    color: var(--text2);
  }
  .empty-state p { margin-top: 8px; font-size: 12px; }
</style>
</head>
<body>
<header>
  <h1><span>Bedrock Traffic Shaper</span> / Test Dashboard</h1>
  <div class="header-links">
    <a href="#" target="_blank" id="cwLink">CloudWatch Dashboard</a>
  </div>
</header>

<div class="layout">
  <div class="sidebar">
    <div class="status-cards">
      <div class="status-card">
        <div class="label">Queue Depth</div>
        <div class="value" id="queueDepthVal">--</div>
        <div class="sub" id="queueModel">loading...</div>
      </div>
      <div class="status-card">
        <div class="label">DLQ Messages</div>
        <div class="value" id="dlqVal">--</div>
        <div class="sub" id="dlqSub">loading...</div>
      </div>
    </div>

    <div class="test-section">
      <h3>CFF Validation</h3>
      <button class="test-btn" onclick="runTest('cff-no-header', 'POST')" id="btn-cff-no">
        Missing x-model-id <span class="badge badge-post">POST</span>
      </button>
      <button class="test-btn" onclick="runTest('cff-with-header', 'POST')" id="btn-cff-yes">
        With x-model-id <span class="badge badge-post">POST</span>
      </button>
    </div>

    <div class="test-section">
      <h3>Pipeline Tests</h3>
      <button class="test-btn" onclick="runTest('smoke-test', 'POST')" id="btn-smoke">
        Smoke Test (1 req) <span class="badge badge-post">POST</span>
      </button>
      <div class="burst-input">
        <label>Burst:</label>
        <input type="number" id="burstCount" value="5" min="1" max="50">
        <label>requests</label>
      </div>
      <button class="test-btn" onclick="runBurstTest()" id="btn-burst">
        Burst Test <span class="badge badge-post">POST</span>
      </button>
    </div>

    <div class="test-section">
      <h3>System Status</h3>
      <button class="test-btn" onclick="refreshStatus()" id="btn-refresh">
        Refresh Status <span class="badge badge-get">GET</span>
      </button>
    </div>

    <div class="test-section">
      <h3>Config</h3>
      <dl class="config-grid" id="configGrid">
        <dt>Model</dt><dd id="cfgModel">--</dd>
        <dt>Burst</dt><dd id="cfgBurst">--</dd>
        <dt>Queue</dt><dd id="cfgQueue">--</dd>
        <dt>RPM</dt><dd id="cfgRpm">--</dd>
        <dt>TPM Burst</dt><dd id="cfgTpm">--</dd>
      </dl>
    </div>
  </div>

  <div class="main">
    <div class="results-header">
      <h2>Test Results</h2>
      <button id="clearBtn" onclick="clearResults()">Clear All</button>
    </div>
    <div id="results">
      <div class="empty-state">
        <p>Run a test from the sidebar to see results here.</p>
      </div>
    </div>
  </div>
</div>

<script>
const BASE = '';
let resultCounter = 0;

// Init
document.addEventListener('DOMContentLoaded', () => {
  document.getElementById('cwLink').href = DASHBOARD_URL_PLACEHOLDER_JS;
  refreshStatus();
  loadConfig();
});

function refreshStatus() {
  fetch(BASE + '/api/queue-depth')
    .then(r => r.json())
    .then(d => {
      document.getElementById('queueDepthVal').textContent = d.queue_depth;
      document.getElementById('queueModel').textContent = d.model_id;
    })
    .catch(() => {
      document.getElementById('queueDepthVal').textContent = '?';
    });

  fetch(BASE + '/api/dlq-status')
    .then(r => r.json())
    .then(d => {
      if (d.error) {
        document.getElementById('dlqVal').textContent = '?';
        document.getElementById('dlqSub').textContent = d.error;
      } else {
        document.getElementById('dlqVal').textContent = d.visible;
        document.getElementById('dlqSub').textContent = d.in_flight + ' in-flight';
      }
    })
    .catch(() => {
      document.getElementById('dlqVal').textContent = '?';
    });
}

function loadConfig() {
  fetch(BASE + '/api/config')
    .then(r => r.json())
    .then(d => {
      document.getElementById('cfgModel').textContent = d.model_id || '--';
      document.getElementById('cfgBurst').textContent = d.burst_capacity || '--';
      document.getElementById('cfgQueue').textContent = d.queue_capacity || '--';
      document.getElementById('cfgRpm').textContent = d.rpm_limit || '--';
      document.getElementById('cfgTpm').textContent = d.tpm_burst_capacity || '0';
    });
}

function addResultEntry(name, statusClass) {
  const id = 'result-' + (++resultCounter);
  const container = document.getElementById('results');

  // Remove empty state
  const empty = container.querySelector('.empty-state');
  if (empty) empty.remove();

  const entry = document.createElement('div');
  entry.className = 'result-entry';
  entry.id = id;
  entry.innerHTML = `
    <div class="result-header" onclick="toggleBody('${id}')">
      <div class="status-dot ${statusClass}"></div>
      <span class="name">${name}</span>
      <span class="meta" id="${id}-meta">running...</span>
    </div>
    <div class="result-body" id="${id}-body"></div>
  `;
  container.prepend(entry);
  return id;
}

function updateResult(id, data, isBurst) {
  const dot = document.querySelector(`#${id} .status-dot`);
  const meta = document.getElementById(`${id}-meta`);
  const body = document.getElementById(`${id}-body`);

  if (isBurst) {
    const results = data;
    const successes = results.filter(r => r.status >= 200 && r.status < 300).length;
    const total = results.length;
    const avgMs = Math.round(results.reduce((s, r) => s + (r.elapsed_ms || 0), 0) / total);

    dot.className = 'status-dot ' + (successes === total ? 'ok' : successes > 0 ? 'warn' : 'err');
    meta.textContent = `${successes}/${total} passed | avg ${avgMs}ms`;

    let html = '<div class="burst-summary">';
    results.forEach(r => {
      const cls = r.status >= 200 && r.status < 300 ? 's2xx' :
                  r.status >= 400 && r.status < 500 ? 's4xx' :
                  r.status >= 500 ? 's5xx' : 's0xx';
      html += `<div class="burst-item ${cls}">${r.status}<br>${r.elapsed_ms}ms</div>`;
    });
    html += '</div>';
    body.innerHTML = html;
    body.classList.add('open');
  } else {
    const ok = data.status >= 200 && data.status < 300;
    const expected400 = data._expectedStatus === 400 && data.status === 400;
    const isPass = ok || expected400;

    dot.className = 'status-dot ' + (isPass ? 'ok' : data.status >= 400 ? (expected400 ? 'ok' : 'warn') : 'err');
    meta.textContent = `${data.status} | ${data.elapsed_ms}ms`;

    let bodyContent = '';
    if (data._expectedStatus) {
      const match = data.status === data._expectedStatus;
      bodyContent += `<div class="field"><span class="field-label">Expected:</span> ${data._expectedStatus} ${match ? '(MATCH)' : '(MISMATCH)'}</div>`;
    }
    bodyContent += `<div class="field"><span class="field-label">Status:</span> ${data.status}</div>`;
    bodyContent += `<div class="field"><span class="field-label">Time:</span> ${data.elapsed_ms}ms</div>`;
    if (data.headers && Object.keys(data.headers).length) {
      bodyContent += `<div class="field"><span class="field-label">Headers:</span><pre>${JSON.stringify(data.headers, null, 2)}</pre></div>`;
    }
    if (data.body) {
      let bodyDisplay = data.body;
      try { bodyDisplay = JSON.stringify(JSON.parse(data.body), null, 2); } catch(e) {}
      bodyContent += `<div class="field"><span class="field-label">Body:</span><pre>${escapeHtml(bodyDisplay)}</pre></div>`;
    }
    body.innerHTML = bodyContent;
    body.classList.add('open');
  }
}

function runTest(endpoint, method) {
  const names = {
    'smoke-test': 'Smoke Test',
    'cff-no-header': 'CFF: No x-model-id',
    'cff-with-header': 'CFF: With x-model-id'
  };
  const name = names[endpoint] || endpoint;
  const id = addResultEntry(name, 'loading');
  const btn = document.querySelector(`[onclick*="${endpoint}"]`);
  if (btn) btn.disabled = true;

  const expectedStatus = endpoint === 'cff-no-header' ? 400 : null;

  fetch(BASE + '/api/' + endpoint, {
    method: method || 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({})
  })
  .then(r => r.json())
  .then(data => {
    if (expectedStatus) data._expectedStatus = expectedStatus;
    updateResult(id, data);
    if (btn) btn.disabled = false;
    refreshStatus();
  })
  .catch(err => {
    updateResult(id, {status: 0, body: err.message, elapsed_ms: 0});
    if (btn) btn.disabled = false;
  });
}

function runBurstTest() {
  const count = parseInt(document.getElementById('burstCount').value) || 5;
  const id = addResultEntry(`Burst Test (${count} requests)`, 'loading');
  const btn = document.getElementById('btn-burst');
  btn.disabled = true;

  fetch(BASE + '/api/burst-test', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({count: count})
  })
  .then(r => r.json())
  .then(data => {
    updateResult(id, data, true);
    btn.disabled = false;
    refreshStatus();
  })
  .catch(err => {
    updateResult(id, [{status: 0, body: err.message, elapsed_ms: 0}], true);
    btn.disabled = false;
  });
}

function toggleBody(id) {
  const body = document.getElementById(id + '-body');
  body.classList.toggle('open');
}

function clearResults() {
  document.getElementById('results').innerHTML = '<div class="empty-state"><p>Run a test from the sidebar to see results here.</p></div>';
  resultCounter = 0;
}

function escapeHtml(text) {
  const el = document.createElement('div');
  el.textContent = text;
  return el.innerHTML;
}
</script>
</body>
</html>
""".replace('DASHBOARD_URL_PLACEHOLDER_JS', json.dumps(DASHBOARD_URL))

DASHBOARD_HTML_BYTES = DASHBOARD_HTML.encode()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description='Bedrock Traffic Shaper — Test Dashboard')
    parser.add_argument('--port', type=int, default=8080, help='Port to listen on (default: 8080)')
    parser.add_argument(
        '--host',
        default=os.environ.get('DASHBOARD_HOST', '127.0.0.1'),
        help='Interface to bind (default: 127.0.0.1, localhost-only). '
             'Set to 0.0.0.0 or override via DASHBOARD_HOST to expose externally.',
    )
    args = parser.parse_args()

    print(f"Bedrock Traffic Shaper — Test Dashboard")
    print(f"  CloudFront:  {CLOUDFRONT_URL}")
    print(f"  DLQ:         {DLQ_URL}")
    print(f"  Model:       {DEFAULT_MODEL}")
    print(f"  Dashboard:   {DASHBOARD_URL}")
    print(f"")
    print(f"  Open: http://localhost:{args.port}")
    print()

    server = http.server.HTTPServer((args.host, args.port), DashboardHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
        server.shutdown()


if __name__ == '__main__':
    main()
