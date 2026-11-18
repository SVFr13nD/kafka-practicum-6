"""Capture the real HDFS UI and a browser report of unmodified execution logs."""
import html
import re
import sys
from pathlib import Path
from playwright.sync_api import expect, sync_playwright
out = Path(sys.argv[1]).resolve()
sections = []
for name in ['subjects.json', 'versions.json', 'topic-describe.txt', 'producer.txt', 'consumer.txt', 'console-consumer.txt', 'services.txt', 'hdfs-sink.txt', 'hdfs-files.txt', 'hdfs-fsck.txt']:
    sections.append(f'<h2>{html.escape(name)}</h2><pre>{html.escape((out/name).read_text())}</pre>')
report = out / 'report.html'
report.write_text('<!doctype html><meta charset="utf-8"><style>body{background:#101827;color:#e5edf5;font:16px monospace;padding:24px}pre{white-space:pre-wrap;background:#1b293c;padding:16px}h2{color:#70dbce}</style>' + f'<h1>Execution evidence: {out.name}</h1><p>Captured command output. Environment: {out.name}.</p>' + ''.join(sections))
with sync_playwright() as p:
    browser = p.chromium.launch()
    page = browser.new_page(viewport={'width':1400,'height':1000})
    page.goto(report.as_uri())
    page.screenshot(path=str(out/'execution-report.png'), full_page=True)
    page.goto('http://localhost:9870/dfshealth.html#tab-overview')
    live_nodes = page.locator('#tab-overview tr').filter(has=page.get_by_role('link', name='Live Nodes', exact=True))
    expect(live_nodes.locator('td')).to_have_text(re.compile(r'^\s*1\s*\('), timeout=30000)
    page.screenshot(path=str(out/'hdfs-ui.png'), full_page=True)
    browser.close()
