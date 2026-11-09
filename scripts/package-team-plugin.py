#!/usr/bin/env python3
"""Build a deterministic, secret-free public package using only the stdlib."""
import hashlib
import json
from pathlib import Path
import re
import zipfile

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / 'plugins/index12-team'
FILES = ('plugin.json', 'mcp.json', 'assets/icon.svg')

manifest = json.loads((SOURCE / 'plugin.json').read_text())
assert manifest['name'] == 'index12-team'
assert re.fullmatch(r'\d+\.\d+\.\d+', manifest['version'])
extension = manifest['extensions']['com.openai']
listing = extension['interface']
for key, limit in [('displayName', 30), ('shortDescription', 30), ('longDescription', 4000), ('developerName', 80)]:
    assert 0 < len(listing[key]) <= limit, key
for key in ('websiteURL', 'supportURL', 'privacyPolicyURL', 'termsOfServiceURL'):
    assert listing[key].startswith('https://api.index12.com/plugins/index12-team'), key
assert len(extension['review']['test_cases']['positive']) == 5
assert len(extension['review']['test_cases']['negative']) == 3
for case in extension['review']['test_cases']['positive']:
    assert all(case.get(key) for key in ('description', 'prompt', 'tools_triggered', 'expected_behavior'))
for key in ('logo', 'composerIcon'):
    assert listing[key] == './assets/icon.svg'
mcp = json.loads((SOURCE / 'mcp.json').read_text())
assert len(mcp['mcpServers']) == 1
server = mcp['mcpServers']['index12-team']
assert server['type'] == 'streamable-http' and server['url'] == 'https://api.index12.com/team-mcp'
assert server['extensions']['com.openai']['auth']['type'] == 'oauth'
assert 'headers' not in server and 'command' not in server
assert 'width="64" height="64"' in (SOURCE / 'assets/icon.svg').read_text()

target = ROOT / 'output/plugins' / ('index12-team-' + manifest['version'] + '.zip')
target.parent.mkdir(parents=True, exist_ok=True)
with zipfile.ZipFile(target, 'w', zipfile.ZIP_DEFLATED) as archive:
    for name in FILES:
        file = SOURCE / name
        assert file.is_file() and not file.is_symlink()
        data = file.read_bytes()
        assert not re.search(rb'idxchat_[A-Za-z0-9_-]{43}|idx12_[A-Za-z0-9_-]{43}|clientSecret|test_credentials|reviewer_instructions', data), name
        info = zipfile.ZipInfo(name, (2026, 11, 9, 0, 0, 0))
        info.compress_type = zipfile.ZIP_DEFLATED
        info.external_attr = 0o644 << 16
        archive.writestr(info, data)
print(target)
print('SHA256 ' + hashlib.sha256(target.read_bytes()).hexdigest())
print('Files: ' + ', '.join(FILES))
