#!/usr/bin/env python3
"""Optional local OpenCode/Ollama authoring evaluation on synthetic PDFs only."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request


def local_url(value):
    parsed = urllib.parse.urlparse(value)
    if parsed.scheme != 'http' or parsed.hostname not in ('127.0.0.1', 'localhost', '::1'):
        raise argparse.ArgumentTypeError('Use a loopback HTTP server')
    return value.rstrip('/')


def request(base, path, value=None, raw=None, mime='application/json'):
    data = raw if raw is not None else json.dumps(value).encode() if value is not None else None
    req = urllib.request.Request(base + path, data=data, headers={'Content-Type': mime})
    try:
        with urllib.request.urlopen(req, timeout=180) as response:
            return response.status, response.read(16 * 1024 * 1024)
    except urllib.error.HTTPError as error:
        return error.code, error.read(1024 * 1024)


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def verify_pdf(path, expected, absent=()):
    subprocess.run(['qpdf', '--check', str(path)], check=True, capture_output=True, timeout=30)
    text = subprocess.check_output(['pdftotext', str(path), '-'], text=True, timeout=30)
    check(all(value in text for value in expected), 'Expected searchable text is missing')
    check(all(value not in text for value in absent), 'Removed text is still searchable')
    return text


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('studio', type=local_url)
    parser.add_argument('--ollama', type=local_url, default='http://127.0.0.1:11434')
    parser.add_argument('--model', default='gemma4:26b')
    parser.add_argument('--opencode', default=shutil.which('opencode') or str(Path.home() / '.opencode/bin/opencode'))
    parser.add_argument('--context-formats', nargs='+', choices=['json', 'toon'], default=['json', 'toon'],
                        help='Input context encodings to compare; model responses are always JSON')
    parser.add_argument('--case', action='append', dest='cases')
    parser.add_argument('--repairs', type=int, choices=range(3), default=2)
    parser.add_argument('--thinking', choices=['none', 'default'], default='none')
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    check(args.model and not args.model.endswith('-cloud'), 'Choose an installed local model')
    root = args.output or Path(tempfile.mkdtemp(prefix='kyo-pdf-model-eval-'))
    root.mkdir(parents=True, exist_ok=True)
    check(not any(root.iterdir()), 'Evidence directory must be empty')
    repo = Path(__file__).resolve().parents[1]
    fixtures = repo / 'target/semantic-fixtures'
    check((fixtures / 'source.pdf').is_file(), 'Run scripts/test-semantic-worker.py first')
    import pymupdf

    def upload(data):
        status, raw = request(args.studio, '/api/documents', raw=data)
        check(status == 200, raw.decode()[:300])
        return json.loads(raw)

    def page(label):
        with pymupdf.open() as doc:
            doc.new_page(width=300, height=200).insert_text((20, 110), label, fontsize=18)
            return doc.tobytes()

    inputs = {
        'source': (fixtures / 'source.pdf').read_bytes(),
        'replace': (fixtures / 'replace-source.pdf').read_bytes(),
        'cover': page('FRONT'),
        'tail': page('TAIL'),
    }
    status, sample = request(args.studio, '/api/sample', raw=b'')
    check(status == 200, 'Sample generation failed')
    sample_id = json.loads(sample)['id']
    status, inputs['long'] = request(args.studio, '/api/documents/' + sample_id + '/file')
    check(status == 200, 'Sample download failed')
    config = {
        'model': 'ollama/' + args.model, 'small_model': 'ollama/' + args.model,
        'enabled_providers': ['ollama'], 'autoupdate': False, 'share': 'disabled', 'snapshot': False,
        'permission': {'*': 'deny'},
        'provider': {'ollama': {'npm': '@ai-sdk/openai-compatible', 'name': 'Local Ollama',
            'options': {'baseURL': args.ollama + '/v1'}, 'models': {args.model: {
                'name': args.model, 'limit': {'context': 16384, 'output': 8192},
                'options': {'reasoningEffort': 'none'} if args.thinking == 'none' else {}}}}},
        'agent': {'recipe': {'mode': 'primary', 'description': 'Construct one recipe from its schema',
            'model': 'ollama/' + args.model, 'permission': {'*': 'deny'},
            'prompt': 'Construct the requested PDF recipe from the supplied schema and instructions. Always return exactly one JSON object, even when context is TOON. Do not call tools or output explanations.'}},
    }
    workspace = Path(tempfile.mkdtemp(prefix='kyo-pdf-opencode-'))
    for name in ['work', 'config', 'cache', 'data', 'state']:
        (workspace / name).mkdir()
    (workspace / 'work/opencode.json').write_text(json.dumps(config, indent=2))
    env = os.environ.copy()
    for key, name in [('XDG_CONFIG_HOME', 'config'), ('XDG_DATA_HOME', 'data'), ('XDG_CACHE_HOME', 'cache'), ('XDG_STATE_HOME', 'state')]:
        env[key] = str(workspace / name)

    cases = [
        ('redact_ocr', ['input', 'write', 'pdf.select-content', 'pdf.plan-redaction', 'pdf.redact', 'pdf.ocr'],
         'Use body. Redact page 1 at x=18, y=18, width=220, height=47 in top-left PDF points. Then OCR the redacted file in English at 144 DPI. Return only the finished PDF.'),
        ('replace_ocr', ['input', 'write', 'pdf.replace-text', 'pdf.ocr'],
         'Use body. Replace the visible text SECRET42 with PUBLIC, then OCR the result in English at 144 DPI. Return only the finished PDF.'),
        ('shared_counts', ['input', 'write', 'bytes.count', 'bytes.high-count'],
         'Use body. Write it once. Return that PDF, its byte count, and its count of bytes above 127. Both counts must inspect the same written file.'),
        ('conditional_stamp', ['input', 'stamp', 'write', 'choose', 'stop'],
         'Use body. If it has 2 through 64 pages, stamp REVIEW at the top. Otherwise leave it unchanged. If evidence is unknown, stop with a review message. Return one PDF. Use one choose node.'),
        ('body_packet', ['input', 'number', 'assemble', 'write'],
         'Bind cover, body and tail. Number only the body, starting at 1, style number, prefix BODY-. Then concatenate cover, numbered body, tail in that order. Return one PDF.'),
        ('ocr_fail_closed', ['input', 'write', 'pdf.ocr'],
         'Use body. OCR in language zzz at 144 DPI and return a PDF only on success. Any operation failure must fail the run; do not recover the original file.'),
    ]
    if args.cases:
        check(set(args.cases).issubset({c[0] for c in cases}), 'Unknown case')
        cases = [case for case in cases if case[0] in args.cases]
    results = []

    def invoke(tool, arguments):
        status, raw = request(args.studio, '/mcp', value={'jsonrpc': '2.0', 'id': 1, 'method': 'tools/call', 'params': {'name': tool, 'arguments': arguments}})
        check(status == 200, 'MCP transport failed')
        return json.loads(raw)['result']

    def run_and_verify(recipe, case_id, folder):
        ops = {node['id']: node['op'] for node in recipe['nodes']}
        if case_id == 'redact_ocr':
            selects = [n for n in recipe['nodes'] if n['op'] == 'pdf.select-content']
            check(len(selects) == 1 and selects[0].get('settings', {}).get('regions') == [
                {'page': 1, 'x': 18, 'y': 18, 'width': 220, 'height': 47}], 'The requested rectangle changed')
        if case_id == 'conditional_stamp':
            choices = [n for n in recipe['nodes'] if n['op'] == 'choose']
            check(len(choices) == 1 and choices[0]['test'] == {'kind': 'page-count', 'min': 2, 'max': 64}, 'The requested condition changed')
            check(ops.get(choices[0]['unknown']) == 'stop', 'Unknown evidence must stop')
        variants = ['short', 'long'] if case_id == 'conditional_stamp' else ['only']
        for variant in variants:
            selected = inputs['long'] if case_id == 'body_packet' or variant == 'long' else inputs['replace'] if case_id == 'replace_ocr' else inputs['source']
            files = {'body': upload(selected)['id']}
            if case_id == 'body_packet':
                files.update(cover=upload(inputs['cover'])['id'], tail=upload(inputs['tail'])['id'])
            arguments = {'recipe': recipe, 'files': files}
            result = invoke('run_pdf_pipeline', arguments)
            (folder / (variant + '-tool-call.json')).write_text(json.dumps(arguments, indent=2))
            (folder / (variant + '-tool-result.json')).write_text(json.dumps(result, indent=2))
            payload = result['structuredContent']
            if case_id == 'ocr_fail_closed':
                check(result['isError'] and payload.get('outputs') == [], 'Failed OCR exposed an output')
                check('OCR' in payload.get('error', '') or 'language' in payload.get('error', '').lower(), 'Unexpected failure reason')
                continue
            check(not result['isError'], payload.get('error', 'Tool failed'))
            check(payload['flags'] == [], 'Unexpected recovery flag')
            files_out = [o for o in payload['outputs'] if o['kind'] == 'file']
            check(len(files_out) == 1, 'Expected exactly one final PDF')
            output = files_out[0]['document']
            status, pdf = request(args.studio, '/api/documents/' + output['id'] + '/file')
            check(status == 200 and hashlib.sha256(pdf).hexdigest() == output['sha256'], 'Output hash mismatch')
            path = folder / (variant + '-output.pdf')
            path.write_bytes(pdf)
            text = verify_pdf(path, ['PUBLIC', 'KEEP VISIBLE'] if case_id == 'replace_ocr' else ['KEEP VISIBLE'] if case_id == 'redact_ocr' else [],
                ['SECRET42'] if case_id in ('redact_ocr', 'replace_ocr') else [])
            if case_id == 'redact_ocr':
                subprocess.run(['pdftoppm', '-r', '72', '-singlefile', str(path), str(folder / variant)], check=True, capture_output=True)
                header, pixels = (folder / (variant + '.ppm')).read_bytes().split(b'\n255\n', 1)
                check(header.split() == [b'P6', b'300', b'200'], 'Unexpected page dimensions')
                check(all(pixels[(y * 300 + 18) * 3:(y * 300 + 238) * 3] == b'\xff' * 660 for y in range(18, 65)), 'Selected pixels are not blank')
            elif case_id == 'shared_counts':
                values = {ops[o['node']]: o['value']['value'] for o in payload['outputs'] if o['kind'] == 'data'}
                check(values == {'bytes.count': len(pdf), 'bytes.high-count': sum(b > 127 for b in pdf)}, 'Count evidence differs from the returned bytes')
                check(payload['sourcePasses'] == 1, 'Compatible probes were not fused')
            elif case_id == 'conditional_stamp':
                decision = payload['decisions'][0]['outcome'].lower()
                check(decision == ('yes' if variant == 'long' else 'no'), 'Wrong branch decision')
                check(('REVIEW' in text) == (variant == 'long'), 'Stamp did not match the selected branch')
            elif case_id == 'body_packet':
                with pymupdf.open(path) as doc:
                    check(len(doc) == 6, 'Packet page count differs')
                    texts = [p.get_text() for p in doc]
                check('FRONT' in texts[0] and 'TAIL' in texts[-1], 'Bookend order differs')
                check(all('BODY-' not in texts[i] for i in (0, 5)), 'Bookends received numbering')
                check(all('BODY-' + str(i) in texts[i] for i in range(1, 5)), 'Body numbering differs')

    for context_format in args.context_formats:
        for case_id, operations, task in cases:
            folder = root / (context_format + '-context-' + case_id)
            folder.mkdir()
            query = urllib.parse.urlencode({'operations': ','.join(operations), 'format': context_format})
            status, definition = request(args.studio, '/api/pipelines/authoring?' + query)
            check(status == 200, definition.decode()[:300])
            (folder / ('definition.' + context_format)).write_bytes(definition)
            status, guide = request(args.studio, '/api/pipelines/authoring?' + urllib.parse.urlencode({'operations': ','.join(operations), 'format': 'json'}))
            check(status == 200, 'Authoring instructions unavailable')
            guide = json.loads(guide)
            check(guide['responseFormat'] == 'json', 'The model response contract must be JSON')
            instructions = guide['instructions']
            prompt = 'Return exactly one JSON recipe object, without markdown or explanations. The definition below is context only.\nDefinition:\n' + definition.decode() + '\nInstructions:\n' + instructions + '\nTask:\n' + task
            (folder / 'prompt.txt').write_text(prompt)
            feedback = prompt
            session = None
            attempts = []
            case_started = time.monotonic()
            for attempt in range(args.repairs + 1):
                command = [args.opencode, '--pure', 'run', '--dir', str(workspace / 'work'), '--model', 'ollama/' + args.model,
                    '--agent', 'recipe', '--format', 'json']
                if session:
                    command += ['--session', session]
                else:
                    command += ['--title', 'Synthetic pipeline ' + case_id]
                command.append(feedback)
                started = time.monotonic()
                with (folder / f'attempt-{attempt}.jsonl').open('w') as stdout, (folder / f'attempt-{attempt}.stderr').open('w') as stderr:
                    process = subprocess.Popen(command, cwd=workspace / 'work', env=env, stdout=stdout, stderr=stderr, start_new_session=True)
                    try:
                        code = process.wait(timeout=360)
                    except subprocess.TimeoutExpired:
                        import signal
                        os.killpg(process.pid, signal.SIGTERM)
                        code = process.wait(timeout=10)
                events = [json.loads(line) for line in (folder / f'attempt-{attempt}.jsonl').read_text().splitlines() if line.startswith('{')]
                if not session:
                    session = next((e.get('sessionID') for e in events if e.get('sessionID')), None)
                content = ''.join(e['part']['text'] for e in events if e.get('type') == 'text')
                (folder / f'candidate-{attempt}.json').write_text(content)
                status, checked = request(args.studio, '/api/pipelines/authoring/check?' + urllib.parse.urlencode({'operations': ','.join(operations), 'format': 'json'}), raw=content.encode())
                record = {'attempt': attempt, 'exitCode': code, 'seconds': round(time.monotonic() - started, 3), 'checked': status == 200}
                try:
                    checked = json.loads(checked)
                    if status != 200:
                        record['issues'] = checked
                        raise AssertionError('Recipe rejected')
                    recipe = checked['recipe']
                    (folder / 'checked-recipe.json').write_text(json.dumps(recipe, indent=2))
                    run_and_verify(recipe, case_id, folder)
                    record['passed'] = True
                except Exception as error:
                    record['passed'] = False
                    record['error'] = str(error)[:500]
                attempts.append(record)
                print(json.dumps({'case': case_id, 'contextFormat': context_format, **record}), flush=True)
                if record['passed']:
                    break
                feedback = 'Repair the complete JSON recipe. Return exactly one JSON object. Original task: ' + task + '\nChecker/verifier feedback:\n' + json.dumps(record.get('issues', record.get('error')))
            result = {'case': case_id, 'contextFormat': context_format, 'passed': attempts[-1]['passed'], 'attempts': attempts,
                'seconds': round(time.monotonic() - case_started, 3)}
            results.append(result)
            (root / 'results.json').write_text(json.dumps({'model': args.model, 'thinking': args.thinking, 'responseFormat': 'json', 'results': results}, indent=2))
    print(json.dumps({'passed': sum(r['passed'] for r in results), 'total': len(results), 'evidence': str(root)}))
    return 0 if all(r['passed'] for r in results) else 1


if __name__ == '__main__':
    raise SystemExit(main())
