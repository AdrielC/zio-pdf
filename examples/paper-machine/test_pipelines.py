"""Synthetic integration checks for data-defined recipes against the local studio."""
import copy
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
import urllib.error
import urllib.request
from pathlib import Path

BASE = sys.argv.pop(1).rstrip('/') if len(sys.argv) > 1 else 'http://127.0.0.1:8087'


def request(path, value=None, raw=None):
    data = raw if raw is not None else json.dumps(value).encode() if value is not None else None
    req = urllib.request.Request(BASE + path, data=data, headers={'Content-Type': 'application/json', 'Accept': 'application/json, text/event-stream'})
    try:
        with urllib.request.urlopen(req, timeout=180) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as error:
        return error.code, error.read()


class PipelineTest(unittest.TestCase):
    def setUp(self):
        status, data = request('/api/sample', raw=b'')
        self.assertEqual(status, 200, data)
        self.doc = json.loads(data)
        self.recipe = {'version': 1, 'name': 'Synthetic recipe', 'nodes': [
            {'id': 'body', 'op': 'input', 'slot': 'body', 'inputs': []},
            {'id': 'cut', 'op': 'select', 'pages': [4, 1, 3], 'inputs': ['body']},
            {'id': 'pdf', 'op': 'write', 'inputs': ['cut']}], 'outputs': ['pdf']}

    def run_recipe(self, recipe=None, bindings=None):
        return request('/api/pipelines/run', {'recipe': recipe or self.recipe, 'bindings': bindings or {'body': self.doc['id']}})

    def example(self, kind):
        status, data = request('/api/pipelines/example?' + kind)
        self.assertEqual(status, 200, data)
        return json.loads(data)

    def output_text(self, document):
        status, pdf = request('/api/documents/' + document['id'] + '/file')
        self.assertEqual(status, 200)
        self.assertEqual(hashlib.sha256(pdf).hexdigest(), document['sha256'])
        if not shutil.which('pdftotext'):
            return None
        with tempfile.TemporaryDirectory(prefix='conditional-pdf-') as folder:
            path = Path(folder) / 'output.pdf'
            path.write_bytes(pdf)
            if shutil.which('qpdf'):
                result = subprocess.run(['qpdf', '--check', str(path)], capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            return subprocess.check_output(['pdftotext', str(path), '-'], text=True)

    def rpc(self, method, params=None, request_id=1):
        call = {'jsonrpc': '2.0', 'method': method, 'params': params or {}}
        if request_id is not None:
            call['id'] = request_id
        status, data = request('/mcp', call)
        if request_id is None:
            self.assertEqual((status, data), (202, b''))
            return None
        self.assertEqual(status, 200, data)
        response = json.loads(data)
        self.assertEqual(response['id'], request_id)
        self.assertIn('result', response, response)
        return response['result']

    def test_mcp_tool_returns_pdf_and_named_branch_decision(self):
        initialized = self.rpc('initialize', {'protocolVersion': '2025-06-18', 'capabilities': {},
                                         'clientInfo': {'name': 'pipeline-test', 'version': '1'}})
        self.assertEqual(initialized['protocolVersion'], '2025-06-18')
        self.rpc('notifications/initialized', request_id=None)
        tools = {tool['name']: tool for tool in self.rpc('tools/list', request_id=2)['tools']}
        self.assertEqual(set(tools), {'run_pdf_pipeline', 'check_pdf_pipeline', 'explain_pdf_pipeline'})
        tool = tools['run_pdf_pipeline']
        variants = tool['inputSchema']['properties']['recipe']['properties']['nodes']['items']['oneOf']
        branches = next(v['required'] for v in variants if v['properties']['op']['const'] == 'choose')
        self.assertTrue({'test', 'source', 'yes', 'no', 'unknown'}.issubset(branches))
        self.assertNotIn('inputs', branches)

        status, data = request('/api/pipelines/model-example?pages')
        self.assertEqual(status, 200, data)
        recipe = json.loads(data)
        choose = next(node for node in recipe['nodes'] if node['op'] == 'choose')
        checked = self.rpc('tools/call', {'name': 'check_pdf_pipeline', 'arguments': {'recipe': recipe}})
        self.assertFalse(checked['isError'], checked)
        self.assertEqual(checked['structuredContent']['recipe'], recipe)
        explain = self.rpc('tools/call', {'name': 'explain_pdf_pipeline',
                                        'arguments': {'recipe': recipe, 'files': {'body': self.doc['id']}}})
        self.assertFalse(explain['isError'], explain)
        self.assertEqual(explain['structuredContent']['decisions'][0]['selected'], 'long')
        self.assertEqual(explain['structuredContent']['deferred'], [])
        status, data = request('/api/pipelines/model-example?validation')
        self.assertEqual(status, 200, data)
        deferred = self.rpc('tools/call', {'name': 'explain_pdf_pipeline',
                                         'arguments': {'recipe': json.loads(data), 'files': {'body': self.doc['id']}}})
        self.assertFalse(deferred['isError'], deferred)
        self.assertEqual(deferred['structuredContent']['decisions'], [])
        self.assertTrue(deferred['structuredContent']['deferred'])
        params = {'name': tool['name'], 'arguments': {'recipe': recipe, 'files': {'body': self.doc['id']}}}
        result = self.rpc('tools/call', params, request_id=3)
        self.assertFalse(result['isError'], result)
        payload = result['structuredContent']
        self.assertEqual(payload['recipeVersion'], recipe['version'])
        self.assertEqual(payload['inputs'][0]['sha256'], self.doc['sha256'])
        self.assertEqual(payload['decisions'][0]['outcome'], 'no')
        self.assertEqual(payload['decisions'][0]['selected'], 'long')
        self.assertIn('short', payload['skipped'])
        self.assertIn('unknown', payload['skipped'])
        output = payload['outputs'][0]['document']
        link, = [item for item in result['content'] if item['type'] == 'resource_link']
        self.assertEqual(link['mimeType'], 'application/pdf')
        self.assertEqual(link['uri'], BASE + '/api/documents/' + output['id'] + '/file')
        with urllib.request.urlopen(link['uri'], timeout=10) as response:
            self.assertEqual(response.headers.get_content_type(), 'application/pdf')
            pdf = response.read()
        self.assertTrue(pdf.startswith(b'%PDF-'))
        self.assertEqual(hashlib.sha256(pdf).hexdigest(), output['sha256'])
        text = self.output_text(output)
        if text is not None:
            self.assertIn('LONG PACKET', text)
            self.assertNotIn('SHORT PACKET', text)

        choose['yes'] = 'missing-node'
        rejected = self.rpc('tools/call', params, request_id=4)
        self.assertTrue(rejected['isError'])
        self.assertEqual(rejected['structuredContent']['issues'][0]['path'],
                         f"nodes[{recipe['nodes'].index(choose)}].yes")
        self.assertFalse(any(item['type'] == 'resource_link' for item in rejected['content']))

    def test_packet_routes_numbered_unnumbered_and_ambiguous_bodies(self):
        def invoke(name, recipe, files):
            return self.rpc('tools/call', {'name': name, 'arguments': {'recipe': recipe, 'files': files}})
        def one_page(page):
            status, data = request('/api/documents/' + self.doc['id'] + '/remix', raw=str(page).encode())
            self.assertEqual(status, 200, data)
            return json.loads(data)['id']

        status, data = request('/api/pipelines/model-example?packet')
        self.assertEqual(status, 200, data)
        recipe = json.loads(data)
        files = {'cover': one_page(1), 'body': self.doc['id'], 'tail': one_page(4)}
        for slot, doc_id in files.items():
            status, data = request('/api/pipeline-inputs/packet-' + self.doc['id'][:12] + '-' + slot + '?document=' + doc_id, raw=b'')
            self.assertEqual(status, 200, data)
        explained = invoke('explain_pdf_pipeline', recipe, files)
        self.assertFalse(explained['isError'], explained)
        self.assertEqual(explained['structuredContent']['decisions'][0]['outcome'], 'no')

        result = invoke('run_pdf_pipeline', recipe, files)
        self.assertFalse(result['isError'], result)
        payload = result['structuredContent']
        self.assertEqual(payload['decisions'][0]['selected'], 'number-body')
        output, receipt = payload['outputs']
        self.assertEqual(receipt['report']['sha256'], output['document']['sha256'])
        self.assertEqual(payload['validationEvidence'][0]['sha256'], output['document']['sha256'])
        text = self.output_text(output['document'])
        if text is not None:
            self.assertIn('Page 1 of 4', text)
            self.assertIn('Page 4 of 4', text)
            self.assertNotIn('Page 5 of', text)

        number = {'version': 1, 'name': 'Number body', 'nodes': [
            {'id': 'body', 'op': 'input', 'slot': 'body', 'inputs': []},
            {'id': 'numbered', 'op': 'number', 'inputs': ['body']},
            {'id': 'pdf', 'op': 'write', 'inputs': ['numbered']}], 'outputs': ['pdf']}
        numbered = invoke('run_pdf_pipeline', number, {'body': self.doc['id']})
        self.assertFalse(numbered['isError'], numbered)
        files['body'] = numbered['structuredContent']['outputs'][0]['document']['id']
        result = invoke('run_pdf_pipeline', recipe, files)
        self.assertFalse(result['isError'], result)
        self.assertEqual(result['structuredContent']['decisions'][0]['outcome'], 'yes')
        self.assertIn('number-body', result['structuredContent']['skipped'])

        raster = b'q 100 0 0 100 50 50 cm BI /W 1 /H 1 /BPC 8 /CS /G ID \x80 EI Q\n'
        objects = [
            b'<< /Type /Catalog /Pages 2 0 R >>',
            b'<< /Type /Pages /Count 1 /Kids [3 0 R] >>',
            b'<< /Type /Page /Parent 2 0 R /MediaBox [0 0 200 200] /Contents 4 0 R >>',
            f'<< /Length {len(raster)} >>\nstream\n'.encode() + raster + b'endstream'
        ]
        pdf = bytearray(b'%PDF-1.7\n')
        offsets = []
        for index, obj in enumerate(objects, 1):
            offsets.append(len(pdf))
            pdf.extend(f'{index} 0 obj\n'.encode() + obj + b'\nendobj\n')
        xref = len(pdf)
        pdf.extend(b'xref\n0 5\n0000000000 65535 f \n')
        for offset in offsets:
            pdf.extend(f'{offset:010d} 00000 n \n'.encode())
        pdf.extend(f'trailer\n<< /Root 1 0 R /Size 5 >>\nstartxref\n{xref}\n%%EOF\n'.encode())
        status, data = request('/api/pipeline-inputs/body', raw=bytes(pdf))
        self.assertEqual(status, 200, data)
        files['body'] = json.loads(data)['id']
        result = invoke('run_pdf_pipeline', recipe, files)
        self.assertTrue(result['isError'])
        self.assertEqual(result['structuredContent']['decisions'][0]['outcome'], 'unknown')
        self.assertEqual(result['structuredContent']['decisions'][0]['selected'], 'review-numbering')
        self.assertEqual(result['structuredContent']['outputs'], [])
        self.assertEqual(result['structuredContent']['validationEvidence'], [])

    @unittest.skipUnless(os.environ.get('KYO_PDF_TEST_GOTENBERG') == '1', 'requires local Gotenberg')
    def test_gotenberg_archive_packet_passes_3b_through_tool(self):
        files = {'body': self.doc['id']}
        for slot, page in [('cover', 1), ('tail', 4)]:
            status, data = request('/api/documents/' + self.doc['id'] + '/remix', raw=str(page).encode())
            self.assertEqual(status, 200, data)
            files[slot] = json.loads(data)['id']

        status, data = request('/api/pipelines/model-example?archive-packet')
        self.assertEqual(status, 200, data)
        recipe = json.loads(data)
        result = self.rpc('tools/call', {'name': 'run_pdf_pipeline',
                                        'arguments': {'files': files, 'recipe': recipe}})
        self.assertFalse(result['isError'], result.get('structuredContent'))
        payload = result['structuredContent']
        decisions = {decision['node']: decision for decision in payload['decisions']}
        self.assertEqual(decisions['body-numbering']['selected'], 'number-body')
        self.assertEqual(decisions['archive-check']['outcome'], 'yes')
        self.assertEqual(payload['executed'].count('convert-3b'), 1)
        self.assertEqual(payload['validatorPasses'], 1)
        file, receipt = payload['outputs']
        self.assertEqual(len(file['document']['pages']), 6)
        self.assertEqual(receipt['report']['sha256'], file['document']['sha256'])
        self.assertEqual(payload['validationEvidence'][0]['sha256'], file['document']['sha256'])
        self.assertEqual([(row['profile'], row['status']) for row in receipt['report']['validation']['results']],
                         [('3b', 'passed')])
        link, = [item for item in result['content'] if item['type'] == 'resource_link']
        with urllib.request.urlopen(link['uri'], timeout=20) as response:
            self.assertEqual(response.headers.get_content_type(), 'application/pdf')
            self.assertEqual(hashlib.sha256(response.read()).hexdigest(), file['document']['sha256'])
        text = self.output_text(file['document'])
        if text is not None:
            pages = text.split('\f')[:-1]
            self.assertEqual(len(pages), 6)
            self.assertNotIn('Page 1 of 4', pages[0])
            for index, page in enumerate(pages[1:5], 1):
                self.assertIn(f'Page {index} of 4', page)
            self.assertNotIn('Page 4 of 4', pages[5])

    def test_court_preflight_reports_source_pages_and_exact_bytes(self):
        def stream(content):
            return f'<< /Length {len(content)} >>\nstream\n'.encode() + content + b'endstream'

        objects = [
            b'<< /Type /Catalog /Pages 2 0 R /Outlines 8 0 R /PageLabels << /Nums [0 << /S /D /St 4 >>] >> >>',
            b'<< /Type /Pages /Count 3 /Kids [3 0 R 4 0 R 5 0 R] /Resources << /Font << /F 10 0 R >> >> >>',
            b'<< /Type /Page /Parent 2 0 R /MediaBox [0 0 300 400] /Contents 6 0 R >>',
            b'<< /Type /Page /Parent 2 0 R /MediaBox [0 0 300 400] /Contents 7 0 R >>',
            b'<< /Type /Page /Parent 2 0 R /MediaBox [0 0 300 400] >>',
            stream(b'BT /F 12 Tf 30 350 Td (Petition for review) Tj ET\n'),
            stream(b'BT /F 12 Tf 30 350 Td (Exhibit B) Tj ET\n'),
            b'<< /Type /Outlines /First 9 0 R /Last 9 0 R /Count 1 >>',
            b'<< /Title (Exhibit B) /Parent 8 0 R /Dest [3 0 R /XYZ null null null] >>',
            b'<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>',
        ]
        pdf = bytearray(b'%PDF-1.7\n')
        offsets = []
        for index, obj in enumerate(objects, 1):
            offsets.append(len(pdf))
            pdf.extend(f'{index} 0 obj\n'.encode() + obj + b'\nendobj\n')
        xref = len(pdf)
        pdf.extend(f'xref\n0 {len(objects) + 1}\n0000000000 65535 f \n'.encode())
        for offset in offsets:
            pdf.extend(f'{offset:010d} 00000 n \n'.encode())
        pdf.extend(f'trailer\n<< /Root 1 0 R /Size {len(objects) + 1} >>\nstartxref\n{xref}\n%%EOF\n'.encode())

        status, data = request('/api/documents', raw=bytes(pdf))
        self.assertEqual(status, 200, data)
        source = json.loads(data)
        status, data = request('/api/documents/' + source['id'] + '/policy-preflight', raw=b'')
        self.assertEqual(status, 200, data)
        report = json.loads(data)
        self.assertEqual(report['sha256'], hashlib.sha256(pdf).hexdigest())
        self.assertEqual(report['profile']['effectiveDate'], '2026-07-01')
        self.assertEqual(report['status'], 'fix-required')
        findings = report['findings']
        self.assertTrue(any(f['check'] == 'file-size' and f['status'] == 'pass' for f in findings))
        self.assertTrue(any(f['check'] == 'pagination' and f['status'] == 'fix-required' and f['page'] == 2 for f in findings))
        self.assertTrue(any(f['check'] == 'searchability' and f['status'] == 'needs-review' and f['page'] == 3 for f in findings))
        self.assertTrue(any(f['check'] == 'exhibit-destinations' and f['status'] == 'needs-review'
                            and f['page'] == 1 and 'page 2' in f['message'] for f in findings), findings)
        self.assertTrue(all(f['source'].startswith('https://courts.ca.gov/') for f in findings))
        status, unchanged = request('/api/documents/' + source['id'] + '/file')
        self.assertEqual((status, unchanged), (200, bytes(pdf)))

        status, data = request('/api/pipelines/model-example?court-preflight')
        self.assertEqual(status, 200, data)
        recipe = json.loads(data)
        call = {'name': 'run_pdf_pipeline', 'arguments': {'recipe': recipe, 'files': {'body': source['id']}}}
        result = self.rpc('tools/call', call)
        self.assertFalse(result['isError'], result)
        file, receipt = result['structuredContent']['outputs']
        self.assertEqual(receipt['report']['sha256'], file['document']['sha256'])
        self.assertEqual(receipt['report']['profile'], report['profile'])

    def test_loaded_policy_compiles_and_runs_through_mcp(self):
        status, data = request('/api/policies/example')
        self.assertEqual(status, 200, data)
        profile = json.loads(data)
        status, schema = request('/api/policies/schema')
        self.assertEqual(status, 200, schema)
        self.assertIn('rules', json.loads(schema)['properties'])

        profile.update(id='synthetic-motion-v1', name='Synthetic motion profile',
                       authority='Synthetic standards body', documentType='Report', source='Synthetic test rules')
        rule = profile['rules'][0]
        profile['rules'] = [rule]
        rule.update(id='page-limit', citation='Synthetic rule 1', source='Synthetic test rules',
                    message='The document must have at most three pages.',
                    condition={'Compare': {'left': {'Fact': {'metric': 'PageCount'}},
                                           'comparison': 'LessThanOrEqual', 'right': {'Number': {'value': 3}}}})
        status, compiled = request('/api/policies/check', profile)
        self.assertEqual(status, 200, compiled)
        self.assertEqual(json.loads(compiled), profile)
        legacy = copy.deepcopy(profile)
        legacy['court'] = legacy.pop('authority')
        legacy['filingType'] = legacy.pop('documentType')
        status, migrated = request('/api/policies/check', legacy)
        self.assertEqual(status, 200, migrated)
        self.assertEqual(json.loads(migrated), profile)
        status, report = request('/api/documents/' + self.doc['id'] + '/policy-preflight', profile)
        self.assertEqual(status, 200, report)
        failed = json.loads(report)
        self.assertEqual(failed['sha256'], self.doc['sha256'])
        self.assertEqual(failed['status'], 'fix-required')
        self.assertEqual(failed['findings'][0]['evidence']['PageCount'], 4)

        rule['condition']['Compare']['right']['Number']['value'] = 4
        status, report = request('/api/documents/' + self.doc['id'] + '/policy-preflight', profile)
        self.assertEqual(status, 200, report)
        passed = json.loads(report)
        self.assertEqual(passed['status'], 'pass')
        self.assertNotEqual(passed['profileSha256'], failed['profileSha256'])
        self.assertEqual(passed['sha256'], failed['sha256'])

        status, data = request('/api/pipelines/model-example?court-preflight')
        self.assertEqual(status, 200, data)
        recipe = json.loads(data)
        operation = next(n for n in recipe['nodes'] if n['op'] == 'preflight-rules')
        operation['rules'] = profile
        checked = self.rpc('tools/call', {'name': 'check_pdf_pipeline', 'arguments': {'recipe': recipe}})
        self.assertFalse(checked['isError'], checked)
        self.assertEqual(checked['structuredContent']['recipe'], recipe)
        operation['rules'] = legacy
        migrated = self.rpc('tools/call', {'name': 'check_pdf_pipeline', 'arguments': {'recipe': recipe}})
        self.assertFalse(migrated['isError'], migrated)
        operation['rules'] = profile
        # Legacy was captured before the threshold changed from three to four.
        migrated_rules = next(n['rules'] for n in migrated['structuredContent']['recipe']['nodes']
                              if n['op'] == 'preflight-rules')
        self.assertEqual(migrated_rules['authority'], profile['authority'])
        result = self.rpc('tools/call', {'name': 'run_pdf_pipeline',
                                        'arguments': {'recipe': recipe, 'files': {'body': self.doc['id']}}})
        self.assertFalse(result['isError'], result)
        file, receipt = result['structuredContent']['outputs']
        self.assertEqual(receipt['report']['status'], 'pass')
        self.assertEqual(receipt['report']['sha256'], file['document']['sha256'])
        self.assertEqual(receipt['report']['profileSha256'], passed['profileSha256'])

        invalid = copy.deepcopy(profile)
        invalid['version'] = 99
        invalid['rules'][0]['scope'] = 'Page'
        status, errors = request('/api/policies/check', invalid)
        self.assertEqual(status, 400, errors)
        issues = json.loads(errors)['issues']
        self.assertTrue(any(i['path'] == 'profile.version' for i in issues))
        self.assertTrue(any(i['code'] == 'Type' for i in issues))
        operation['rules'] = invalid
        rejected = self.rpc('tools/call', {'name': 'check_pdf_pipeline', 'arguments': {'recipe': recipe}})
        self.assertTrue(rejected['isError'])
        self.assertTrue(any('.rules.version' in i['path'] for i in rejected['structuredContent']['issues']))

    def test_page_count_example_runs_only_the_selected_stamp(self):
        recipe = self.example('pages')
        self.assertEqual(recipe['version'], 3)
        status, checked = request('/api/pipelines/check', recipe)
        self.assertEqual(status, 200, checked)
        self.assertEqual(json.loads(checked)['recipe'], recipe)
        status, data = self.run_recipe(recipe)
        self.assertEqual(status, 200, data)
        result = json.loads(data)
        self.assertEqual(result['decisions'][0]['outcome'], 'no')
        self.assertEqual(result['decisions'][0]['selected'], 'long')
        self.assertIn('short', result['skipped'])
        self.assertIn('unknown', result['skipped'])
        text = self.output_text(result['outputs'][0]['document'])
        if text is not None:
            self.assertIn('LONG PACKET', text)
            self.assertNotIn('SHORT PACKET', text)
        status, data = request('/api/documents/' + self.doc['id'] + '/remix', raw=b'1,2')
        self.assertEqual(status, 200, data)
        small = json.loads(data)
        status, data = self.run_recipe(recipe, {'body': small['id']})
        self.assertEqual(status, 200, data)
        result = json.loads(data)
        self.assertEqual(result['decisions'][0]['selected'], 'short')
        self.assertIn('long', result['skipped'])

    def test_validation_choice_reuses_passed_bytes_and_marks_failed_documents(self):
        recipe = self.example('validation')
        status, valid = request('/api/validation-samples/valid')
        self.assertEqual(status, 200)
        status, data = request('/api/pipeline-inputs/conditional-archive', raw=valid)
        self.assertEqual(status, 200, data)
        valid_doc = json.loads(data)
        status, data = self.run_recipe(recipe, {'body': valid_doc['id']})
        self.assertEqual(status, 200, data)
        result = json.loads(data)
        self.assertEqual(result['decisions'][0]['outcome'], 'yes')
        self.assertEqual(result['validatorPasses'], 1)
        self.assertEqual(set(result['skipped']), {'review-source', 'review', 'review-pdf', 'unable'})
        output, before, after = result['outputs']
        self.assertEqual(output['document']['sha256'], before['report']['sha256'])
        self.assertEqual(before['report']['checkedAt'], after['report']['checkedAt'])
        self.assertEqual(after['report']['validation']['results'][0]['status'], 'passed')
        status, data = self.run_recipe(recipe)
        self.assertEqual(status, 200, data)
        result = json.loads(data)
        self.assertEqual(result['decisions'][0]['outcome'], 'no')
        self.assertEqual(result['decisions'][0]['selected'], 'review-pdf')
        self.assertEqual(result['validatorPasses'], 2)
        self.assertEqual(len(result['executed']), len(set(result['executed'])))
        output, before, after = result['outputs']
        self.assertNotEqual(before['report']['sha256'], after['report']['sha256'])
        self.assertEqual(after['report']['sha256'], output['document']['sha256'])
        text = self.output_text(output['document'])
        if text is not None:
            self.assertIn('REVIEW REQUIRED', text)

    def test_missing_validation_evidence_routes_to_unknown_stop(self):
        recipe = self.example('validation')
        route = next(n for n in recipe['nodes'] if n['id'] == 'route')
        route['test']['profile'] = '2b'
        next(n for n in recipe['nodes'] if n['id'] == 'unable')['message'] = 'No completed verdict for requested profile'
        status, data = self.run_recipe(recipe)
        self.assertEqual(status, 400, data)
        self.assertIn('No completed verdict', json.loads(data)['error'])
        self.assertEqual(request('/api/documents/' + self.doc['id'] + '/file')[0], 200)

    def test_conditional_validation_pass_budget_and_combined_profiles(self):
        recipe = {'version': 2, 'name': 'Bounded checks', 'nodes': [
            {'id': 'body', 'op': 'input', 'slot': 'body', 'inputs': []},
            {'id': 'route', 'op': 'choose', 'kind': 'document',
             'test': {'kind': 'page-count', 'min': 1, 'max': 64},
             'inputs': ['body', 'body', 'body', 'body']},
            {'id': 'pdf', 'op': 'write', 'inputs': ['route']},
            *[{'id': f'check{i}', 'op': 'validate', 'profiles': profiles, 'inputs': ['pdf']}
              for i, profiles in enumerate([['3b'], ['2b'], ['3u'], ['2b', '3u']])]],
            'outputs': ['check0', 'check1', 'check2', 'check3']}
        status, data = self.run_recipe(recipe)
        self.assertEqual(status, 400, data)
        self.assertIn('at most three validation passes', json.loads(data)['error'])
        recipe['nodes'][-4]['profiles'] = ['3b', '2b', '3u']
        status, data = self.run_recipe(recipe)
        self.assertEqual(status, 200, data)
        result = json.loads(data)
        self.assertEqual(result['validatorPasses'], 1)
        self.assertEqual(result['validationRuns'][0]['profiles'], ['2b', '3b', '3u'])

    def test_nested_routes_skip_expired_bindings_and_runtime_invalid_pages(self):
        recipe = {'version': 2, 'name': 'Nested lazy routes', 'nodes': [
            {'id': 'body', 'op': 'input', 'slot': 'body', 'inputs': []},
            {'id': 'optional', 'op': 'input', 'slot': 'optional', 'inputs': []},
            {'id': 'bad', 'op': 'select', 'pages': [65], 'inputs': ['body']},
            {'id': 'stop', 'op': 'stop', 'message': 'Unknown count', 'kind': 'document', 'inputs': []},
            {'id': 'inner', 'op': 'choose', 'test': {'kind': 'page-count', 'min': 1, 'max': 3}, 'kind': 'document', 'inputs': ['body', 'optional', 'body', 'stop']},
            {'id': 'outer', 'op': 'choose', 'test': {'kind': 'page-count', 'min': 1, 'max': 3}, 'kind': 'document', 'inputs': ['body', 'bad', 'inner', 'stop']},
            {'id': 'pdf', 'op': 'write', 'inputs': ['outer']}], 'outputs': ['pdf']}
        status, data = self.run_recipe(recipe, {'body': self.doc['id'], 'optional': 'expired'})
        self.assertEqual(status, 200, data)
        result = json.loads(data)
        self.assertEqual({d['node'] for d in result['decisions']}, {'outer', 'inner'})
        self.assertEqual(set(result['skipped']), {'optional', 'bad', 'stop'})
        self.assertEqual(result['validatorPasses'], 0)
        next(n for n in recipe['nodes'] if n['id'] == 'outer')['test']['max'] = 64
        self.assertEqual(self.run_recipe(recipe)[0], 400)

    def test_unselected_validation_profiles_do_not_run_speculatively(self):
        recipe = {'version': 2, 'name': 'Demanded checks only', 'nodes': [
            {'id': 'body', 'op': 'input', 'slot': 'body', 'inputs': []},
            {'id': 'pdf', 'op': 'write', 'inputs': ['body']},
            {'id': 'wanted', 'op': 'validate', 'profiles': ['3b'], 'inputs': ['pdf']},
            {'id': 'unused', 'op': 'validate', 'profiles': ['2b', '3u'], 'inputs': ['pdf']},
            {'id': 'route', 'op': 'choose', 'test': {'kind': 'page-count', 'min': 1, 'max': 64}, 'kind': 'receipt', 'inputs': ['body', 'wanted', 'wanted', 'unused']}], 'outputs': ['route']}
        status, data = self.run_recipe(recipe)
        self.assertEqual(status, 200, data)
        result = json.loads(data)
        self.assertEqual(result['validatorPasses'], 1)
        self.assertEqual(result['validationRuns'][0]['profiles'], ['3b'])
        self.assertEqual(result['skipped'], ['unused'])

    def test_conditional_schema_rejects_mixed_ports_bad_ranges_and_old_versions(self):
        recipe = self.example('pages')
        recipe['version'] = 1
        status, data = request('/api/pipelines/check', recipe)
        self.assertEqual(status, 400, data)
        self.assertIn('Version', [i['code'] for i in json.loads(data)['issues']])
        recipe['version'] = 3
        route = next(n for n in recipe['nodes'] if n['id'] == 'route')
        route['test']['min'] = 0
        route['test']['max'] = 99
        self.assertIn('Setting', [i['code'] for i in json.loads(request('/api/pipelines/check', recipe)[1])['issues']])
        route['test'] = {'kind': 'page-count', 'min': 1, 'max': 3}
        route['inputs'][1] = 'pdf'
        self.assertIn('Type', [i['code'] for i in json.loads(request('/api/pipelines/check', recipe)[1])['issues']])
        route['test']['arbitraryCode'] = 'rejected'
        self.assertIn('Structure', [i['code'] for i in json.loads(request('/api/pipelines/check', recipe)[1])['issues']])

    def test_volga_example_round_trips_to_portable_data(self):
        status, data = request('/api/pipelines/example')
        self.assertEqual(status, 200, data)
        recipe = json.loads(data)
        self.assertEqual([n['op'] for n in recipe['nodes']], ['input', 'select', 'stamp', 'number', 'write', 'validate'])
        status, checked = request('/api/pipelines/check', recipe)
        self.assertEqual(status, 200, checked)
        self.assertEqual(json.loads(checked)['recipe'], recipe)
        self.assertEqual(recipe['outputs'], ['pdf', 'checks'])

    def test_arrow_projection_and_sequential_stop_round_trip_with_typed_ports(self):
        recipe = {'version': 3, 'name': 'Arrow ports', 'nodes': [
            {'id': 'body', 'op': 'input', 'slot': 'body', 'inputs': []},
            {'id': 'pdf', 'op': 'write', 'inputs': ['body']},
            {'id': 'checks', 'op': 'validate', 'profiles': ['3b'], 'inputs': ['pdf']},
            {'id': 'keep', 'op': 'keep', 'index': 0, 'kinds': ['file', 'receipt'], 'inputs': ['pdf', 'checks']}],
            'outputs': ['keep']}
        status, data = request('/api/pipelines/check', recipe)
        self.assertEqual(status, 200, data)
        self.assertEqual(json.loads(data)['recipe'], recipe)
        status, data = self.run_recipe(recipe)
        self.assertEqual(status, 200, data)
        result = json.loads(data)
        self.assertEqual(result['executed'], ['body', 'pdf', 'checks', 'keep'])
        self.assertEqual(result['validatorPasses'], 1)
        self.assertEqual(result['outputs'][0]['document']['sha256'], result['validationRuns'][0]['sha256'])
        invalid = copy.deepcopy(recipe)
        invalid['version'] = 2
        self.assertIn('Version', [i['code'] for i in json.loads(request('/api/pipelines/check', invalid)[1])['issues']])
        invalid['version'] = 3
        invalid['nodes'][-1]['index'] = 9
        self.assertIn('Setting', [i['code'] for i in json.loads(request('/api/pipelines/check', invalid)[1])['issues']])
        invalid['nodes'][-1]['index'] = 0
        invalid['nodes'][-1]['kinds'] = ['file', 'file']
        self.assertIn('Type', [i['code'] for i in json.loads(request('/api/pipelines/check', invalid)[1])['issues']])
        invalid['nodes'][-1]['kinds'][1] = 'arbitrary-class'
        self.assertIn('Structure', [i['code'] for i in json.loads(request('/api/pipelines/check', invalid)[1])['issues']])
        recipe['nodes'][-1] = {'id': 'keep', 'op': 'stop-after', 'message': 'Checked before stopping',
                               'kind': 'file', 'kinds': ['receipt'], 'inputs': ['checks']}
        status, data = request('/api/pipelines/check', recipe)
        self.assertEqual(status, 200, data)
        self.assertEqual(json.loads(data)['recipe'], recipe)
        status, data = self.run_recipe(recipe)
        self.assertEqual(status, 400, data)
        self.assertIn('Checked before stopping', json.loads(data)['error'])

    def test_strict_json_and_structural_limits(self):
        for raw in [b'{"version":1,"version":1}', b'{}{}', b'{"name":"\xff"}', b'[' * 18 + b']' * 18, b' ' * 32769]:
            with self.subTest(raw=raw[:60]):
                status, data = request('/api/pipelines/check', raw=raw)
                self.assertEqual(status, 400, data)
                self.assertEqual(json.loads(data)['issues'][0]['code'], 'Structure')
        recipe = copy.deepcopy(self.recipe)
        recipe['nodes'][1]['unexpected'] = 'no'
        recipe['nodes'][1]['pages'] = ['1', 1.2]
        status, data = request('/api/pipelines/check', recipe)
        self.assertEqual(status, 400, data)
        self.assertEqual(len(json.loads(data)['issues']), 3)

    def test_independent_errors_then_port_reference_and_cycle_checks(self):
        recipe = copy.deepcopy(self.recipe)
        recipe['nodes'][1] = {'id': 'cut', 'op': 'number', 'start': 0, 'style': 'bad', 'prefix': 'x' * 25, 'inputs': ['body']}
        status, data = request('/api/pipelines/check', recipe)
        self.assertEqual(status, 400, data)
        self.assertEqual([i['path'] for i in json.loads(data)['issues']], ['nodes[1].start', 'nodes[1].style', 'nodes[1].prefix'])
        recipe['nodes'][1] = {'id': 'cut', 'op': 'stamp', 'text': 'DRAFT', 'inputs': ['pdf']}
        self.assertIn('Type', [i['code'] for i in json.loads(request('/api/pipelines/check', recipe)[1])['issues']])
        recipe['nodes'][1]['inputs'] = ['cut']
        self.assertIn('Cycle', [i['code'] for i in json.loads(request('/api/pipelines/check', recipe)[1])['issues']])
        self.assertEqual(self.run_recipe(bindings={'body': 'expired'})[0], 400)
        self.assertEqual(self.run_recipe(bindings={'extra': self.doc['id']})[0], 400)

    def test_selection_fork_join_and_searchable_finishing(self):
        nodes = self.recipe['nodes']
        nodes[2:2] = [
            {'id': 'join', 'op': 'assemble', 'catalog': 0, 'inputs': ['cut', 'cut']},
            {'id': 'stamp', 'op': 'stamp', 'text': 'REVIEW (A)', 'placement': 'top', 'inputs': ['join']},
            {'id': 'number', 'op': 'number', 'start': 7, 'style': 'bates', 'prefix': 'DOC-', 'inputs': ['stamp']}]
        nodes[-1]['inputs'] = ['number']
        status, data = self.run_recipe()
        self.assertEqual(status, 200, data)
        result = json.loads(data)
        self.assertEqual(len(result['executed']), len(set(result['executed'])))
        doc = result['outputs'][0]['document']
        self.assertEqual(len(doc['pages']), 6)
        status, pdf = request('/api/documents/' + doc['id'] + '/file')
        self.assertEqual(status, 200)
        self.assertEqual(hashlib.sha256(pdf).hexdigest(), doc['sha256'])
        if shutil.which('qpdf') and shutil.which('pdftotext'):
            with tempfile.TemporaryDirectory(prefix='pipeline-synthetic-') as folder:
                path = Path(folder) / 'packet.pdf'
                path.write_bytes(pdf)
                checked = subprocess.run(['qpdf', '--check', str(path)], capture_output=True, text=True)
                self.assertEqual(checked.returncode, 0, checked.stdout + checked.stderr)
                pages = subprocess.check_output(['pdftotext', str(path), '-'], text=True).split('\f')[:-1]
                for i, (page, title) in enumerate(zip(pages, ['FORM', 'ORBIT', 'SIGNAL'] * 2)):
                    self.assertIn(title, page)
                    self.assertIn('REVIEW (A)', page)
                    self.assertIn(f'DOC-{i + 7:06}', page)

    def test_validation_branches_share_one_pass_and_exact_output_bytes(self):
        status, valid = request('/api/validation-samples/valid')
        self.assertEqual(status, 200)
        status, doc = request('/api/pipeline-inputs/archive', raw=valid)
        self.assertEqual(status, 200, doc)
        archive = json.loads(doc)
        recipe = {'version': 1, 'name': 'Archive recipe', 'nodes': [
            {'id': 'source', 'op': 'input', 'slot': 'archive', 'inputs': []},
            {'id': 'stamp', 'op': 'stamp', 'text': 'ARCHIVE', 'inputs': ['source']},
            {'id': 'number', 'op': 'number', 'start': 1, 'prefix': 'DOC-', 'style': 'bates', 'inputs': ['stamp']},
            {'id': 'pdf', 'op': 'write', 'inputs': ['number']},
            {'id': 'check3', 'op': 'validate', 'profiles': ['3b'], 'inputs': ['pdf']},
            {'id': 'check2', 'op': 'validate', 'profiles': ['2b', '3u'], 'inputs': ['pdf']}], 'outputs': ['pdf', 'check3', 'check2']}
        status, data = self.run_recipe(recipe, {'archive': archive['id']})
        self.assertEqual(status, 200, data)
        result = json.loads(data)
        self.assertEqual(result['validatorPasses'], 1)
        output, receipt3, receipt2 = result['outputs']
        hash_value = output['document']['sha256']
        self.assertEqual(receipt3['report']['sha256'], hash_value)
        self.assertEqual(receipt2['report']['sha256'], hash_value)
        self.assertEqual(receipt3['report']['checkedAt'], receipt2['report']['checkedAt'])
        self.assertEqual([r['profile'] for r in receipt3['report']['validation']['results']], ['3b'])
        self.assertEqual({r['profile'] for r in receipt2['report']['validation']['results']}, {'2b', '3u'})
        self.assertEqual(receipt3['report']['validation']['results'][0]['status'], 'passed')

    def test_multiple_outputs_and_pinned_inputs_survive_explorer_eviction(self):
        status, data = request('/api/pipeline-inputs/body?document=' + self.doc['id'], raw=b'')
        self.assertEqual(status, 200, data)
        for _ in range(4):
            self.assertEqual(request('/api/sample', raw=b'')[0], 200)
        recipe = copy.deepcopy(self.recipe)
        recipe['nodes'].append({'id': 'original', 'op': 'write', 'inputs': ['body']})
        recipe['outputs'].append('original')
        status, data = self.run_recipe(recipe)
        self.assertEqual(status, 200, data)
        outputs = json.loads(data)['outputs']
        self.assertEqual([len(o['document']['pages']) for o in outputs], [3, 4])
        for output in outputs:
            self.assertEqual(request('/api/documents/' + output['document']['id'] + '/preview?page=1')[0], 200)
        self.assertEqual(self.run_recipe()[0], 200)

    def test_execution_failure_does_not_change_source_or_accept_foreign_origin(self):
        self.recipe['nodes'][1]['pages'] = [65]
        self.assertEqual(self.run_recipe()[0], 400)
        self.assertEqual(request('/api/documents/' + self.doc['id'] + '/file')[0], 200)
        req = urllib.request.Request(BASE + '/api/pipelines/run', data=b'{}', headers={'Origin': 'https://example.org'})
        with self.assertRaises(urllib.error.HTTPError) as error:
            urllib.request.urlopen(req)
        self.assertEqual(error.exception.code, 403)


if __name__ == '__main__':
    unittest.main(verbosity=2)
