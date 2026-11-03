"""Synthetic integration checks for data-defined recipes against the local studio."""
import copy
import hashlib
import json
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
    req = urllib.request.Request(BASE + path, data=data, headers={'Content-Type': 'application/json'})
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

    def test_volga_example_round_trips_to_portable_data(self):
        status, data = request('/api/pipelines/example')
        self.assertEqual(status, 200, data)
        recipe = json.loads(data)
        self.assertEqual([n['op'] for n in recipe['nodes']], ['input', 'select', 'stamp', 'number', 'write', 'validate'])
        status, checked = request('/api/pipelines/check', recipe)
        self.assertEqual(status, 200, checked)
        self.assertEqual(json.loads(checked)['recipe'], recipe)
        self.assertEqual(recipe['outputs'], ['pdf', 'checks'])

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
