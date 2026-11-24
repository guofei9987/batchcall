import json
import logging
import tempfile
import threading
import time
import unittest
from pathlib import Path

from batchcall import LogConfig, RetryConfig, StorageConfig, iter_results, load_success_ids, run
from batchcall.runner import _CallerState


class Caller:
    def __init__(self, name='test', concurrency=2, adaptive=False, max_concurrency=10):
        self.name = name
        self.concurrency = concurrency
        self.adaptive = adaptive
        self.max_concurrency = max_concurrency
        self.active = self.peak = 0
        self.calls = []
        self.lock = threading.Lock()

    def __call__(self, payload):
        with self.lock:
            self.active += 1
            self.peak = max(self.peak, self.active)
            self.calls.append(payload)
        try:
            time.sleep(0.002)
            return {'status': 'success', 'output': payload}
        finally:
            with self.lock:
                self.active -= 1


class BatchCallTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'results.jsonl'
        self.storage = StorageConfig(self.path, save_every_n=3)
        self.logs = LogConfig(console=False, log_every_n=2)

    def execute(self, data, callers, **kwargs):
        return run(data, callers, storage_config=kwargs.pop('storage_config', self.storage),
                   log_config=kwargs.pop('log_config', self.logs), **kwargs)

    def test_concurrent_and_resume(self):
        a, b = Caller('a', 2), Caller('b', 4)
        data = {str(i): json.dumps(i) for i in range(40)}
        s = self.execute(data, [a, b])
        self.assertEqual((s['success'], s['attempts'], s['failed']), (40, 40, 0))
        self.assertGreater(len(a.calls), 0)
        self.assertGreater(len(b.calls), 0)
        self.assertLessEqual(a.peak, 2)
        self.assertLessEqual(b.peak, 4)
        self.assertEqual(load_success_ids(self.path), set(data))
        self.assertEqual(len(list(iter_results(self.path))), 40)
        s = self.execute(data, [a, b])
        self.assertEqual((s['skipped'], s['attempts']), (40, 0))

    def test_rounds_and_retry_budget(self):
        counts = {}
        lock = threading.Lock()
        def caller(payload):
            with lock:
                counts[payload] = counts.get(payload, 0) + 1
                n = counts[payload]
            if payload == '1' and n == 1:
                return {'status': 'rate_limited', 'output': None}
            if payload == '2':
                raise RuntimeError('failure')
            return {'status': 'success', 'output': payload, 'duration_s': 1.23456}
        s = self.execute({'a':'0', 'b':'1', 'c':'2'}, caller, retry_config=RetryConfig(2))
        self.assertEqual((s['attempts'], s['success'], s['failed'], s['rate_limited']), (6, 2, 1, 1))
        records = list(iter_results(self.path))
        self.assertEqual([r['round'] for r in records], [0,0,0,1,1,2])
        self.assertEqual({r['id'] for r in records if r['round']==0}, {'a','b','c'})
        self.assertEqual(next(r for r in records if r['id']=='a')['output']['elapsed'], 1.235)
        s = self.execute({'a':'0', 'b':'1', 'c':'2'}, caller, retry_config=RetryConfig(0))
        self.assertEqual((s['skipped'], s['attempts'], s['failed']), (2,1,1))

    def test_tail_repair(self):
        caller = Caller()
        self.execute({'a':'0'}, caller)
        original = self.path.read_bytes()
        self.path.write_bytes(original + b'{"id":"broken","output":\xe4')
        self.execute({'a':'0','b':'1'}, caller)
        self.assertEqual(load_success_ids(self.path), {'a','b'})
        self.assertTrue(self.path.read_bytes().startswith(original))
        self.path.write_bytes(self.path.read_bytes().rstrip(b'\n'))
        s = self.execute({'a':'0','b':'1'}, caller)
        self.assertEqual(s['skipped'], 2)
        self.assertTrue(self.path.read_bytes().endswith(b'\n'))

    def test_middle_corruption(self):
        self.path.write_bytes(b'broken\n{}\n')
        before = self.path.read_bytes()
        with self.assertRaises(ValueError):
            self.execute({'a':'0'}, Caller())
        self.assertEqual(self.path.read_bytes(), before)

    def test_adaptive_thresholds_independent(self):
        a = _CallerState.from_caller(Caller('a', 10, True, 11), 0)
        b = _CallerState.from_caller(Caller('b', 20, True, 30), 1)
        logger = logging.Logger('test')
        for i in range(100):
            a.completed('rate_limited' if i == 0 else 'success', logger)
            b.completed('rate_limited' if i < 6 else 'success', logger)
        self.assertEqual((a.current, b.current), (11, 10))
        for i in range(100):
            b.completed('rate_limited' if i < 5 else 'success', logger)
        self.assertEqual(b.current, 10)
        for i in range(100):
            a.completed('success', logger)
        self.assertEqual(a.current, 11)
        self.assertEqual(a.caller.concurrency, 10)
        b.current = 1
        for i in range(100):
            b.completed('rate_limited', logger)
        self.assertEqual(b.current, 1)

    def test_resume_false_and_invalid_result(self):
        self.execute({'a':'0'}, Caller())
        storage = StorageConfig(self.path, resume=False)
        s = self.execute({'a':'0'}, lambda x: {}, storage_config=storage, retry_config=RetryConfig(0))
        self.assertEqual((s['skipped'], s['failed']), (0,1))
        self.assertEqual(len(list(iter_results(self.path))), 2)
        self.assertEqual(load_success_ids(self.path), {'a'})

    def test_config_and_path_validation(self):
        with self.assertRaises(ValueError):
            StorageConfig(save_every_n=0)
        c = Caller(concurrency=20, adaptive=True, max_concurrency=10)
        with self.assertRaises(ValueError):
            self.execute({'a':'0'}, c)
        with self.assertRaises(ValueError):
            self.execute({'a':'0'}, [c,c])
        with self.assertRaises(ValueError):
            self.execute({'a':'0'}, Caller(), log_config=LogConfig(log_path=self.path))
        self.assertFalse(self.path.exists())

    def test_logs_and_empty(self):
        log_path = self.path.with_suffix('.log')
        s = self.execute({}, Caller(), log_config=LogConfig(console=False, log_path=log_path))
        self.assertEqual(s['attempts'], 0)
        self.assertEqual(list(iter_results(self.path)), [])
        self.assertIn('FINISH', log_path.read_text())


if __name__ == '__main__':
    unittest.main()
