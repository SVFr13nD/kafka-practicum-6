"""Regression checks without Kafka: old/duplicate records and failed HDFS writes."""
import os
from pathlib import Path
import runpy
import types
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]

class Message:
    def __init__(self, ident):
        self.ident = ident
    def partition(self):
        return 0
    def offset(self):
        return int(self.ident.rsplit('-', 1)[-1])

class FakeConsumer:
    def __init__(self, config):
        self.messages = iter(map(Message, ['old-0', 'run-0', 'run-0'] + [f'run-{i}' for i in range(1, 6)]))
        self.committed = []
        self.closed = False
    def subscribe(self, topics):
        pass
    def poll(self, timeout):
        return next(self.messages)
    def commit(self, message, asynchronous):
        self.committed.append(message.ident)
    def close(self):
        self.closed = True

class RunIsolation(unittest.TestCase):
    def execute(self, filename, fail_write=False):
        consumer = FakeConsumer({})
        common = types.ModuleType('common')
        common.kafka = lambda: {}
        common.TOPIC = 'orders'
        common.registered_schema = lambda: {}
        common.decode = lambda msg, schema: {'id': msg.ident}
        common.expected_ids = lambda: {f'run-{i}' for i in range(6)}
        kafka = types.ModuleType('confluent_kafka')
        kafka.Consumer = lambda config: consumer
        requests = types.ModuleType('requests')
        writes = []
        def put(url, **kwargs):
            if 'data' in kwargs:
                if fail_write:
                    raise RuntimeError('HDFS unavailable')
                writes.append(kwargs['data'])
            return types.SimpleNamespace(status_code=307, headers={'Location': 'http://datanode/file'}, raise_for_status=lambda: None)
        requests.put = put
        modules = {'common': common, 'confluent_kafka': kafka, 'requests': requests}
        with patch.dict('sys.modules', modules), patch.dict(os.environ, RUN_ID='run'):
            if fail_write:
                with self.assertRaisesRegex(RuntimeError, 'HDFS unavailable'):
                    runpy.run_path(str(ROOT/'app'/filename))
            else:
                runpy.run_path(str(ROOT/'app'/filename))
        self.assertTrue(consumer.closed)
        return consumer, writes

    def test_reader_ignores_old_and_duplicate_ids(self):
        consumer, _ = self.execute('consumer.py')
        self.assertEqual(consumer.committed, [f'run-{i}' for i in range(6)])

    def test_sink_writes_each_current_record_once(self):
        consumer, writes = self.execute('hdfs_sink.py')
        self.assertEqual(len(writes), 6)
        self.assertEqual(consumer.committed, [f'run-{i}' for i in range(6)])

    def test_sink_does_not_commit_failed_write(self):
        consumer, _ = self.execute('hdfs_sink.py', fail_write=True)
        self.assertEqual(consumer.committed, [])

if __name__ == '__main__':
    unittest.main()
