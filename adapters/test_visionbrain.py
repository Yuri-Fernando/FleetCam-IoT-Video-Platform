import importlib.util
import os
import pathlib
import tempfile
import unittest
import json

class SpoolTests(unittest.TestCase):
    def test_duplicate_and_device_binding(self):
        with tempfile.TemporaryDirectory() as folder:
            os.environ['FLEETCAM_ADAPTER_DB'] = str(pathlib.Path(folder) / 'spool.sqlite3')
            os.environ['FLEETCAM_DEVICE'] = 'test-device'
            spec = importlib.util.spec_from_file_location('visionbrain_adapter', pathlib.Path(__file__).with_name('visionbrain.py'))
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            module.setup()
            event = {'event_type':'capture_quality_degraded','timestamp':'2026-01-01T00:00:00Z','frame_index':1,'payload':{}}
            self.assertEqual(module.enqueue(event), module.enqueue(event))
            with module.db() as conn:
                self.assertEqual(conn.execute('SELECT count(*) FROM queue').fetchone()[0], 1)
            legacy = pathlib.Path(folder) / 'legacy.jsonl'
            record = {'delivery_status':'pending','reason':'timeout','event':event}
            legacy.write_text(json.dumps(record) + '\n', encoding='utf-8')
            self.assertEqual(module.import_legacy_spool(legacy), 1)
            with module.db() as conn:
                self.assertEqual(conn.execute('SELECT count(*) FROM queue').fetchone()[0], 1)
            self.assertTrue(legacy.exists())
            trusted = pathlib.Path(folder) / 'trusted'
            trusted.mkdir()
            outside = pathlib.Path(folder) / 'outside.png'
            outside.write_bytes(b'not read')
            external = dict(event, frame_index=2, payload={'snapshot':str(outside)})
            digest = module.enqueue(external)
            with module.db() as conn:
                conn.execute('UPDATE queue SET delivered=1 WHERE digest=?', (digest,))
            module.SNAPSHOT_ROOT = str(trusted)
            module.replay_snapshots()
            with module.db() as conn:
                self.assertEqual(conn.execute('SELECT snapshot_delivered FROM queue WHERE digest=?',(digest,)).fetchone()[0],0)
            module.DEVICE = 'other-device'
            with self.assertRaises(ValueError): module.setup()

if __name__ == '__main__': unittest.main()
