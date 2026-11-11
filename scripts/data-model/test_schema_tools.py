"""Testes unitários (sem Docker) das ferramentas de catálogo do FleetCam."""
from __future__ import annotations

import copy
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import schema_tools as st  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
GENERATED = ROOT / "docs" / "data-model" / "generated"


def _model():
    return {"tables": {"devices": {
        "columns": [{"name": "tenant_id", "type": "text", "nullable": False, "default": None},
                    {"name": "id", "type": "text", "nullable": False, "default": None}],
        "primary_key": ["tenant_id", "id"], "foreign_keys": [], "unique": [], "checks": [], "indexes": []}}}


class DiffTest(unittest.TestCase):
    def test_no_change(self):
        self.assertEqual(st.diff(_model(), _model()), [])

    def test_added_column_and_check(self):
        new = _model()
        new["tables"]["devices"]["columns"].append(
            {"name": "lifecycle_state", "type": "text", "nullable": False, "default": "'PROVISIONED'::text"})
        new["tables"]["devices"]["checks"].append("CHECK (lifecycle_state = ANY (ARRAY['A'::text]))")
        d = st.diff(_model(), new)
        self.assertTrue(any(x.startswith("+ coluna devices.lifecycle_state text NOT NULL") for x in d))
        self.assertTrue(any(x.startswith("+ check devices") for x in d))

    def test_type_and_nullability(self):
        new = copy.deepcopy(_model())
        new["tables"]["devices"]["columns"][1].update(type="uuid", nullable=True)
        self.assertEqual(st.diff(_model(), new), ["~ tipo devices.id: text -> uuid", "~ nulidade devices.id: NOT NULL -> NULL"])


class DictionaryTest(unittest.TestCase):
    def test_full_coverage(self):
        csv_text, errors = st.dictionary(_model(), {"devices": {"tenant_id": ["t", "INTERNO"], "id": ["i", "INTERNO"]}})
        self.assertEqual(errors, [])
        self.assertIn("devices,tenant_id,text,NO,,YES", csv_text)

    def test_missing_and_stale_descriptions(self):
        _, errors = st.dictionary(_model(), {"devices": {"tenant_id": ["t", "INTERNO"], "fantasma": ["x", "INTERNO"]}})
        self.assertIn("coluna sem descrição: devices.id", errors)
        self.assertIn("descrição sem coluna física: devices.fantasma", errors)

    def test_invalid_classification(self):
        _, errors = st.dictionary(_model(), {"devices": {"tenant_id": ["t", "SECRETO"], "id": ["i", "INTERNO"]}})
        self.assertTrue(any("classificação inválida" in e for e in errors))


class CommittedArtifactsTest(unittest.TestCase):
    """Os artefatos versionados vieram do laboratório e batem com as descrições."""

    def test_committed_dictionary_covers_committed_snapshot(self):
        latest = sorted(GENERATED.glob("schema-v*.json"), key=lambda p: int(p.stem.split("-v")[1]))[-1]
        model = json.loads(latest.read_text(encoding="utf-8"))
        desc = json.loads((ROOT / "docs" / "data-model" / "descriptions.json").read_text(encoding="utf-8"))
        csv_text, errors = st.dictionary(model, desc)
        self.assertEqual(errors, [])
        self.assertEqual(csv_text, (ROOT / "docs" / "data-model" / "dictionary.csv").read_text(encoding="utf-8"))

    def test_multitenant_fks_are_composite(self):
        latest = sorted(GENERATED.glob("schema-v*.json"), key=lambda p: int(p.stem.split("-v")[1]))[-1]
        model = json.loads(latest.read_text(encoding="utf-8"))
        for fk in model["tables"]["ingest_events"]["foreign_keys"]:
            self.assertEqual(fk["columns"], ["tenant_id", "device_id"])


if __name__ == "__main__":
    unittest.main()
