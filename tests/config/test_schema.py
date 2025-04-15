# Copyright © 2024, 2025 Brockmann Consult and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import unittest

from zappend.config import get_config_schema


class ConfigSchemaTest(unittest.TestCase):
    def test_get_config_schema(self):
        schema = get_config_schema()
        self.assertIn("properties", schema)
        self.assertIsInstance(schema["properties"], dict)
        self.assertEqual(
            {
                "append_dim",
                "append_step",
                "attrs",
                "attrs_update_mode",
                "disable_rollback",
                "dry_run",
                "excluded_variables",
                "extra",
                "force_new",
                "fixed_dims",
                "included_variables",
                "logging",
                "permit_eval",
                "persist_mem_slices",
                "profiling",
                "slice_engine",
                "slice_polling",
                "slice_source",
                "slice_source_kwargs",
                "slice_storage_options",
                "target_storage_options",
                "target_dir",
                "temp_dir",
                "temp_storage_options",
                "variables",
                "version",
                "zarr_version",
            },
            set(schema["properties"].keys()),
        )

    def test_get_config_schema_json(self):
        # Smoke test is sufficient here
        text = get_config_schema(format="json")
        self.assertIsInstance(text, str)
        self.assertTrue(len(text) > 0)

    def test_get_config_schema_md(self):
        # Smoke test is sufficient here
        text = get_config_schema(format="md")
        self.assertIsInstance(text, str)
        self.assertTrue(len(text) > 0)
