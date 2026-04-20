from __future__ import annotations

import os
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.fetch_and_update import r2_key_for_db
from scripts.r2_storage import R2Config, load_dotenv
from tests.test_fetch_and_update import WorkspaceTemporaryDirectory


class R2StorageTests(unittest.TestCase):
    def test_r2_config_uses_expected_default_endpoint_and_prefix(self) -> None:
        env = {
            "R2_ACCOUNT_ID": "account123",
            "R2_BUCKET": "nav-archive",
            "R2_ACCESS_KEY_ID": "access",
            "R2_SECRET_ACCESS_KEY": "secret",
            "R2_PREFIX": "archive",
        }
        with patch.dict(os.environ, env, clear=True):
            config = R2Config.from_env()

        self.assertEqual(config.endpoint, "https://account123.r2.cloudflarestorage.com")
        self.assertEqual(config.object_key("db/nav.db"), "archive/db/nav.db")

    def test_r2_db_keys_are_stable(self) -> None:
        data_dir = Path("data")

        self.assertEqual(r2_key_for_db(data_dir / "nav.db", data_dir), "db/nav.db")
        self.assertEqual(
            r2_key_for_db(data_dir / "nav_fy_2026_27.db", data_dir),
            "db/nav_fy_2026_27.db",
        )

    def test_dotenv_loader_does_not_override_existing_environment(self) -> None:
        with WorkspaceTemporaryDirectory() as tmp:
            env_path = Path(tmp) / ".env"
            env_path.write_text("R2_BUCKET=from-file\nR2_PREFIX=from-file\n", encoding="utf-8")

            with patch.dict(os.environ, {"R2_BUCKET": "existing"}, clear=True):
                load_dotenv(env_path)
                self.assertEqual(os.environ["R2_BUCKET"], "existing")
                self.assertEqual(os.environ["R2_PREFIX"], "from-file")


if __name__ == "__main__":
    unittest.main()
