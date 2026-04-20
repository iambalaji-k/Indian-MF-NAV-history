from __future__ import annotations

import os
import urllib.error
import unittest
from pathlib import Path
from unittest.mock import Mock, call, patch

from scripts.fetch_and_update import r2_key_for_db
from scripts.r2_storage import atomic_upload_object, load_dotenv, r2_lock, R2Config, run_with_retries
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

    def test_retry_logic_retries_transient_errors(self) -> None:
        operation = Mock(side_effect=[urllib.error.URLError("temporary"), "ok"])

        with patch("scripts.r2_storage.time.sleep") as sleep:
            result = run_with_retries("test operation", operation, retries=2)

        self.assertEqual(result, "ok")
        self.assertEqual(operation.call_count, 2)
        sleep.assert_called_once()

    def test_atomic_master_upload_uses_tmp_verify_backups_and_promote(self) -> None:
        config = R2Config(
            account_id="account123",
            bucket="nav-archive",
            access_key_id="access",
            secret_access_key="secret",
            endpoint="https://account123.r2.cloudflarestorage.com",
        )
        with WorkspaceTemporaryDirectory() as tmp:
            source = Path(tmp) / "nav.db"
            source.write_bytes(b"sqlite")

            with (
                patch("scripts.r2_storage.upload_object") as upload,
                patch("scripts.r2_storage.verify_object_exists") as verify,
                patch("scripts.r2_storage.copy_object") as copy,
                patch("scripts.r2_storage.delete_object") as delete,
            ):
                atomic_upload_object(config, "db/nav.db", source)

        upload.assert_called_once_with(config, "db/nav.db.tmp", source)
        verify.assert_has_calls([call(config, "db/nav.db.tmp"), call(config, "db/nav.db")])
        copy.assert_has_calls(
            [
                call(config, "db/nav.db.bak1", "db/nav.db.bak2"),
                call(config, "db/nav.db", "db/nav.db.bak1"),
                call(config, "db/nav.db.tmp", "db/nav.db"),
            ]
        )
        delete.assert_called_once_with(config, "db/nav.db.tmp")

    def test_r2_lock_uses_expected_lock_key(self) -> None:
        config = R2Config(
            account_id="account123",
            bucket="nav-archive",
            access_key_id="access",
            secret_access_key="secret",
            endpoint="https://account123.r2.cloudflarestorage.com",
        )

        with (
            patch("scripts.r2_storage.upload_bytes") as upload,
            patch("scripts.r2_storage.delete_object") as delete,
        ):
            with r2_lock(config):
                pass

        self.assertEqual(upload.call_args.args[1], "lock/nav.lock")
        self.assertEqual(upload.call_args.kwargs["extra_headers"], {"if-none-match": "*"})
        delete.assert_called_once_with(config, "lock/nav.lock")


if __name__ == "__main__":
    unittest.main()
