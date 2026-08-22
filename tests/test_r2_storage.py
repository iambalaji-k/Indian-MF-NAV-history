from __future__ import annotations

import io
import os
import urllib.error
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock, call, patch

from scripts.fetch_and_update import r2_key_for_db
from scripts.r2_storage import (
    R2Config,
    _read_lock_timestamp,
    acquire_r2_lock,
    atomic_upload_object,
    download_object,
    load_dotenv,
    r2_lock,
    run_with_retries,
)
from tests.test_fetch_and_update import WorkspaceTemporaryDirectory


def http_error(code: int) -> urllib.error.HTTPError:
    return urllib.error.HTTPError("https://example.invalid", code, "err", {}, io.BytesIO(b"err"))


def make_config() -> R2Config:
    return R2Config(
        account_id="account123",
        bucket="nav-archive",
        access_key_id="access",
        secret_access_key="secret",
        endpoint="https://account123.r2.cloudflarestorage.com",
    )


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
        self.assertEqual(config.object_key("db/nav_fy_2026_27.db"), "archive/db/nav_fy_2026_27.db")

    def test_r2_db_keys_are_stable(self) -> None:
        data_dir = Path("data")

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

    def test_atomic_db_upload_uses_tmp_verify_backups_and_promote(self) -> None:
        config = R2Config(
            account_id="account123",
            bucket="nav-archive",
            access_key_id="access",
            secret_access_key="secret",
            endpoint="https://account123.r2.cloudflarestorage.com",
        )
        with WorkspaceTemporaryDirectory() as tmp:
            source = Path(tmp) / "nav_fy_2026_27.db"
            source.write_bytes(b"sqlite")

            with (
                patch("scripts.r2_storage.upload_object") as upload,
                patch("scripts.r2_storage.verify_object_exists") as verify,
                patch("scripts.r2_storage.copy_object") as copy,
                patch("scripts.r2_storage.delete_object") as delete,
            ):
                atomic_upload_object(config, "db/nav_fy_2026_27.db", source)

        upload.assert_called_once_with(config, "db/nav_fy_2026_27.db.tmp", source)
        verify.assert_has_calls([call(config, "db/nav_fy_2026_27.db.tmp"), call(config, "db/nav_fy_2026_27.db")])
        copy.assert_has_calls(
            [
                call(config, "db/nav_fy_2026_27.db.bak1", "db/nav_fy_2026_27.db.bak2"),
                call(config, "db/nav_fy_2026_27.db", "db/nav_fy_2026_27.db.bak1"),
                call(config, "db/nav_fy_2026_27.db.tmp", "db/nav_fy_2026_27.db"),
            ]
        )
        delete.assert_called_once_with(config, "db/nav_fy_2026_27.db.tmp")

    def test_r2_lock_uses_expected_lock_key(self) -> None:
        config = make_config()

        with (
            patch("scripts.r2_storage.upload_bytes") as upload,
            patch("scripts.r2_storage.delete_object") as delete,
        ):
            with r2_lock(config):
                pass

        self.assertEqual(upload.call_args.args[1], "lock/nav.lock")
        self.assertEqual(upload.call_args.kwargs["extra_headers"], {"if-none-match": "*"})
        delete.assert_called_once_with(config, "lock/nav.lock")

    def test_stale_lock_is_taken_over(self) -> None:
        config = make_config()
        stale_time = datetime.now(timezone.utc) - timedelta(hours=3)

        with (
            patch(
                "scripts.r2_storage.upload_bytes",
                side_effect=[http_error(409), None],
            ) as upload,
            patch("scripts.r2_storage._read_lock_timestamp", return_value=stale_time),
            patch("scripts.r2_storage.delete_object") as delete,
        ):
            with r2_lock(config):
                pass

        self.assertEqual(upload.call_count, 2)
        # One delete for the takeover, one for the release.
        self.assertEqual(delete.call_count, 2)

    def test_fresh_lock_rejects_without_takeover(self) -> None:
        config = make_config()
        fresh_time = datetime.now(timezone.utc) - timedelta(minutes=5)

        with (
            patch("scripts.r2_storage.upload_bytes", side_effect=http_error(409)),
            patch("scripts.r2_storage._read_lock_timestamp", return_value=fresh_time),
            patch("scripts.r2_storage.delete_object") as delete,
        ):
            with self.assertRaises(RuntimeError):
                acquire_r2_lock(config)

        delete.assert_not_called()

    def test_naive_lock_timestamp_is_treated_as_utc(self) -> None:
        config = make_config()

        class FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def read(self, size: int = -1) -> bytes:
                return b"2020-01-01T00:00:00"  # naive ISO stamp, no tz offset

        with patch("scripts.r2_storage.urllib.request.urlopen", return_value=FakeResponse()):
            stamp = _read_lock_timestamp(config, "lock/nav.lock")

        self.assertIsNotNone(stamp)
        self.assertIsNotNone(stamp.tzinfo)
        # Aware-vs-naive subtraction must not raise TypeError.
        age_seconds = (datetime.now(timezone.utc) - stamp).total_seconds()
        self.assertGreater(age_seconds, 3600)

    def test_download_verifies_sha256_metadata(self) -> None:
        config = make_config()

        class FakeResponse:
            def __init__(self, metadata_hash: str | None, payload: bytes) -> None:
                self._payload = payload
                self.headers = {"x-amz-meta-sha256": metadata_hash} if metadata_hash else {}

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def read(self, size: int = -1) -> bytes:
                data = self._payload
                self._payload = b""
                return data

        payload = b"sqlite-bytes"
        destination = Path("ignored") / "nav.db"

        with WorkspaceTemporaryDirectory() as tmp:
            good_destination = Path(tmp) / "good.db"
            bad_destination = Path(tmp) / "bad.db"
            from scripts.r2_storage import sha256_hex

            with patch(
                "scripts.r2_storage.urllib.request.urlopen",
                side_effect=[
                    FakeResponse(sha256_hex(payload), payload),
                    FakeResponse("0" * 96, payload),
                ],
            ):
                self.assertTrue(download_object(config, "db/good.db", good_destination))
                with self.assertRaises(RuntimeError):
                    download_object(config, "db/bad.db", bad_destination)

            self.assertEqual(good_destination.read_bytes(), payload)
            self.assertFalse(bad_destination.exists(), "corrupt download must not leave artifacts")
            self.assertFalse((Path(tmp) / "bad.db.part").exists())


if __name__ == "__main__":
    unittest.main()
