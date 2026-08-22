from __future__ import annotations

import hashlib
import hmac
import logging
import os
import random
import time
import urllib.error
import urllib.parse
import urllib.request
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass(frozen=True)
class R2Config:
    account_id: str
    bucket: str
    access_key_id: str
    secret_access_key: str
    endpoint: str
    prefix: str = ""
    region: str = "auto"

    @classmethod
    def from_env(cls) -> "R2Config":
        account_id = required_env("R2_ACCOUNT_ID")
        endpoint = os.environ.get("R2_ENDPOINT") or f"https://{account_id}.r2.cloudflarestorage.com"
        return cls(
            account_id=account_id,
            bucket=required_env("R2_BUCKET"),
            access_key_id=required_env("R2_ACCESS_KEY_ID"),
            secret_access_key=required_env("R2_SECRET_ACCESS_KEY"),
            endpoint=endpoint.rstrip("/"),
            prefix=os.environ.get("R2_PREFIX", "").strip("/"),
            region=os.environ.get("R2_REGION", "auto"),
        )

    def object_key(self, name: str) -> str:
        clean_name = name.replace("\\", "/").lstrip("/")
        if not self.prefix:
            return clean_name
        return f"{self.prefix}/{clean_name}"


def required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def sha256_hex(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def hmac_sha256(key: bytes, message: str) -> bytes:
    return hmac.new(key, message.encode("utf-8"), hashlib.sha256).digest()


def signing_key(secret_key: str, date_stamp: str, region: str) -> bytes:
    date_key = hmac_sha256(("AWS4" + secret_key).encode("utf-8"), date_stamp)
    region_key = hmac_sha256(date_key, region)
    service_key = hmac_sha256(region_key, "s3")
    return hmac_sha256(service_key, "aws4_request")


def signed_request(
    config: R2Config,
    method: str,
    key: str,
    payload: bytes = b"",
    extra_headers: dict[str, str] | None = None,
) -> urllib.request.Request:
    now = datetime.now(timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")
    parsed_endpoint = urllib.parse.urlparse(config.endpoint)
    object_path = f"/{config.bucket}/{urllib.parse.quote(config.object_key(key), safe='/')}"
    url = urllib.parse.urlunparse(
        (
            parsed_endpoint.scheme,
            parsed_endpoint.netloc,
            object_path,
            "",
            "",
            "",
        )
    )

    payload_hash = sha256_hex(payload)
    headers = {
        "host": parsed_endpoint.netloc,
        "x-amz-content-sha256": payload_hash,
        "x-amz-date": amz_date,
    }
    if extra_headers:
        headers.update({name.lower(): value for name, value in extra_headers.items()})
    canonical_headers = "".join(f"{name}:{headers[name]}\n" for name in sorted(headers))
    signed_headers = ";".join(sorted(headers))
    canonical_request = "\n".join(
        [
            method,
            object_path,
            "",
            canonical_headers,
            signed_headers,
            payload_hash,
        ]
    )
    credential_scope = f"{date_stamp}/{config.region}/s3/aws4_request"
    string_to_sign = "\n".join(
        [
            "AWS4-HMAC-SHA256",
            amz_date,
            credential_scope,
            sha256_hex(canonical_request.encode("utf-8")),
        ]
    )
    signature = hmac.new(
        signing_key(config.secret_access_key, date_stamp, config.region),
        string_to_sign.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    headers["Authorization"] = (
        "AWS4-HMAC-SHA256 "
        f"Credential={config.access_key_id}/{credential_scope}, "
        f"SignedHeaders={signed_headers}, "
        f"Signature={signature}"
    )

    request_payload = payload if method == "PUT" and payload else None
    return urllib.request.Request(url, data=request_payload, headers=headers, method=method)


def run_with_retries(operation_name: str, operation, retries: int = 3, base_delay: float = 1.0):
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return operation()
        except urllib.error.HTTPError as exc:
            exc.close()
            if exc.code in {400, 401, 403, 404, 409, 412}:
                raise
            last_error = exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            last_error = exc

        logging.warning("%s attempt %s/%s failed: %s", operation_name, attempt, retries, last_error)
        if attempt < retries:
            time.sleep(base_delay * (2 ** (attempt - 1)) + random.uniform(0.0, base_delay))

    raise RuntimeError(f"{operation_name} failed after {retries} attempts") from last_error


def download_object(config: R2Config, key: str, destination: Path, retries: int = 3) -> bool:
    def operation() -> bool:
        request = signed_request(config, "GET", key)
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                expected_hash = response.headers.get("x-amz-meta-sha256")
                hasher = hashlib.sha256()
                destination.parent.mkdir(parents=True, exist_ok=True)
                temp_destination = destination.with_name(destination.name + ".part")
                try:
                    with temp_destination.open("wb") as handle:
                        for chunk in iter(lambda: response.read(65536), b""):
                            hasher.update(chunk)
                            handle.write(chunk)
                    actual_hash = hasher.hexdigest()
                    if expected_hash and actual_hash != expected_hash:
                        raise RuntimeError(
                            f"Checksum mismatch for {key}: expected {expected_hash}, got {actual_hash}"
                        )
                    temp_destination.replace(destination)
                except Exception:
                    temp_destination.unlink(missing_ok=True)
                    raise
            logging.info("Downloaded r2://%s/%s to %s", config.bucket, config.object_key(key), destination)
            return True
        except urllib.error.HTTPError as exc:
            exc.close()
            if exc.code == 404:
                logging.info("R2 object does not exist yet: r2://%s/%s", config.bucket, config.object_key(key))
                return False
            raise

    return run_with_retries(f"Download {key}", operation, retries=retries)


def upload_object(config: R2Config, key: str, source: Path, retries: int = 3, extra_headers: dict[str, str] | None = None) -> None:
    payload = source.read_bytes()
    headers = dict(extra_headers or {})
    headers.setdefault("x-amz-meta-sha256", sha256_hex(payload))

    def operation() -> None:
        request = signed_request(config, "PUT", key, payload, extra_headers=headers)
        with urllib.request.urlopen(request, timeout=60) as response:
            response.read()
        logging.info("Uploaded %s to r2://%s/%s", source, config.bucket, config.object_key(key))

    run_with_retries(f"Upload {key}", operation, retries=retries)


def upload_bytes(
    config: R2Config,
    key: str,
    payload: bytes,
    extra_headers: dict[str, str] | None = None,
    retries: int = 3,
) -> None:
    def operation() -> None:
        request = signed_request(config, "PUT", key, payload, extra_headers=extra_headers)
        with urllib.request.urlopen(request, timeout=30) as response:
            response.read()

    run_with_retries(f"Upload {key}", operation, retries=retries)


def head_object(config: R2Config, key: str, retries: int = 3) -> bool:
    def operation() -> bool:
        request = signed_request(config, "HEAD", key)
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                response.read()
            return True
        except urllib.error.HTTPError as exc:
            exc.close()
            if exc.code == 404:
                return False
            raise

    return run_with_retries(f"Head {key}", operation, retries=retries)


def delete_object(config: R2Config, key: str, retries: int = 3) -> None:
    def operation() -> None:
        request = signed_request(config, "DELETE", key)
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                response.read()
        except urllib.error.HTTPError as exc:
            exc.close()
            if exc.code != 404:
                raise

    run_with_retries(f"Delete {key}", operation, retries=retries)


def copy_object(config: R2Config, source_key: str, destination_key: str, retries: int = 3) -> bool:
    encoded_source = urllib.parse.quote(f"/{config.bucket}/{config.object_key(source_key)}", safe="/")

    def operation() -> bool:
        request = signed_request(
            config,
            "PUT",
            destination_key,
            extra_headers={"x-amz-copy-source": encoded_source},
        )
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                response.read()
            logging.info(
                "Copied r2://%s/%s to r2://%s/%s",
                config.bucket,
                config.object_key(source_key),
                config.bucket,
                config.object_key(destination_key),
            )
            return True
        except urllib.error.HTTPError as exc:
            exc.close()
            if exc.code == 404:
                logging.info("R2 source object missing, skipping copy: r2://%s/%s", config.bucket, config.object_key(source_key))
                return False
            raise

    return run_with_retries(f"Copy {source_key} to {destination_key}", operation, retries=retries)


def verify_object_exists(config: R2Config, key: str) -> None:
    if not head_object(config, key):
        raise RuntimeError(f"R2 verification failed; object does not exist: {key}")


def rotate_r2_backups(config: R2Config, key: str) -> None:
    backup_1 = f"{key}.bak1"
    backup_2 = f"{key}.bak2"
    copy_object(config, backup_1, backup_2)
    copy_object(config, key, backup_1)


def file_sha256(path: Path) -> str:
    if not path.exists():
        return ""
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def atomic_upload_object(config: R2Config, key: str, source: Path, rotate_backups: bool = True) -> None:
    temp_key = f"{key}.tmp"
    upload_object(config, temp_key, source)
    verify_object_exists(config, temp_key)
    if rotate_backups and key.endswith(".db"):
        rotate_r2_backups(config, key)
    copy_object(config, temp_key, key)
    verify_object_exists(config, key)
    delete_object(config, temp_key)


LOCK_STALE_AFTER_SECONDS = 3600


def _read_lock_timestamp(config: R2Config, key: str) -> datetime | None:
    request = signed_request(config, "GET", key)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read(256).decode("utf-8", errors="replace").strip()
    except urllib.error.HTTPError as exc:
        exc.close()
        if exc.code == 404:
            return None
        raise
    except (urllib.error.URLError, TimeoutError, OSError):
        return None
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def acquire_r2_lock(config: R2Config, key: str = "lock/nav.lock", stale_after_seconds: int = LOCK_STALE_AFTER_SECONDS) -> None:
    payload = datetime.now(timezone.utc).isoformat().encode("utf-8")
    max_attempts = 2
    for attempt in range(1, max_attempts + 1):
        try:
            upload_bytes(config, key, payload, extra_headers={"if-none-match": "*"}, retries=1)
            logging.info("Acquired R2 lock r2://%s/%s", config.bucket, config.object_key(key))
            return
        except urllib.error.HTTPError as exc:
            exc.close()
            if exc.code not in {409, 412}:
                raise
            if attempt == max_attempts:
                break
        lock_time = _read_lock_timestamp(config, key)
        lock_age = (datetime.now(timezone.utc) - lock_time).total_seconds() if lock_time else None
        if lock_age is None or lock_age <= stale_after_seconds:
            break
        logging.warning(
            "Stale R2 lock detected: %s held for %.0fs (> %ss); taking over",
            key,
            lock_age,
            stale_after_seconds,
        )
        delete_object(config, key)
    raise RuntimeError(f"R2 lock already exists: {key}")


@contextmanager
def r2_lock(config: R2Config, key: str = "lock/nav.lock", stale_after_seconds: int = LOCK_STALE_AFTER_SECONDS):
    acquire_r2_lock(config, key, stale_after_seconds=stale_after_seconds)

    try:
        yield
    finally:
        delete_object(config, key)
        logging.info("Released R2 lock r2://%s/%s", config.bucket, config.object_key(key))
