from __future__ import annotations

import hashlib
import hmac
import logging
import os
import urllib.error
import urllib.parse
import urllib.request
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


def signed_request(config: R2Config, method: str, key: str, payload: bytes = b"") -> urllib.request.Request:
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

    return urllib.request.Request(url, data=payload if method != "GET" else None, headers=headers, method=method)


def download_object(config: R2Config, key: str, destination: Path) -> bool:
    request = signed_request(config, "GET", key)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(response.read())
        logging.info("Downloaded r2://%s/%s to %s", config.bucket, config.object_key(key), destination)
        return True
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            logging.info("R2 object does not exist yet: r2://%s/%s", config.bucket, config.object_key(key))
            return False
        raise


def upload_object(config: R2Config, key: str, source: Path) -> None:
    payload = source.read_bytes()
    request = signed_request(config, "PUT", key, payload)
    with urllib.request.urlopen(request, timeout=60) as response:
        response.read()
    logging.info("Uploaded %s to r2://%s/%s", source, config.bucket, config.object_key(key))
