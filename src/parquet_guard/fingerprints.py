"""Project-scoped fingerprints for redacted contract evidence."""

from __future__ import annotations

from hashlib import sha256
import hmac


FINGERPRINT_PREFIX = b"agentsor-file-contracts/v1\x00"
MINIMUM_KEY_BYTES = 32


class FingerprintKeyError(ValueError):
    """Raised when a project fingerprint key is too weak or ambiguous."""


def validate_fingerprint_key(key: bytes) -> bytes:
    """Return a defensive key copy after enforcing a 256-bit minimum."""

    if not isinstance(key, bytes) or len(key) < MINIMUM_KEY_BYTES:
        raise FingerprintKeyError("fingerprint keys must contain at least 32 bytes")
    return bytes(key)


def new_project_hmac(key: bytes, domain: str) -> hmac.HMAC:
    """Create a domain-separated HMAC-SHA256 state for streamed input."""

    checked_key = validate_fingerprint_key(key)
    if (
        not isinstance(domain, str)
        or not domain
        or not domain.isascii()
        or "\x00" in domain
    ):
        raise ValueError("fingerprint domains must be non-empty ASCII")
    value = hmac.new(checked_key, digestmod=sha256)
    value.update(FINGERPRINT_PREFIX)
    value.update(domain.encode("ascii"))
    value.update(b"\x00")
    return value


def project_fingerprint(key: bytes, domain: str, payload: bytes) -> str:
    """Return a project-keyed, domain-separated HMAC-SHA256 fingerprint."""

    if not isinstance(payload, bytes):
        raise TypeError("fingerprint payloads must be bytes")
    value = new_project_hmac(key, domain)
    value.update(payload)
    return value.hexdigest()
