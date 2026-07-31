"""PBKDF2 password helpers for the environment-configured dashboard login."""

from __future__ import annotations

import base64
import hashlib
import hmac
import secrets
import sys

_ALGORITHM = "pbkdf2_sha256"
_ITERATIONS = 600_000


def hash_password(password: str) -> str:
    """Create a portable PBKDF2 hash suitable for ADMIN_PASSWORD_HASH."""
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, _ITERATIONS)
    return "$".join(
        (
            _ALGORITHM,
            str(_ITERATIONS),
            base64.urlsafe_b64encode(salt).decode(),
            base64.urlsafe_b64encode(digest).decode(),
        )
    )


def verify_password(password: str, encoded: str) -> bool:
    """Compare a password against a supported PBKDF2 hash in constant time."""
    try:
        algorithm, raw_iterations, encoded_salt, encoded_digest = encoded.split("$", 3)
        if algorithm != _ALGORITHM:
            return False
        salt = base64.urlsafe_b64decode(encoded_salt.encode())
        expected = base64.urlsafe_b64decode(encoded_digest.encode())
        actual = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, int(raw_iterations))
    except (TypeError, ValueError):
        return False
    return hmac.compare_digest(actual, expected)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python -m src.admin.passwords '<password>'")
    print(hash_password(sys.argv[1]))
