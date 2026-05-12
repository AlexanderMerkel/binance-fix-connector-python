"""Shared test configuration and fixtures."""

from pathlib import Path

import pytest
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives.serialization import load_pem_private_key

TEST_KEY_PATH = Path("~/.config/test-fixtures/ed25519_test_key.pem").expanduser()


def load_test_key() -> ed25519.Ed25519PrivateKey:
    """Load the shared Ed25519 test key, or generate one if the fixture is missing."""
    if TEST_KEY_PATH.exists():
        key = load_pem_private_key(TEST_KEY_PATH.read_bytes(), password=None)
        if isinstance(key, ed25519.Ed25519PrivateKey):
            return key
    return ed25519.Ed25519PrivateKey.generate()


@pytest.fixture
def ed25519_test_key() -> ed25519.Ed25519PrivateKey:
    return load_test_key()


@pytest.fixture
def test_key_path() -> Path:
    return TEST_KEY_PATH
