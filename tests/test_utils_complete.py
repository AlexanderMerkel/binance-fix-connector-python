"""
Comprehensive tests for utils module to achieve 100% coverage.

Tests all error paths and edge cases following KISS principles.
"""

import os
import tempfile
import unittest

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519

from binance_fix_connector_async.utils import get_api_key, get_private_key
from tests.conftest import TEST_KEY_PATH
from tests.test_base import FixConnectorTestBase


class TestUtilsComprehensive(FixConnectorTestBase):
    """
    Complete test coverage for utils module.

    Tests all branches including error conditions.
    """

    def test_get_private_key_valid_path(self):
        """Test get_private_key with shared fixture key."""
        if not TEST_KEY_PATH.exists():
            self.skipTest(f"Test fixture key not found at {TEST_KEY_PATH}")
        private_key = get_private_key(str(TEST_KEY_PATH))
        self.assertIsNotNone(private_key)

    def test_get_private_key_valid_openssh_path(self):
        """Test get_private_key with OpenSSH private key format."""
        key = ed25519.Ed25519PrivateKey.generate()
        key_bytes = key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.OpenSSH,
            encryption_algorithm=serialization.NoEncryption(),
        )

        with tempfile.NamedTemporaryFile(mode="wb", suffix=".pem", delete=False) as f:
            f.write(key_bytes)
            temp_path = f.name

        try:
            private_key = get_private_key(temp_path)
            self.assertIsInstance(private_key, ed25519.Ed25519PrivateKey)
        finally:
            os.unlink(temp_path)

    def test_get_private_key_empty_path(self):
        """Test get_private_key raises ValueError for empty path (line 28)."""
        with self.assertRaises(ValueError) as context:
            get_private_key("")

        self.assertEqual(str(context.exception), "Private key path is required")

    def test_get_private_key_none_path(self):
        """Test get_private_key raises ValueError for None path (line 28)."""
        with self.assertRaises(ValueError) as context:
            get_private_key(None)

        self.assertEqual(str(context.exception), "Private key path is required")

    def test_get_api_key_valid_config(self):
        """Test get_api_key with valid configuration file."""
        config_content = """[keys]
API_KEY = x
PATH_TO_PRIVATE_KEY_PEM_FILE = /path/to/key.pem
"""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".ini", delete=False) as f:
            f.write(config_content)
            temp_path = f.name

        try:
            api_key, key_path = get_api_key(temp_path)
            self.assertEqual(api_key, "x")
            self.assertEqual(key_path, "/path/to/key.pem")
        finally:
            os.unlink(temp_path)

    def test_get_api_key_valid_flat_json_config(self):
        """Test get_api_key with flat JSON configuration."""
        config_content = """{
  "BINANCE_FIX_KEY": "json_api_key",
  "BINANCE_FIX_PRIVATE_KEY_PATH": "~/json_key.pem"
}
"""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write(config_content)
            temp_path = f.name

        try:
            api_key, key_path = get_api_key(temp_path)
            self.assertEqual(api_key, "json_api_key")
            self.assertEqual(key_path, os.path.expanduser("~/json_key.pem"))
        finally:
            os.unlink(temp_path)

    def test_get_api_key_valid_environment_json_config(self):
        """Test get_api_key with environment-section JSON configuration."""
        config_content = """{
  "environment": "testnet",
  "testnet": {
    "BINANCE_FIX_KEY": "testnet_api_key",
    "BINANCE_FIX_PRIVATE_KEY_PATH": "/path/to/testnet.pem"
  }
}
"""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write(config_content)
            temp_path = f.name

        try:
            api_key, key_path = get_api_key(temp_path)
            self.assertEqual(api_key, "testnet_api_key")
            self.assertEqual(key_path, "/path/to/testnet.pem")
        finally:
            os.unlink(temp_path)

    def test_get_api_key_empty_path(self):
        """Test get_api_key raises ValueError for empty path (line 47-48)."""
        with self.assertRaises(ValueError) as context:
            get_api_key("")

        self.assertEqual(str(context.exception), "Config path is required")

    def test_get_api_key_none_path(self):
        """Test get_api_key raises ValueError for None path (line 47-48)."""
        with self.assertRaises(ValueError) as context:
            get_api_key(None)

        self.assertEqual(str(context.exception), "Config path is required")

    def test_get_api_key_missing_file(self):
        """Test get_api_key with missing configuration file (line 49-51)."""
        # This tests the ConfigParser.read() path when file doesn't exist
        non_existent_path = "/tmp/non_existent_config_file_12345.ini"

        # ConfigParser.read() silently handles missing files, so we test KeyError
        with self.assertRaises(KeyError):
            get_api_key(non_existent_path)

    def test_get_api_key_malformed_config(self):
        """Test get_api_key with malformed configuration file (line 49-51)."""
        malformed_content = """[invalid_section]
WRONG_KEY = wrong_value
"""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".ini", delete=False) as f:
            f.write(malformed_content)
            temp_path = f.name

        try:
            with self.assertRaises(KeyError):
                get_api_key(temp_path)
        finally:
            os.unlink(temp_path)

    def test_get_api_key_missing_keys_section(self):
        """Test get_api_key with config missing 'keys' section (line 49-51)."""
        config_content = """[other_section]
some_key = some_value
"""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".ini", delete=False) as f:
            f.write(config_content)
            temp_path = f.name

        try:
            with self.assertRaises(KeyError):
                get_api_key(temp_path)
        finally:
            os.unlink(temp_path)

    def test_get_api_key_missing_api_key(self):
        """Test get_api_key with config missing API_KEY (line 49-51)."""
        config_content = """[keys]
PATH_TO_PRIVATE_KEY_PEM_FILE = /path/to/key.pem
"""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".ini", delete=False) as f:
            f.write(config_content)
            temp_path = f.name

        try:
            with self.assertRaises(KeyError):
                get_api_key(temp_path)
        finally:
            os.unlink(temp_path)

    def test_get_private_key_file_permission_error(self):
        """Test get_private_key handles file permission errors."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".pem", delete=False) as f:
            f.write("test content")
            temp_path = f.name

        try:
            # Remove read permissions
            os.chmod(temp_path, 0o000)

            with self.assertRaises(PermissionError):
                get_private_key(temp_path)
        finally:
            # Restore permissions for cleanup
            os.chmod(temp_path, 0o644)
            os.unlink(temp_path)

    def test_get_private_key_invalid_pem_content(self):
        """Test get_private_key with invalid PEM content."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".pem", delete=False) as f:
            f.write("Invalid PEM content")
            temp_path = f.name

        try:
            with self.assertRaises(ValueError):
                get_private_key(temp_path)
        finally:
            os.unlink(temp_path)


if __name__ == "__main__":
    unittest.main()
