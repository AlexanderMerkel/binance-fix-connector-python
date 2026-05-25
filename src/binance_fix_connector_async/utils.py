"""
Utility functions for the async Binance FIX Connector.

Provides key and configuration loading helpers for the async connector.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import time
from configparser import ConfigParser
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any

import aiohttp
from cryptography.hazmat.primitives.serialization import load_pem_private_key, load_ssh_private_key

if TYPE_CHECKING:
    from cryptography.hazmat.primitives.asymmetric.types import PrivateKeyTypes


class SessionType(StrEnum):
    MARKET_DATA = "market_data"
    ORDER_ENTRY = "order_entry"
    DROP_COPY = "drop_copy"


def get_private_key(key_path: str) -> PrivateKeyTypes:
    """
    Load a private key from a PEM or OpenSSH private key file.

    Args:
        key_path (str): Path to the PEM file containing the private key

    Returns:
        Ed25519PrivateKey: The loaded private key

    Raises:
        ValueError: If key_path is empty or None
    """
    if not key_path:
        raise ValueError("Private key path is required")
    with Path(key_path).open("rb") as f:
        private_key_from_file = f.read()
    try:
        return load_pem_private_key(private_key_from_file, password=None)
    except ValueError as pem_error:
        try:
            return load_ssh_private_key(private_key_from_file, password=None)
        except ValueError as ssh_error:
            raise pem_error from ssh_error


def _expand_path(path: str) -> str:
    path_str = str(path)
    return str(Path(path_str).expanduser()) if path_str.startswith("~") else path_str


def get_api_key(config_path: str, environment: str | None = None) -> tuple[str, str]:
    """
    Load API key and private key path from a configuration file.

    Args:
        config_path (str): Path to the configuration file
        environment (str | None): JSON environment section to load. Defaults to the file's "environment" value.

    Returns:
        tuple[str, str]: A tuple containing (API_KEY, PATH_TO_PRIVATE_KEY_PEM_FILE)

    Raises:
        ValueError: If config_path is empty or None
    """
    if not config_path:
        raise ValueError("Config path is required")
    path = Path(config_path)
    if path.suffix.lower() == ".json":
        with path.open() as f:
            config = json.load(f)
        if "BINANCE_FIX_KEY" in config:
            return config["BINANCE_FIX_KEY"], _expand_path(config["BINANCE_FIX_PRIVATE_KEY_PATH"])
        env_name = environment or config.get("environment", "testnet")
        env_config = config[env_name]
        return env_config["BINANCE_FIX_KEY"], _expand_path(env_config["BINANCE_FIX_PRIVATE_KEY_PATH"])

    config = ConfigParser()
    config.read(config_path)
    return config["keys"]["API_KEY"], _expand_path(config["keys"]["PATH_TO_PRIVATE_KEY_PEM_FILE"])


async def check_fix_api_permissions(
    api_key: str,
    hmac_secret: str,
    base_url: str = "https://api.binance.com",
) -> dict[str, Any]:
    """
    Check whether a HMAC REST API key reports FIX API permissions.

    This function calls Binance's mainnet /sapi/v1/account/apiRestrictions endpoint.
    It is only a REST-side helper for HMAC credentials; Ed25519 FIX key
    compatibility is validated by an actual FIX logon.

    Args:
        api_key: The Binance REST API key to check
        hmac_secret: The API secret for HMAC signature (for REST API calls)
        base_url: The Binance REST API base URL (default: production)

    Returns:
        Dict containing:
            - has_fix_api: Whether FIX API trading permission is enabled
            - has_fix_api_read_only: Whether FIX API read-only permission is enabled
            - can_use_drop_copy: Whether the key can use drop copy (either permission)
            - raw_response: The full API response for debugging

    Raises:
        aiohttp.ClientError: If the API request fails
        ValueError: If the response is invalid
    """
    endpoint = "/sapi/v1/account/apiRestrictions"
    timestamp = int(time.time() * 1000)

    query_string = f"timestamp={timestamp}"

    signature = hmac.new(
        hmac_secret.encode("utf-8"),
        query_string.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    url = f"{base_url}{endpoint}?{query_string}&signature={signature}"

    headers = {"X-MBX-APIKEY": api_key}

    async with aiohttp.ClientSession() as session, session.get(url, headers=headers) as response:
        if response.status == 401:
            error_text = await response.text()
            raise aiohttp.ClientError(f"Authentication failed (401): invalid API key or signature: {error_text}")
        if response.status == 403:
            error_text = await response.text()
            raise aiohttp.ClientError(f"Permission denied (403): API key lacks required permissions: {error_text}")
        if response.status != 200:
            error_text = await response.text()
            raise aiohttp.ClientError(f"API request failed with status {response.status}: {error_text}")

        data = await response.json()

        has_fix_api = data.get("enableFixApiTrade", False)
        has_fix_api_read_only = data.get("enableFixReadOnly", False)
        can_use_drop_copy = has_fix_api or has_fix_api_read_only

        return {
            "has_fix_api": has_fix_api,
            "has_fix_api_read_only": has_fix_api_read_only,
            "can_use_drop_copy": can_use_drop_copy,
            "raw_response": data,
        }


def validate_fix_permissions_for_session(
    permissions: dict[str, bool],
    session_type: str | SessionType,
) -> tuple[bool, str | None]:
    """
    Validate if the given permissions are sufficient for a specific FIX session type.

    Args:
        permissions: Dict returned by check_fix_api_permissions()
        session_type: One of SessionType values or "market_data", "order_entry", "drop_copy"

    Returns:
        Tuple of (is_valid, error_message)
        - is_valid: True if permissions are sufficient
        - error_message: None if valid, otherwise describes the missing permission
    """
    try:
        st = SessionType(str(session_type))
    except ValueError:
        return False, f"Unknown session type: {session_type}"

    if st is SessionType.ORDER_ENTRY:
        if not permissions["has_fix_api"]:
            return False, "Order Entry sessions require FIX_API permission"

    elif st is SessionType.DROP_COPY:
        if not permissions["can_use_drop_copy"]:
            return False, "Drop Copy sessions require either FIX_API or FIX_API_READ_ONLY permission"

    elif st is SessionType.MARKET_DATA and not (permissions["has_fix_api"] or permissions["has_fix_api_read_only"]):
        return False, "Market Data sessions require either FIX_API or FIX_API_READ_ONLY permission"

    return True, None
