#!/usr/bin/env python3
"""
Tests for API permission checking functionality.

This module tests the check_fix_api_permissions and validate_fix_permissions_for_session
functions that help users verify their API keys have the correct FIX permissions.
"""

from contextlib import contextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from binance_fix_connector_async.utils import (
    check_fix_api_permissions,
    validate_fix_permissions_for_session,
)


@contextmanager
def mock_aiohttp_response(*, status=200, json_data=None, text_data=None):
    with patch("aiohttp.ClientSession") as mock_session_class:
        mock_session = MagicMock()
        mock_session_class.return_value.__aenter__.return_value = mock_session

        mock_resp = MagicMock()
        mock_resp.status = status
        if json_data is not None:
            mock_resp.json = AsyncMock(return_value=json_data)
        if text_data is not None:
            mock_resp.text = AsyncMock(return_value=text_data)
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=None)

        mock_session.get.return_value = mock_resp
        yield mock_session


FULL_PERMISSIONS_RESPONSE = {
    "ipRestrict": False,
    "createTime": 1698645219000,
    "enableReading": True,
    "enableSpotAndMarginTrading": True,
    "enableWithdrawals": False,
    "enableInternalTransfer": True,
    "enableMargin": False,
    "enableFutures": False,
    "enableFixApiTrade": True,
    "enableFixReadOnly": True,
}

CHECK_ARGS = ("test_api_key", "test_hmac_secret", "https://api.binance.com")


class TestApiPermissionChecking:
    """Test API permission checking functionality."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("response_overrides", "expected"),
        [
            pytest.param(
                {},
                {"has_fix_api": True, "has_fix_api_read_only": True, "can_use_drop_copy": True},
                id="success",
            ),
            pytest.param(
                {"enableFixApiTrade": False, "enableFixReadOnly": False},
                {"has_fix_api": False, "has_fix_api_read_only": False, "can_use_drop_copy": False},
                id="no_fix_permissions",
            ),
            pytest.param(
                {"enableSpotAndMarginTrading": False, "enableFixApiTrade": False, "enableFixReadOnly": True},
                {"has_fix_api": False, "has_fix_api_read_only": True, "can_use_drop_copy": True},
                id="read_only",
            ),
        ],
    )
    async def test_check_fix_api_permissions(self, response_overrides, expected):
        response = {**FULL_PERMISSIONS_RESPONSE, **response_overrides}
        with mock_aiohttp_response(json_data=response):
            result = await check_fix_api_permissions(*CHECK_ARGS)
            for key, value in expected.items():
                assert result[key] is value

    @pytest.mark.asyncio
    async def test_check_fix_api_permissions_api_error(self):
        error_text = '{"code":-2015,"msg":"Invalid API-key, IP, or permissions for action."}'
        with mock_aiohttp_response(status=401, text_data=error_text):
            with pytest.raises(aiohttp.ClientError) as exc_info:
                await check_fix_api_permissions("invalid_api_key", "invalid_secret", "https://api.binance.com")

            assert "401" in str(exc_info.value)
            assert "Invalid API-key" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_check_fix_api_permissions_missing_fields(self):
        response = {
            "ipRestrict": False,
            "createTime": 1698645219000,
            "enableReading": True,
            "enableSpotAndMarginTrading": True,
        }
        with mock_aiohttp_response(json_data=response):
            result = await check_fix_api_permissions(*CHECK_ARGS)

            assert result["has_fix_api"] is False
            assert result["has_fix_api_read_only"] is False
            assert result["can_use_drop_copy"] is False


class TestPermissionValidation:
    """Test permission validation for different session types."""

    @pytest.mark.parametrize(
        ("session_type", "permissions", "expected_valid", "expected_error_substr"),
        [
            (
                "order_entry",
                {"has_fix_api": True, "has_fix_api_read_only": False, "can_use_drop_copy": True},
                True,
                None,
            ),
            (
                "order_entry",
                {"has_fix_api": False, "has_fix_api_read_only": True, "can_use_drop_copy": True},
                False,
                "Order Entry sessions require FIX_API permission",
            ),
            ("drop_copy", {"has_fix_api": True, "has_fix_api_read_only": False, "can_use_drop_copy": True}, True, None),
            ("drop_copy", {"has_fix_api": False, "has_fix_api_read_only": True, "can_use_drop_copy": True}, True, None),
            (
                "drop_copy",
                {"has_fix_api": False, "has_fix_api_read_only": False, "can_use_drop_copy": False},
                False,
                "Drop Copy sessions require either FIX_API or FIX_API_READ_ONLY",
            ),
            (
                "market_data",
                {"has_fix_api": True, "has_fix_api_read_only": False, "can_use_drop_copy": True},
                True,
                None,
            ),
            (
                "market_data",
                {"has_fix_api": False, "has_fix_api_read_only": True, "can_use_drop_copy": True},
                True,
                None,
            ),
            (
                "market_data",
                {"has_fix_api": False, "has_fix_api_read_only": False, "can_use_drop_copy": False},
                False,
                "Market Data sessions require either FIX_API or FIX_API_READ_ONLY",
            ),
            (
                "unknown_type",
                {"has_fix_api": True, "has_fix_api_read_only": True, "can_use_drop_copy": True},
                False,
                "Unknown session type: unknown_type",
            ),
        ],
    )
    def test_validate_permissions(self, session_type, permissions, expected_valid, expected_error_substr):
        is_valid, error_msg = validate_fix_permissions_for_session(permissions, session_type)
        assert is_valid is expected_valid
        if expected_error_substr:
            assert expected_error_substr in error_msg
        else:
            assert error_msg is None


if __name__ == "__main__":
    pytest.main([__file__])
