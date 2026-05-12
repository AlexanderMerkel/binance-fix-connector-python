"""Tests for example helper behavior."""

import os
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "examples"))

from common import get_safe_limit_order_params, load_credentials


class MockResponse:
    def __init__(self, payload):
        self.payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    def raise_for_status(self):
        return None

    async def json(self):
        return self.payload


class MockClientSession:
    def __init__(self, payloads):
        self.payloads = payloads

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    def get(self, url, params=None):
        if "bookTicker" in url:
            return MockResponse(self.payloads["ticker"])
        if "avgPrice" in url:
            return MockResponse(self.payloads["avg_price"])
        if "exchangeInfo" in url:
            return MockResponse(self.payloads["exchange_info"])
        raise AssertionError(f"Unexpected URL: {url}")


def _mock_market_payloads():
    return {
        "ticker": {"bidPrice": "100.00", "askPrice": "101.00"},
        "avg_price": {"price": "100.00"},
        "exchange_info": {
            "symbols": [
                {
                    "filters": [
                        {"filterType": "PRICE_FILTER", "minPrice": "0.10", "maxPrice": "100000", "tickSize": "0.10"},
                        {"filterType": "LOT_SIZE", "minQty": "0.001", "maxQty": "10", "stepSize": "0.001"},
                        {"filterType": "MIN_NOTIONAL", "minNotional": "10"},
                        {
                            "filterType": "PERCENT_PRICE_BY_SIDE",
                            "bidMultiplierUp": "1.1",
                            "bidMultiplierDown": "0.9",
                            "askMultiplierUp": "1.2",
                            "askMultiplierDown": "0.9",
                        },
                    ]
                }
            ]
        },
    }


@patch("common.get_private_key")
def test_load_credentials_prefers_testnet_env_by_default(mock_get_private_key):
    mock_get_private_key.return_value = object()
    env = {
        "BINANCE_FIX_KEY": "mainnet_key",
        "BINANCE_FIX_PRIVATE_KEY_PATH": "/mainnet.pem",
        "BINANCE_TESTNET_FIX_KEY": "testnet_key",
        "BINANCE_TESTNET_FIX_PRIVATE_KEY_PATH": "/testnet.pem",
    }

    with patch.dict(os.environ, env, clear=True):
        api_key, _private_key = load_credentials()

    assert api_key == "testnet_key"
    mock_get_private_key.assert_called_once_with("/testnet.pem")


@patch("common.get_private_key")
def test_load_credentials_uses_mainnet_when_requested(mock_get_private_key):
    mock_get_private_key.return_value = object()
    env = {
        "USE_TESTNET": "false",
        "BINANCE_FIX_KEY": "mainnet_key",
        "BINANCE_FIX_PRIVATE_KEY_PATH": "/mainnet.pem",
        "BINANCE_TESTNET_FIX_KEY": "testnet_key",
        "BINANCE_TESTNET_FIX_PRIVATE_KEY_PATH": "/testnet.pem",
    }

    with patch.dict(os.environ, env, clear=True):
        api_key, _private_key = load_credentials()

    assert api_key == "mainnet_key"
    mock_get_private_key.assert_called_once_with("/mainnet.pem")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("side", "expected_price", "expected_quantity"),
    [
        ("1", "99.00", "0.102"),
        ("2", "102.10", "0.098"),
    ],
)
async def test_safe_limit_order_params_respect_filters(side, expected_price, expected_quantity):
    with patch("common.aiohttp.ClientSession", return_value=MockClientSession(_mock_market_payloads())):
        params = await get_safe_limit_order_params("BTCUSDT", side, target_quantity="0.01")

    assert params.price == expected_price
    assert params.quantity == expected_quantity
