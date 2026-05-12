"""Base test utilities for Binance FIX Connector tests."""

import unittest
from typing import Any
from unittest.mock import AsyncMock, MagicMock

from simplefix import FixMessage

from binance_fix_connector_async.fix_connector import BinanceFixConnector

from .conftest import load_test_key


class FixConnectorTestBase(unittest.IsolatedAsyncioTestCase):
    """Base class for all FIX connector unit tests."""

    def setUp(self):
        self.valid_endpoint = "tcp+tls://test.example.com:9000"
        self.valid_api_key = "test_api_key"
        self.valid_private_key = load_test_key()
        self.valid_sender_comp_id = "TEST"
        self.valid_target_comp_id = "SPOT"

        self.mock_reader = AsyncMock()
        self.mock_writer = MagicMock()
        self.mock_writer.drain = AsyncMock()
        self.mock_writer.close = MagicMock()
        self.mock_writer.wait_closed = AsyncMock()

        self.connector = self.create_connector()

    def create_connector(self, **kwargs) -> BinanceFixConnector:
        default_params = {
            "endpoint": self.valid_endpoint,
            "api_key": self.valid_api_key,
            "private_key": self.valid_private_key,
            "sender_comp_id": self.valid_sender_comp_id,
        }
        default_params.update(kwargs)
        return BinanceFixConnector(**default_params)

    def create_fix_message(self, msg_type: str = "D", pairs: dict[str, Any] | None = None) -> FixMessage:
        msg = FixMessage()
        msg.append_pair("8", "FIX.4.4")
        msg.append_pair("35", msg_type)

        if pairs:
            for tag, value in pairs.items():
                msg.append_pair(tag, value)

        return msg
