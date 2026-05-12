#!/usr/bin/env python3
"""
End-to-End Test Framework for Binance FIX Connector (Async)

This module provides the foundation for comprehensive E2E testing of the async
Binance FIX Connector, including base classes, utilities, and test fixtures
for testing against real testnet endpoints.
"""

import asyncio
import logging
import os
import time
import unittest
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Optional

import pytest
from cryptography.hazmat.primitives.asymmetric import ed25519
from simplefix import FixMessage

from binance_fix_connector_async import (
    create_drop_copy_session,
    create_market_data_session,
    create_order_entry_session,
)
from binance_fix_connector_async.fix_connector import BinanceFixConnector, FixMsgTypes, FixTags
from binance_fix_connector_async.utils import get_api_key, get_private_key

# Test configuration
TESTNET_ENDPOINTS = {
    "market_data": "tcp+tls://fix-md.testnet.binance.vision:9000",
    "order_entry": "tcp+tls://fix-oe.testnet.binance.vision:9000",
    "drop_copy": "tcp+tls://fix-dc.testnet.binance.vision:9000",
}

# Test timeouts and limits
DEFAULT_TIMEOUT = 30
MESSAGE_TIMEOUT = 5
CONCURRENT_SESSION_LIMIT = 5

logger = logging.getLogger(__name__)


def test_testnet_dry_run_does_not_require_credentials(monkeypatch):
    for env_name in (
        "BINANCE_TESTNET_FIX_KEY",
        "BINANCE_TESTNET_API_KEY",
        "BINANCE_TESTNET_FIX_PRIVATE_KEY_PATH",
        "BINANCE_TESTNET_PRIVATE_KEY_PATH",
    ):
        monkeypatch.delenv(env_name, raising=False)

    from tests.run_e2e_tests import E2ETestRunner

    assert E2ETestRunner().dry_run_testnet_tests() == 0


def test_e2e_runner_mocked_alias_is_not_named_unit():
    from tests.run_e2e_tests import E2ETestRunner

    runner = E2ETestRunner()
    args = runner.get_pytest_args(markers=["mocked"], parallel=False, verbose=False, capture="")

    assert "unit" not in runner.test_markers
    assert runner.test_markers["mocked"] == "not requires_testnet and not load_test and not error_scenario"
    assert "-m" in args
    assert args[args.index("-m") + 1] == runner.test_markers["mocked"]


class E2ECredentials:
    """Manages test credentials and configuration."""

    def __init__(self, config_path: Optional[str] = None):
        """Initialize with config file path."""
        self.config_path = config_path or self._find_config_file()
        self.api_key: Optional[str] = None
        self.private_key: Optional[ed25519.Ed25519PrivateKey] = None
        self._load_credentials()

    def _find_config_file(self) -> str:
        """Find configuration file in standard locations."""
        possible_paths = [
            Path("config.json"),  # New JSON config format
            Path("examples/config.ini"),
            Path("config.ini"),
        ]

        for path in possible_paths:
            if path.exists():
                return str(path)

        # Return example file as fallback (will use mock credentials)
        return "examples/config.ini.example"

    def _load_credentials(self) -> None:
        """Load credentials from config file."""
        try:
            env_key = os.getenv("BINANCE_TESTNET_FIX_KEY") or os.getenv("BINANCE_TESTNET_API_KEY")
            env_pem = os.getenv("BINANCE_TESTNET_FIX_PRIVATE_KEY_PATH") or os.getenv("BINANCE_TESTNET_PRIVATE_KEY_PATH")
            if env_key and env_pem:
                self.api_key = env_key
                self.private_key = get_private_key(env_pem)
                logger.info("Loaded testnet credentials from environment")
                return

            config_path = Path(self.config_path)
            if config_path.exists() and "example" not in self.config_path:
                self.api_key, private_key_path = get_api_key(str(config_path))
                self.private_key = get_private_key(private_key_path)
                logger.info("Loaded credentials from %s", config_path)
            else:
                # Use mock credentials for unit testing
                self._create_mock_credentials()
                logger.info("Using mock credentials for testing")
        except Exception as e:
            logger.warning("Failed to load credentials: %s, using mock credentials", e)
            self._create_mock_credentials()

    def _create_mock_credentials(self) -> None:
        """Create mock credentials for testing."""
        self.api_key = "test_api_key_for_e2e_testing"
        self.private_key = ed25519.Ed25519PrivateKey.generate()

    @property
    def has_real_credentials(self) -> bool:
        """Check if real testnet credentials are available."""
        return self.api_key and "test_api_key" not in self.api_key and self.private_key is not None


class SessionMetrics:
    """Tracks performance and behavioral metrics for sessions."""

    def __init__(self):
        """Initialize metrics tracking."""
        self.reset()

    def reset(self) -> None:
        """Reset all metrics."""
        self.connection_time: Optional[float] = None
        self.logon_time: Optional[float] = None
        self.logout_time: Optional[float] = None
        self.messages_sent: int = 0
        self.messages_received: int = 0
        self.errors: list[str] = []
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None

    def start_timing(self) -> None:
        """Start timing session."""
        self.start_time = time.time()

    def end_timing(self) -> None:
        """End timing session."""
        self.end_time = time.time()

    @property
    def total_duration(self) -> Optional[float]:
        """Get total session duration."""
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return None


class E2ETestSession:
    """Managed test session with automatic cleanup and monitoring."""

    def __init__(
        self,
        session_type: str,
        credentials: E2ECredentials,
        endpoint_override: Optional[str] = None,
        use_real_testnet: bool = False,
    ):
        """Initialize E2E test session."""
        self.session_type = session_type
        self.credentials = credentials
        self.endpoint_override = endpoint_override
        self.use_real_testnet = use_real_testnet
        self.session: Optional[BinanceFixConnector] = None
        self.metrics = SessionMetrics()
        self._connected = False
        self._logged_in = False

    async def __aenter__(self) -> tuple[BinanceFixConnector, SessionMetrics]:
        """Async context manager entry."""
        await self.connect()
        return self.session, self.metrics

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit with cleanup."""
        await self.cleanup()

    async def connect(self) -> None:
        """Connect and initialize session."""
        self.metrics.start_timing()
        endpoint = self._resolve_endpoint()

        if self.use_real_testnet and self.credentials.has_real_credentials:
            await self._connect_real(endpoint)
        else:
            await self._connect_mocked(endpoint)

        logger.info("Created %s session (testnet=%s)", self.session_type, self.use_real_testnet)

    def _resolve_endpoint(self) -> str:
        """Determine the endpoint URL based on configuration."""
        if self.endpoint_override:
            return self.endpoint_override
        if self.use_real_testnet and self.credentials.has_real_credentials:
            endpoint = TESTNET_ENDPOINTS[self.session_type]
            logger.info("Using testnet endpoint for %s: %s", self.session_type, endpoint)
            return endpoint
        return f"tcp+tls://mock-{self.session_type}.example.com:9000"

    async def _connect_mocked(self, endpoint: str) -> None:
        """Set up a mocked connection for unit testing."""
        from unittest.mock import AsyncMock, MagicMock, patch

        start_time = time.time()
        mock_reader = AsyncMock()
        mock_writer = AsyncMock()

        mock_writer.write = MagicMock()
        mock_writer.close = MagicMock()
        mock_writer.drain = AsyncMock()
        mock_writer.wait_closed = AsyncMock()

        async def _blocking_read(n=0):
            await asyncio.Future()

        mock_reader.read = _blocking_read

        async def _mock_logon(connector, recv_window=None):
            logon_msg = FixMessage()
            logon_msg.append_pair(FixTags.MSG_TYPE, FixMsgTypes.LOGON)
            await connector.on_message_received([logon_msg])

        with (
            patch(
                "binance_fix_connector_async.fix_connector.asyncio.open_connection",
                return_value=(mock_reader, mock_writer),
            ),
            patch.object(BinanceFixConnector, "logon", new=_mock_logon),
        ):
            self.session = await self._create_session_for_type(endpoint)

        self.session._reader = mock_reader
        self.session._writer = mock_writer
        self._connected = True
        self._logged_in = False

        self.metrics.messages_sent = 0
        self.metrics.connection_time = time.time() - start_time
        self._mock_reader = mock_reader

        original_send = self.session.send_message
        metrics_ref = self.metrics

        async def _tracked_send(message, *, raw=False):
            await original_send(message, raw=raw)
            metrics_ref.messages_sent += 1

        self.session.send_message = _tracked_send

    async def _connect_real(self, endpoint: str) -> None:
        """Establish a real testnet connection."""
        start_time = time.time()
        try:
            self.session = await self._create_session_for_type(endpoint)
            self._connected = True
            self._logged_in = True
            self.metrics.connection_time = time.time() - start_time
            self.metrics.messages_sent = len(self.session.messages_sent)
            self.metrics.messages_received = len(self.session._message_history)
        except Exception as e:
            self.metrics.errors.append(f"Connection failed: {e}")
            raise

    async def _create_session_for_type(self, endpoint: str):
        """Create a FIX session based on session_type."""
        if self.session_type == "market_data":
            return await create_market_data_session(
                api_key=self.credentials.api_key,
                private_key=self.credentials.private_key,
                endpoint=endpoint,
            )
        if self.session_type == "order_entry":
            return await create_order_entry_session(
                api_key=self.credentials.api_key,
                private_key=self.credentials.private_key,
                endpoint=endpoint,
            )
        if self.session_type == "drop_copy":
            return await create_drop_copy_session(
                api_key=self.credentials.api_key,
                private_key=self.credentials.private_key,
                endpoint=endpoint,
            )
        raise ValueError(f"Unknown session type: {self.session_type}")

    async def logon(self, recv_window: str = "5000") -> list[FixMessage]:
        """Perform logon and return response messages."""
        if not self.session:
            raise RuntimeError("Session not connected")

        start_time = time.time()

        try:
            await self.session.logon(recv_window=recv_window)
            self.metrics.messages_sent += 1

            # Wait for logon response if using real testnet
            if self.use_real_testnet and self.credentials.has_real_credentials:
                messages = await self.session.retrieve_messages_until(
                    FixMsgTypes.LOGON, timeout_seconds=DEFAULT_TIMEOUT
                )
                self.metrics.messages_received += len(messages)
                self._logged_in = True
                self.metrics.logon_time = time.time() - start_time
                return messages
            # Mock response for unit testing - simulate successful logon
            self._logged_in = True
            self.metrics.logon_time = time.time() - start_time
            return []

        except Exception as e:
            self.metrics.errors.append(f"Logon failed: {e}")
            raise

    async def logout(self, text: str = "E2E test logout") -> list[FixMessage]:
        """Perform logout and return response messages."""
        if not self.session:
            return []

        start_time = time.time()

        try:
            await self.session.logout(text=text)
            self.metrics.messages_sent += 1

            # Wait for logout response if using real testnet
            if self.use_real_testnet and self.credentials.has_real_credentials:
                messages = await self.session.retrieve_messages_until(
                    FixMsgTypes.LOGOUT, timeout_seconds=DEFAULT_TIMEOUT
                )
                self.metrics.messages_received += len(messages)
                self._logged_in = False
                self.metrics.logout_time = time.time() - start_time
                return messages
            # Mock response for unit testing - simulate successful logout
            self._logged_in = False
            self.metrics.logout_time = time.time() - start_time
            return []

        except Exception as e:
            self.metrics.errors.append(f"Logout failed: {e}")
            logger.warning("Logout error: %s", e)
            return []

    async def cleanup(self) -> None:
        """Clean up session resources."""
        try:
            if self._logged_in:
                await self.logout()

            if self.session and self._connected:
                await self.session.disconnect()
                self._connected = False

            self.metrics.end_timing()

        except Exception as e:
            logger.warning("Cleanup error: %s", e)
            self.metrics.errors.append(f"Cleanup failed: {e}")


class BaseE2ETest(unittest.IsolatedAsyncioTestCase):
    """Base class for all E2E tests with common setup and utilities."""

    @classmethod
    def setUpClass(cls) -> None:
        """Set up class-level test fixtures."""
        cls.credentials = E2ECredentials()
        cls.session_metrics: dict[str, SessionMetrics] = {}

    def setUp(self) -> None:
        """Set up individual test."""
        self.test_start_time = time.time()
        self.test_errors: list[str] = []

    def tearDown(self) -> None:
        """Tear down individual test."""
        test_duration = time.time() - self.test_start_time
        logger.info("Test %s completed in %.2fs", self._testMethodName, test_duration)

        if self.test_errors:
            logger.warning("Test errors: %s", self.test_errors)

    @asynccontextmanager
    async def create_test_session(
        self,
        session_type: str,
        use_real_testnet: bool = False,
        endpoint_override: Optional[str] = None,
    ):
        """Create managed test session with automatic cleanup."""
        session_wrapper = E2ETestSession(
            session_type=session_type,
            credentials=self.credentials,
            endpoint_override=endpoint_override,
            use_real_testnet=use_real_testnet,
        )

        async with session_wrapper as (session, metrics):
            # Store metrics for analysis
            self.session_metrics[f"{session_type}_{int(time.time())}"] = metrics
            yield session, metrics

    async def wait_for_messages(
        self,
        session: BinanceFixConnector,
        expected_types: list[str],
        timeout: float = MESSAGE_TIMEOUT,
    ) -> list[FixMessage]:
        """Wait for specific message types with timeout."""
        start_time = time.time()
        received_messages = []

        while time.time() - start_time < timeout:
            messages = await session.get_all_new_messages_received()
            received_messages.extend(messages)

            # Check if we have all expected types
            received_types = {msg.get(FixTags.MSG_TYPE).decode() for msg in received_messages}
            if all(msg_type in received_types for msg_type in expected_types):
                return received_messages

            await asyncio.sleep(0.1)  # Small delay to prevent busy waiting

        raise TimeoutError(f"Timeout waiting for messages: {expected_types}")

    def assert_message_structure(
        self,
        message: FixMessage,
        expected_type: str,
        required_fields: Optional[list[str]] = None,
    ) -> None:
        """Assert message has correct structure and required fields."""
        # Check message type
        msg_type = message.get(FixTags.MSG_TYPE)
        self.assertIsNotNone(msg_type, "Message missing type field")
        self.assertEqual(msg_type.decode(), expected_type)

        # Check required header fields
        required_header_fields = [
            FixTags.BEGIN_STRING,
            FixTags.BODY_LENGTH,
            FixTags.MSG_TYPE,
            FixTags.SENDER_COMP_ID,
            FixTags.TARGET_COMP_ID,
            FixTags.MSG_SEQ_NUM,
            FixTags.SENDING_TIME,
        ]

        for field in required_header_fields:
            value = message.get(field)
            self.assertIsNotNone(value, f"Message missing required header field: {field}")

        # Check additional required fields if provided
        if required_fields:
            for field in required_fields:
                value = message.get(field)
                self.assertIsNotNone(value, f"Message missing required field: {field}")

    def assert_session_metrics(
        self,
        metrics: SessionMetrics,
        min_messages: int = 0,
        max_errors: int = 0,
        max_duration: Optional[float] = None,
    ) -> None:
        """Assert session metrics meet expectations."""
        total_messages = metrics.messages_sent + metrics.messages_received
        self.assertGreaterEqual(total_messages, min_messages)
        self.assertLessEqual(len(metrics.errors), max_errors)

        if max_duration and metrics.total_duration:
            self.assertLessEqual(metrics.total_duration, max_duration)

        if metrics.errors:
            logger.warning("Session had %s errors: %s", len(metrics.errors), metrics.errors)

    def _check_auto_logon_rejection(self, session: BinanceFixConnector, initial_messages: list[FixMessage]) -> None:
        """Check initial messages for rejection/logout and skip test if session was rejected."""
        for msg in initial_messages:
            msg_type = msg.get("35")
            if msg_type and msg_type.decode() in ["5", "3"]:
                msg_text = msg.get("58")
                if msg_text:
                    self.skipTest(f"Session rejected: {msg_text.decode()}")
                else:
                    self.skipTest("Session was rejected during auto-logon")

        if not session.is_connected:
            self.skipTest("Session disconnected during auto-logon")

    async def _create_and_setup_session(self, session_type: str) -> tuple[BinanceFixConnector, SessionMetrics]:
        """Create and set up a mocked session, returning (session, metrics)."""
        e2e_session = E2ETestSession(
            session_type=session_type,
            credentials=self.credentials,
            use_real_testnet=False,
        )
        await e2e_session.connect()
        return e2e_session.session, e2e_session.metrics

    async def _cleanup_session(self, session: BinanceFixConnector) -> None:
        """Clean up a session safely."""
        try:
            if hasattr(session, "is_connected") and session.is_connected:
                await session.disconnect()
        except Exception:
            logger.debug("Cleanup error", exc_info=True)

    async def _create_order(
        self,
        session,
        order_id: Optional[str] = None,
        *,
        symbol: str = "BTCUSDT",
        side: str = "1",
        order_type: str = "2",
        quantity: str = "0.001",
        price: str = "50000.00",
    ) -> FixMessage:
        """Create an order message for testing."""
        if order_id is None:
            order_id = f"TEST_{int(time.time())}"

        msg = await session.create_fix_message_with_basic_header("D", "5000")
        msg.append_pair("11", order_id)
        msg.append_pair("55", symbol)
        msg.append_pair("54", side)
        msg.append_pair("40", order_type)
        msg.append_pair("38", quantity)
        msg.append_pair("44", price)
        msg.append_pair("59", "1")
        return msg

    async def _create_execution_report(
        self,
        session,
        order_id: str,
        exec_type: str = "F",
        order_status: str = "2",
        symbol: str = "BTCUSDT",
        side: str = "1",
        quantity: str = "0.001",
        fill_qty: str | None = None,
        fill_price: str | None = None,
    ) -> FixMessage:
        """Create execution report message for testing."""
        msg = await session.create_fix_message_with_basic_header("8", "5000")
        msg.append_pair("11", order_id)
        msg.append_pair("37", f"SERVER_{order_id}")
        msg.append_pair("17", f"EXEC_{uuid.uuid4().hex[:8]}")
        msg.append_pair("150", exec_type)
        msg.append_pair("39", order_status)
        msg.append_pair("55", symbol)
        msg.append_pair("54", side)
        msg.append_pair("38", quantity)
        msg.append_pair("60", session.current_utc_time())
        if fill_qty:
            msg.append_pair("14", fill_qty)
            msg.append_pair("32", fill_qty)
        if fill_price:
            msg.append_pair("31", fill_price)
            msg.append_pair("6", fill_price)
        return msg

    async def _create_cancel_order(
        self,
        session,
        original_order_id: str,
        symbol: str = "BTCUSDT",
    ) -> FixMessage:
        """Create an order cancel request message for testing."""
        msg = await session.create_fix_message_with_basic_header("F", "5000")
        cancel_id = f"CANCEL_{int(time.time())}_{uuid.uuid4().hex[:8]}"
        msg.append_pair("11", cancel_id)
        msg.append_pair("41", original_order_id)
        msg.append_pair("55", symbol)
        return msg

    async def _create_market_data_request(
        self,
        session,
        symbol: str,
        md_req_type: str,
        md_entry_types: list[str] | None = None,
        md_req_id: str | None = None,
    ) -> FixMessage:
        """Create a market data request message."""
        msg = await session.create_fix_message_with_basic_header("V", "5000")
        msg.append_pair("262", md_req_id or f"MD_{int(time.time())}_{symbol}")
        msg.append_pair("263", md_req_type)
        msg.append_pair("264", "1")
        msg.append_pair("266", "Y")
        msg.append_pair("146", "1")
        msg.append_pair("55", symbol)
        if md_entry_types is None:
            md_entry_types = ["0", "1"]
        msg.append_pair("267", str(len(md_entry_types)))
        for entry_type in md_entry_types:
            msg.append_pair("269", entry_type)
        return msg

    async def _create_large_execution_report(self, session, index: int):
        """Create large execution report for memory pressure testing."""
        msg = await session.create_fix_message_with_basic_header("8", "5000")

        # Add many fields to make message large
        msg.append_pair("11", f"LARGE_ORDER_{index}")
        msg.append_pair("37", f"SERVER_ORDER_{index}")
        msg.append_pair("17", f"EXEC_{index}")
        msg.append_pair("150", "F")
        msg.append_pair("39", "2")
        msg.append_pair("55", "BTCUSDT")
        msg.append_pair("54", "1")
        msg.append_pair("38", "0.001")
        msg.append_pair("60", session.current_utc_time())

        # Add extra data to make message larger
        for i in range(20):
            msg.append_pair(f"900{i:02d}", f"LARGE_DATA_FIELD_{index}_{i}_" + "X" * 50)

        return msg

    async def _create_fake_market_data(
        self,
        session,
        symbol: str = "BTCUSDT",
        price: str = "50000.00",
        include_price_levels: bool = False,
    ) -> FixMessage:
        """Create fake market data message for testing."""
        msg = await session.create_fix_message_with_basic_header("W", "5000")
        msg.append_pair("55", symbol)

        if include_price_levels:
            msg.append_pair("268", "2")
            msg.append_pair("269", "0")
            msg.append_pair("270", str(price))
            msg.append_pair("271", "1.5")
            msg.append_pair("269", "1")
            msg.append_pair("270", str(float(price) + 1))
            msg.append_pair("271", "2.0")
        else:
            msg.append_pair("268", "1")
            msg.append_pair("269", "0")
            msg.append_pair("270", str(price))
            msg.append_pair("271", "1.0")

        return msg


# Test utilities
async def simulate_network_failure(session: BinanceFixConnector, duration: float = 1.0) -> None:
    """Simulate network failure by temporarily disrupting connection."""
    if hasattr(session, "_writer") and session._writer:
        # Temporarily close writer to simulate network issue
        original_writer = session._writer
        session._writer = None
        await asyncio.sleep(duration)
        session._writer = original_writer


async def create_concurrent_sessions(
    credentials: E2ECredentials,
    session_configs: list[dict[str, Any]],
    max_concurrent: int = CONCURRENT_SESSION_LIMIT,
) -> list[tuple[BinanceFixConnector, SessionMetrics]]:
    """Create multiple concurrent sessions for load testing."""
    sessions = []
    semaphore = asyncio.Semaphore(max_concurrent)

    async def create_session(config: dict[str, Any]) -> tuple[BinanceFixConnector, SessionMetrics]:
        async with semaphore:
            session_wrapper = E2ETestSession(
                session_type=config["type"],
                credentials=credentials,
                use_real_testnet=config.get("use_real_testnet", False),
                endpoint_override=config.get("endpoint"),
            )
            await session_wrapper.connect()
            return session_wrapper.session, session_wrapper.metrics

    # Create sessions concurrently
    tasks = [create_session(config) for config in session_configs]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Filter out exceptions and return successful sessions
    for result in results:
        if not isinstance(result, Exception):
            sessions.append(result)
        else:
            logger.warning("Failed to create session: %s", result)

    return sessions


if __name__ == "__main__":
    pytest.main([__file__])
