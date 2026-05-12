#!/usr/bin/env python3
"""
Comprehensive unit tests for async Binance FIX Connector.

Validates API compatibility and functionality, ensuring identical
behavior to the synchronous version.
"""

import asyncio
import base64
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from simplefix import FixMessage

from binance_fix_connector_async.fix_connector import (
    FIX_DC_URL,
    FIX_MD_URL,
    FIX_OE_URL,
    MAX_BUFFER_SIZE,
    MAX_MESSAGE_HISTORY_SIZE,
    MAX_SENDER_ID_LENGTH,
    BinanceFixConnector,
    FixMsgTypes,
    FixTags,
    create_drop_copy_session,
    create_market_data_session,
    create_order_entry_session,
)
from tests.test_base import FixConnectorTestBase

SOH = b"\x01"

EXPECTED_FIX_MSG_TYPES = {
    "HEARTBEAT": "0",
    "TEST_REQUEST": "1",
    "REJECT": "3",
    "LOGOUT": "5",
    "LOGON": "A",
    "NEWS": "B",
    "NEW_ORDER_SINGLE": "D",
    "NEW_ORDER_LIST": "E",
    "ORDER_CANCEL_REQUEST": "F",
    "LIST_STATUS": "N",
    "EXECUTION_REPORT": "8",
    "ORDER_CANCEL_REJECT": "9",
    "ORDER_MASS_CANCEL_REQUEST": "q",
    "ORDER_MASS_CANCEL_REPORT": "r",
    "MARKET_DATA_REQUEST": "V",
    "MARKET_DATA_SNAPSHOT": "W",
    "MARKET_DATA_INCREMENTAL_REFRESH": "X",
    "MARKET_DATA_REQUEST_REJECT": "Y",
    "INSTRUMENT_LIST_REQUEST": "x",
    "INSTRUMENT_LIST": "y",
    "LIMIT_QUERY": "XLQ",
    "LIMIT_RESPONSE": "XLR",
    "ORDER_CANCEL_REPLACE_REQUEST": "XCN",
    "ORDER_AMEND_KEEP_PRIORITY_REQUEST": "XAK",
    "ORDER_AMEND_REJECT": "XAR",
}


def _encoded_pairs(encoded: bytes) -> list[tuple[str, str]]:
    return [
        (tag.decode("ascii"), value.decode("ascii"))
        for part in encoded.rstrip(SOH).split(SOH)
        if part
        for tag, value in [part.split(b"=", 1)]
    ]


def _encoded_tag_values(encoded: bytes) -> dict[str, str]:
    return dict(_encoded_pairs(encoded))


def _fix_body_length(encoded: bytes) -> int:
    body_start = encoded.index(b"\x0135=") + 1
    body_end = encoded.rindex(b"\x0110=") + 1
    return body_end - body_start


def _fix_checksum(encoded: bytes) -> str:
    checksum_tag_start = encoded.rindex(b"10=")
    return f"{sum(encoded[:checksum_tag_start]) % 256:03d}"


class _TaskStub:
    def __init__(self, *, done: bool = False, cancelled: bool = False):
        self._done = done
        self._cancelled = cancelled
        self.cancel = MagicMock()
        self.add_done_callback = MagicMock()

    def done(self):
        return self._done

    def cancelled(self):
        return self._cancelled

    def __await__(self):
        if False:
            yield None
        return None


def _mock_task(*, done: bool = False, cancelled: bool = False):
    return _TaskStub(done=done, cancelled=cancelled)


def _close_created_coroutine_and_return(task):
    def _side_effect(coro, *args, **kwargs):
        if hasattr(coro, "close"):
            coro.close()
        return task

    return _side_effect


class TestBinanceFixConnectorConstructor(FixConnectorTestBase):
    """Test constructor parameter validation and initialization."""

    def test_constructor_with_valid_parameters(self):
        connector = self.create_connector()

        self.assertEqual(connector.endpoint, self.valid_endpoint)
        self.assertEqual(connector.api_key, self.valid_api_key)
        self.assertEqual(connector.private_key, self.valid_private_key)
        self.assertEqual(connector.sender_comp_id, self.valid_sender_comp_id)

        self.assertEqual(connector.target_comp_id, "SPOT")
        self.assertEqual(connector.fix_version, "FIX.4.4")
        self.assertEqual(connector.socket_buffer_size, MAX_BUFFER_SIZE)
        self.assertEqual(connector.heart_bt_int, 30)
        self.assertTrue(connector.reset_seq_num_flag)
        self.assertEqual(connector.encrypt_method, 0)
        self.assertEqual(connector.message_handling, 2)
        self.assertEqual(connector.response_mode, 1)
        self.assertFalse(connector.drop_copy_flag)

        self.assertIsInstance(connector._lock, asyncio.Lock)
        self.assertIsNone(connector._reader)
        self.assertIsNone(connector._writer)
        self.assertIsNone(connector._receive_task)
        self.assertFalse(connector.is_connected)
        self.assertEqual(connector._message_history, [])
        self.assertEqual(connector.msg_seq_num, 1)
        self.assertEqual(len(connector.messages_sent), 0)

    def test_constructor_with_custom_parameters(self):
        connector = self.create_connector(
            target_comp_id="FUTURES",
            fix_version="FIX.4.2",
            socket_buffer_size=8192,
            heart_bt_int=60,
            reset_seq_num_flag="Y",
            encrypt_method=0,
            message_handling=1,
            response_mode=2,
            drop_copy_flag=True,
        )

        self.assertEqual(connector.target_comp_id, "FUTURES")
        self.assertEqual(connector.fix_version, "FIX.4.2")
        self.assertEqual(connector.socket_buffer_size, 8192)
        self.assertEqual(connector.heart_bt_int, 60)
        self.assertEqual(connector.reset_seq_num_flag, "Y")
        self.assertEqual(connector.encrypt_method, 0)
        self.assertEqual(connector.message_handling, 1)
        self.assertEqual(connector.response_mode, 2)
        self.assertTrue(connector.drop_copy_flag)

    def test_constructor_endpoint_validation(self):
        with self.assertRaises(ValueError) as cm:
            self.create_connector(endpoint=None)
        self.assertIn("endpoint can not be None or empty", str(cm.exception))

        with self.assertRaises(ValueError) as cm:
            self.create_connector(endpoint="")
        self.assertIn("endpoint can not be None or empty", str(cm.exception))

    def test_constructor_api_key_validation(self):
        with self.assertRaises(ValueError) as cm:
            self.create_connector(api_key=None)
        self.assertIn("api_key can not be None or empty", str(cm.exception))

        with self.assertRaises(ValueError) as cm:
            self.create_connector(api_key="")
        self.assertIn("api_key can not be None or empty", str(cm.exception))

    def test_constructor_private_key_validation(self):
        with self.assertRaises(ValueError) as cm:
            self.create_connector(private_key=None)
        self.assertIn("private_key can not be None or empty", str(cm.exception))

    def test_constructor_sender_comp_id_validation(self):
        with self.assertRaises(ValueError) as cm:
            self.create_connector(sender_comp_id=None)
        self.assertIn("sender_comp_id can not be None or empty", str(cm.exception))

        with self.assertRaises(ValueError) as cm:
            self.create_connector(sender_comp_id="")
        self.assertIn("sender_comp_id can not be None or empty", str(cm.exception))

        with self.assertRaises(ValueError) as cm:
            self.create_connector(sender_comp_id="TOOLONGID")
        self.assertIn("sender_comp_id can not be longer than 8 characters", str(cm.exception))

    def test_constructor_multiple_validation_errors(self):
        with self.assertRaises(ValueError) as cm:
            BinanceFixConnector(
                endpoint="",
                api_key="",
                private_key=None,
                sender_comp_id="",
            )
        error_message = str(cm.exception)
        self.assertIn("endpoint can not be None or empty", error_message)
        self.assertIn("api_key can not be None or empty", error_message)
        self.assertIn("private_key can not be None or empty", error_message)
        self.assertIn("sender_comp_id can not be None or empty", error_message)

    def test_parameter_validation_comprehensive(self):
        invalid_params = {
            "endpoint": [None, ""],
            "api_key": [None, ""],
            "heart_bt_int": [-1, 0, 4, 61, 3601],
            "socket_buffer_size": [0, -1, 1024 * 1024 * 100],
        }
        for param_name, invalid_values in invalid_params.items():
            for invalid_value in invalid_values:
                with self.subTest(param=param_name, value=invalid_value):
                    params = {
                        "endpoint": self.valid_endpoint,
                        "api_key": self.valid_api_key,
                        "private_key": self.valid_private_key,
                        "sender_comp_id": self.valid_sender_comp_id,
                        param_name: invalid_value,
                    }
                    if param_name in ["endpoint", "api_key", "heart_bt_int"]:
                        with self.assertRaises((ValueError, TypeError)):
                            BinanceFixConnector(**params)
                    else:
                        connector = BinanceFixConnector(**params)
                        self.assertIsNotNone(connector)

    def test_protocol_parameter_validation(self):
        with self.assertRaises(ValueError) as cm:
            self.create_connector(reset_seq_num_flag="N")
        self.assertIn("reset_seq_num_flag must be 'Y'", str(cm.exception))

        with self.assertRaises(ValueError) as cm:
            self.create_connector(encrypt_method=1)
        self.assertIn("encrypt_method must be 0", str(cm.exception))

    def test_constructor_type_conversion(self):
        connector = self.create_connector(
            sender_comp_id=123,
            target_comp_id=456,
            fix_version=4.4,
        )

        self.assertEqual(connector.sender_comp_id, "123")
        self.assertEqual(connector.target_comp_id, "456")
        self.assertEqual(connector.fix_version, "4.4")
        self.assertIsInstance(connector.sender_comp_id, str)
        self.assertIsInstance(connector.target_comp_id, str)
        self.assertIsInstance(connector.fix_version, str)


class TestFactoryFunctions(FixConnectorTestBase):
    """Test factory function behavior and parameter handling."""

    @patch("binance_fix_connector_async.fix_connector._create_session")
    async def test_create_market_data_session_defaults(self, mock_create_session):
        mock_connector = MagicMock()
        mock_create_session.return_value = mock_connector

        result = await create_market_data_session(
            api_key=self.valid_api_key,
            private_key=self.valid_private_key,
        )

        self.assertEqual(result, mock_connector)
        mock_create_session.assert_called_once_with(
            "market_data",
            self.valid_api_key,
            self.valid_private_key,
            FIX_MD_URL,
            "WATCH",
            "SPOT",
            "FIX.4.4",
            30,
            2,
            recv_window=None,
        )

    @patch("binance_fix_connector_async.fix_connector._create_session")
    async def test_create_order_entry_session_defaults(self, mock_create_session):
        mock_connector = MagicMock()
        mock_create_session.return_value = mock_connector

        result = await create_order_entry_session(
            api_key=self.valid_api_key,
            private_key=self.valid_private_key,
        )

        self.assertEqual(result, mock_connector)
        mock_create_session.assert_called_once_with(
            "order_entry",
            self.valid_api_key,
            self.valid_private_key,
            FIX_OE_URL,
            "TRADE",
            "SPOT",
            "FIX.4.4",
            30,
            2,
            1,
            None,
        )

    @patch("binance_fix_connector_async.fix_connector._create_session")
    async def test_create_drop_copy_session_defaults(self, mock_create_session):
        mock_connector = MagicMock()
        mock_create_session.return_value = mock_connector

        result = await create_drop_copy_session(
            api_key=self.valid_api_key,
            private_key=self.valid_private_key,
        )

        self.assertEqual(result, mock_connector)
        mock_create_session.assert_called_once_with(
            "drop_copy",
            self.valid_api_key,
            self.valid_private_key,
            FIX_DC_URL,
            "TECH",
            "SPOT",
            "FIX.4.4",
            30,
            2,
            1,
            None,
        )

    @patch("binance_fix_connector_async.fix_connector.BinanceFixConnector")
    async def test_sender_comp_id_length_truncation(self, mock_connector_cls):
        mock_connector = mock_connector_cls.return_value
        mock_connector.connect = AsyncMock()
        mock_connector.logon = AsyncMock()
        logon_msg = FixMessage()
        logon_msg.append_pair(FixTags.MSG_TYPE, FixMsgTypes.LOGON)
        mock_connector.retrieve_messages_until = AsyncMock(return_value=[logon_msg])
        mock_connector._decode = BinanceFixConnector._decode
        mock_connector._retrieve_cursor = 0

        await create_market_data_session(
            api_key=self.valid_api_key,
            private_key=self.valid_private_key,
            sender_comp_id="VERYLONGID",
        )

        _args, kwargs = mock_connector_cls.call_args
        self.assertEqual(kwargs["sender_comp_id"], "BMDVERYL")
        self.assertEqual(len(kwargs["sender_comp_id"]), MAX_SENDER_ID_LENGTH)


class TestConnectionMethods(FixConnectorTestBase):
    """Test connection and disconnection functionality."""

    @patch("ssl.create_default_context")
    @patch("asyncio.open_connection")
    async def test_connect_ssl_error(self, mock_open_connection, mock_ssl_context):
        mock_ssl_context.side_effect = Exception("SSL Error")
        with self.assertRaises(Exception):
            await self.connector.connect()

    async def test_disconnect_with_task_cleanup_error(self):
        self.connector._reader = self.mock_reader
        self.connector._writer = self.mock_writer
        self.connector.is_connected = True

        mock_task = MagicMock()
        mock_task.cancel = MagicMock(side_effect=OSError("Cancel failed"))
        mock_task.done.return_value = False
        self.connector._receive_task = mock_task

        await self.connector.disconnect()
        self.assertFalse(self.connector.is_connected)

    async def test_disconnect_with_task_runtime_cleanup_error(self):
        self.connector._reader = self.mock_reader
        self.connector._writer = self.mock_writer
        self.connector.is_connected = True

        class RuntimeErrorTask:
            def cancel(self):
                return None

            def done(self):
                return False

            def __await__(self):
                raise RuntimeError("await wasn't used with future")
                yield

        self.connector._receive_task = RuntimeErrorTask()

        await self.connector.disconnect()
        self.assertFalse(self.connector.is_connected)

    @patch("binance_fix_connector_async.fix_connector.asyncio.open_connection")
    @patch("binance_fix_connector_async.fix_connector.ssl.create_default_context")
    @patch("binance_fix_connector_async.fix_connector.asyncio.create_task")
    async def test_connect_success(self, mock_create_task, mock_ssl_context, mock_open_connection):
        mock_reader = AsyncMock()
        mock_writer = AsyncMock()
        mock_open_connection.return_value = (mock_reader, mock_writer)
        mock_ssl_context.return_value = MagicMock()
        mock_task = _mock_task()
        mock_create_task.side_effect = _close_created_coroutine_and_return(mock_task)

        await self.connector.connect()

        self.assertTrue(self.connector.is_connected)
        self.assertEqual(self.connector._reader, mock_reader)
        self.assertEqual(self.connector._writer, mock_writer)
        self.assertEqual(self.connector._receive_task, mock_task)

        mock_open_connection.assert_called_once_with("test.example.com", 9000, ssl=mock_ssl_context.return_value)

    @patch("binance_fix_connector_async.fix_connector.asyncio.open_connection")
    @patch("binance_fix_connector_async.fix_connector.asyncio.create_task")
    async def test_connect_failure(self, mock_create_task, mock_open_connection):
        mock_open_connection.side_effect = ConnectionError("Connection failed")

        with self.assertRaises(ConnectionError):
            await self.connector.connect()

        self.assertFalse(self.connector.is_connected)
        self.assertIsNone(self.connector._reader)
        self.assertIsNone(self.connector._writer)

    async def test_disconnect_clean_shutdown(self):
        mock_writer = AsyncMock()
        mock_writer.close = MagicMock()

        async def mock_receive_messages():
            try:
                await asyncio.sleep(0.01)
            except asyncio.CancelledError:
                raise

        mock_task = asyncio.create_task(mock_receive_messages())

        self.connector._writer = mock_writer
        self.connector._receive_task = mock_task
        self.connector.is_connected = True

        await self.connector.disconnect()

        self.assertFalse(self.connector.is_connected)
        self.assertTrue(mock_task.cancelled())
        mock_writer.close.assert_called_once()
        mock_writer.wait_closed.assert_called_once()
        self.assertIsNone(self.connector._writer)
        self.assertIsNone(self.connector._reader)

    async def test_disconnect_with_writer_cleanup_cancelled(self):
        mock_writer = AsyncMock()
        mock_writer.close = MagicMock()
        mock_writer.wait_closed = AsyncMock(side_effect=asyncio.CancelledError)

        self.connector._writer = mock_writer
        self.connector.is_connected = True

        await self.connector.disconnect()

        self.assertFalse(self.connector.is_connected)
        mock_writer.close.assert_called_once()
        mock_writer.wait_closed.assert_called_once()
        self.assertIsNone(self.connector._writer)
        self.assertIsNone(self.connector._reader)

    async def test_disconnect_no_connection(self):
        await self.connector.disconnect()

        self.assertFalse(self.connector.is_connected)
        self.assertIsNone(self.connector._writer)
        self.assertIsNone(self.connector._reader)


class TestUtilityMethods(FixConnectorTestBase):
    """Test utility and helper methods."""

    def test_current_utc_time_format(self):
        time_str = self.connector.current_utc_time()
        self.assertRegex(time_str, r"^\d{8}-\d{2}:\d{2}:\d{2}\.\d{6}$")
        datetime.strptime(time_str, "%Y%m%d-%H:%M:%S.%f")

    def test_generate_signature_valid(self):
        signature = self.connector.generate_signature("TEST", "SPOT", 1, "20250301-01:00:00.000000")
        self.assertIsInstance(signature, str)

        try:
            base64.b64decode(signature)
        except Exception:
            self.fail("Signature is not valid base64")

    def test_generate_signature_no_private_key(self):
        self.connector.private_key = None

        with self.assertRaises(ValueError) as cm:
            self.connector.generate_signature("TEST", "SPOT", 1, "20250301-01:00:00.000000")

        self.assertIn("Please provide an Ed25519 key", str(cm.exception))

    async def test_create_fix_message_with_basic_header(self):
        with patch.object(self.connector, "current_utc_time") as mock_time:
            mock_time.return_value = "20250301-01:00:00.000000"
            msg = await self.connector.create_fix_message_with_basic_header("D", "5000")

        self.assertIsInstance(msg, FixMessage)
        self.assertEqual(msg.get(FixTags.BEGIN_STRING).decode(), "FIX.4.4")
        self.assertEqual(msg.get(FixTags.MSG_TYPE).decode(), "D")
        self.assertEqual(msg.get(FixTags.SENDER_COMP_ID).decode(), "TEST")
        self.assertEqual(msg.get(FixTags.TARGET_COMP_ID).decode(), "SPOT")
        self.assertEqual(msg.get(FixTags.MSG_SEQ_NUM).decode(), "0")
        self.assertEqual(msg.get(FixTags.SENDING_TIME).decode(), "20250301-01:00:00.000000")
        self.assertEqual(msg.get(FixTags.RECV_WINDOW).decode(), "5000")


class TestMessageHandling(FixConnectorTestBase):
    """Test message sending, receiving, and parsing functionality."""

    async def test_send_message_success(self):
        mock_writer = AsyncMock()
        mock_writer.write = MagicMock()
        mock_writer.drain = AsyncMock()
        self.connector._writer = mock_writer

        msg = FixMessage()
        msg.append_pair("8", "FIX.4.4", header=True)
        msg.append_pair("35", "D", header=True)

        await self.connector.send_message(msg)

        mock_writer.write.assert_called_once()
        mock_writer.drain.assert_called_once()
        self.assertEqual(len(self.connector.messages_sent), 1)
        self.assertEqual(self.connector.messages_sent[0], msg)

    async def test_send_message_no_connection_raises(self):
        self.connector._writer = None

        msg = FixMessage()
        msg.append_pair("35", "D")

        with self.assertRaises(ConnectionError):
            await self.connector.send_message(msg)
        self.assertEqual(len(self.connector.messages_sent), 0)

    async def test_get_all_new_messages_received_empty(self):
        messages = await self.connector.get_all_new_messages_received()
        self.assertEqual(messages, [])

    async def test_get_all_new_messages_received_with_messages(self):
        msg1 = FixMessage()
        msg1.append_pair("35", "8")
        msg2 = FixMessage()
        msg2.append_pair("35", "9")

        await self.connector.on_message_received([msg1])
        await self.connector.on_message_received([msg2])

        messages = await self.connector.get_all_new_messages_received()

        self.assertEqual(len(messages), 2)
        self.assertEqual(messages[0], msg1)
        self.assertEqual(messages[1], msg2)
        self.assertEqual(await self.connector.get_all_new_messages_received(), [])

    async def test_retrieve_messages_until_timeout(self):
        messages = await self.connector.retrieve_messages_until("8", timeout_seconds=0.1)
        self.assertEqual(messages, [])

    async def test_send_message_without_connection_raises(self):
        self.connector._writer = None
        msg = self.create_fix_message()
        with self.assertRaises(ConnectionError):
            await self.connector.send_message(msg)

    @patch("asyncio.create_task")
    @patch("asyncio.open_connection")
    async def test_concurrent_message_sending(self, mock_open_connection, mock_create_task):
        mock_open_connection.return_value = (self.mock_reader, self.mock_writer)
        mock_task = _mock_task()
        mock_create_task.side_effect = _close_created_coroutine_and_return(mock_task)

        await self.connector.connect()

        messages = [self.create_fix_message("D", {"38": "1.0", "44": f"5000{i}", "55": "BTCUSDT"}) for i in range(10)]
        send_tasks = [self.connector.send_message(msg) for msg in messages]
        await asyncio.gather(*send_tasks, return_exceptions=True)
        self.assertGreater(len(self.connector.messages_sent), 0)
        await self.connector.disconnect()

    @patch("asyncio.open_connection")
    async def test_large_message_handling(self, mock_open_connection):
        mock_open_connection.return_value = (self.mock_reader, self.mock_writer)
        await self.connector.connect()

        large_msg = self.create_fix_message("D")
        for i in range(100):
            large_msg.append_pair(f"600{i:02d}", f"LARGE_VALUE_{i}" * 10)
        await self.connector.send_message(large_msg)
        self.assertGreater(len(self.connector.messages_sent), 0)
        await self.connector.disconnect()

    async def test_retrieve_messages_until_found(self):
        msg1 = FixMessage()
        msg1.append_pair("35", "9")
        msg2 = FixMessage()
        msg2.append_pair("35", "8")

        await self.connector.on_message_received([msg1])
        await self.connector.on_message_received([msg2])

        messages = await self.connector.retrieve_messages_until("8", timeout_seconds=1)

        self.assertEqual(len(messages), 2)
        self.assertEqual(messages[0], msg1)
        self.assertEqual(messages[1], msg2)

    async def test_retrieve_messages_until_allows_concurrent_waiters(self):
        async def wait_for_execution_report():
            return await self.connector.retrieve_messages_until("8", timeout_seconds=1)

        waiters = [
            asyncio.create_task(wait_for_execution_report()),
            asyncio.create_task(wait_for_execution_report()),
        ]
        await asyncio.sleep(0)

        msg = FixMessage()
        msg.append_pair("35", "8")
        await self.connector.on_message_received([msg])

        results = await asyncio.gather(*waiters)
        self.assertEqual([[m.get("35").decode("utf-8") for m in result] for result in results], [["8"], ["8"]])

    def test_parse_server_response_empty(self):
        self.connector._receive_buffer = b""
        messages = self.connector.parse_server_response()
        self.assertEqual(messages, [])

    def test_parse_server_response_incomplete(self):
        self.connector._receive_buffer = b"8=FIX.4.4\x019=10\x0135=D"
        messages = self.connector.parse_server_response()
        self.assertEqual(messages, [])
        self.assertGreater(len(self.connector._receive_buffer), 0)

    def test_parse_server_response_complete(self):
        complete_msg = b"8=FIX.4.4\x019=25\x0135=D\x0149=TEST\x0156=SPOT\x0110=123\x01"
        self.connector._receive_buffer = complete_msg

        messages = self.connector.parse_server_response()

        self.assertEqual(len(messages), 1)
        self.assertIsInstance(messages[0], FixMessage)
        self.assertEqual(self.connector._receive_buffer, b"")


class TestProtocolMethods(FixConnectorTestBase):
    """Test FIX protocol methods (logon, logout, heartbeat, etc.)."""

    def setUp(self):
        super().setUp()
        self.connector._writer = self.mock_writer
        self.connector.is_connected = True

    @patch.object(BinanceFixConnector, "current_utc_time")
    async def test_logon_message_construction(self, mock_time):
        mock_time.return_value = "20250301-01:00:00.000000"

        await self.connector.logon(recv_window="5000")

        self.connector._writer.write.assert_called_once()
        self.assertEqual(len(self.connector.messages_sent), 1)
        self.assertEqual(self.connector.msg_seq_num, 1)

    @patch.object(BinanceFixConnector, "current_utc_time")
    async def test_logout_message_construction(self, mock_time):
        mock_time.return_value = "20250301-01:00:00.000000"

        await self.connector.logout(text="Test logout", recv_window="5000")

        self.connector._writer.write.assert_called_once()
        self.assertEqual(len(self.connector.messages_sent), 1)

    @patch.object(BinanceFixConnector, "current_utc_time")
    async def test_heartbeat_message_construction(self, mock_time):
        mock_time.return_value = "20250301-01:00:00.000000"

        await self.connector.heartbeat(test_req_id="TEST123", recv_window="5000")

        self.connector._writer.write.assert_called_once()
        self.assertEqual(len(self.connector.messages_sent), 1)

    @patch.object(BinanceFixConnector, "current_utc_time")
    async def test_test_request_message_construction(self, mock_time):
        mock_time.return_value = "20250301-01:00:00.000000"

        await self.connector.test_request(test_req_id="TEST456", recv_window="5000")

        self.connector._writer.write.assert_called_once()
        self.assertEqual(len(self.connector.messages_sent), 1)

    async def test_on_message_received_test_request_handling(self):
        test_req_msg = FixMessage()
        test_req_msg.append_pair(FixTags.MSG_TYPE, FixMsgTypes.TEST_REQUEST)
        test_req_msg.append_pair(FixTags.TEST_REQ_ID, "AUTO_TEST_123")

        await self.connector.on_message_received([test_req_msg])

        self.assertGreater(self.connector._writer.write.call_count, 0)

    async def test_on_message_received_normal_message(self):
        normal_msg = FixMessage()
        normal_msg.append_pair(FixTags.MSG_TYPE, "8")

        await self.connector.on_message_received([normal_msg])

        messages = await self.connector.get_all_new_messages_received()
        self.assertEqual(messages, [normal_msg])

    async def test_receive_apis_do_not_steal_each_other_messages(self):
        msg = FixMessage()
        msg.append_pair(FixTags.MSG_TYPE, "8")

        await self.connector.on_message_received([msg])

        all_messages = await self.connector.get_all_new_messages_received()
        retrieved = await self.connector.retrieve_messages_until("8", timeout_seconds=0.1)

        self.assertEqual(all_messages, [msg])
        self.assertEqual(retrieved, [msg])

    async def test_message_history_cap_adjusts_cursors(self):
        self.connector._message_history = [FixMessage() for _ in range(MAX_MESSAGE_HISTORY_SIZE)]
        self.connector._get_all_cursor = 10
        self.connector._retrieve_cursor = 20
        msg = FixMessage()

        await self.connector.on_message_received([msg])

        self.assertEqual(len(self.connector._message_history), MAX_MESSAGE_HISTORY_SIZE)
        self.assertIs(self.connector._message_history[-1], msg)
        self.assertEqual(self.connector._get_all_cursor, 9)
        self.assertEqual(self.connector._retrieve_cursor, 19)


class TestErrorScenarios(FixConnectorTestBase):
    """Test error handling and edge cases."""

    @patch("binance_fix_connector_async.fix_connector.asyncio.open_connection")
    @patch("binance_fix_connector_async.fix_connector.asyncio.create_task")
    async def test_connect_exception_handling(self, mock_create_task, mock_open_connection):
        mock_open_connection.side_effect = OSError("Network error")

        with self.assertRaises(OSError):
            await self.connector.connect()

    async def test_send_message_exception_handling(self):
        mock_writer = AsyncMock()
        mock_writer.write = MagicMock(side_effect=OSError("Write error"))
        self.connector._writer = mock_writer

        msg = FixMessage()
        msg.append_pair(8, "FIX.4.4", header=True)
        msg.append_pair(35, "D", header=True)

        with self.assertRaises(OSError):
            await self.connector.send_message(msg)
        self.assertEqual(len(self.connector.messages_sent), 0)

    @patch("binance_fix_connector_async.fix_connector.asyncio.open_connection")
    @patch("binance_fix_connector_async.fix_connector.asyncio.create_task")
    async def test_receive_messages_task_cancellation(self, mock_create_task, mock_open_connection):
        mock_reader = AsyncMock()
        mock_writer = AsyncMock()
        mock_open_connection.return_value = (mock_reader, mock_writer)

        mock_task = _mock_task(done=True, cancelled=True)
        mock_create_task.side_effect = _close_created_coroutine_and_return(mock_task)

        await self.connector.connect()

        self.assertTrue(self.connector.is_connected)
        self.assertEqual(self.connector._reader, mock_reader)
        self.assertEqual(self.connector._writer, mock_writer)


class TestParseServerResponseEdgeCases(FixConnectorTestBase):
    """Test parse_server_response with multi-message, partial, and boundary inputs."""

    def _make_msg(self, msg_type="D", seq="1"):
        return (f"8=FIX.4.4\x019=25\x0135={msg_type}\x0149=TEST\x0156=SPOT\x0134={seq}\x0110=123\x01").encode()

    def test_two_complete_messages(self):
        self.connector._receive_buffer = self._make_msg("D", "1") + self._make_msg("A", "2")
        messages = self.connector.parse_server_response()
        self.assertEqual(len(messages), 2)
        self.assertEqual(self.connector._receive_buffer, b"")

    def test_complete_plus_partial_preserves_partial(self):
        partial = b"8=FIX.4.4\x019=25\x0135=D\x0149=TEST"
        self.connector._receive_buffer = self._make_msg("A", "1") + partial
        messages = self.connector.parse_server_response()
        self.assertEqual(len(messages), 1)
        self.assertGreater(len(self.connector._receive_buffer), 0)
        self.assertIn(b"35=D", self.connector._receive_buffer)

    def test_three_messages_plus_partial(self):
        partial = b"8=FIX.4.4\x019=10\x0135=0"
        self.connector._receive_buffer = (
            self._make_msg("D", "1") + self._make_msg("A", "2") + self._make_msg("5", "3") + partial
        )
        messages = self.connector.parse_server_response()
        self.assertEqual(len(messages), 3)
        self.assertGreater(len(self.connector._receive_buffer), 0)

    def test_single_partial_returns_empty(self):
        self.connector._receive_buffer = b"8=FIX.4.4\x019=25\x0135=D\x0149=TEST"
        messages = self.connector.parse_server_response()
        self.assertEqual(messages, [])

    def test_incremental_buffer_accumulation(self):
        full_msg = self._make_msg("A", "1")
        mid = len(full_msg) // 2

        self.connector._receive_buffer = full_msg[:mid]
        messages = self.connector.parse_server_response()
        self.assertEqual(messages, [])
        self.assertGreater(len(self.connector._receive_buffer), 0)

        self.connector._receive_buffer += full_msg[mid:]
        messages = self.connector.parse_server_response()
        self.assertEqual(len(messages), 1)
        self.assertEqual(self.connector._receive_buffer, b"")

    async def test_receive_messages_with_version_prefix(self):
        raw_data = b"8=FIX.4.4\x019=50\x0135=A\x0149=SPOT\x0156=TEST\x0134=1\x0152=20240101-12:00:00\x0110=123\x01"
        mock_reader = AsyncMock()
        mock_reader.read.side_effect = [raw_data, b""]
        self.connector._reader = mock_reader
        self.connector._writer = MagicMock()
        self.connector._writer.drain = AsyncMock()
        self.connector.is_connected = True

        await self.connector._receive_messages()
        messages = await self.connector.get_all_new_messages_received()
        self.assertGreater(len(messages), 0)

    async def test_receive_messages_incomplete_buffered(self):
        incomplete_data = b"8=FIX.4.4\x019=100\x0135=A\x01"
        mock_reader = AsyncMock()
        mock_reader.read.side_effect = [incomplete_data, b""]
        self.connector._reader = mock_reader
        self.connector._writer = MagicMock()
        self.connector._writer.drain = AsyncMock()
        self.connector.is_connected = True

        await self.connector._receive_messages()
        self.assertIsNotNone(self.connector._receive_buffer)


class TestOnMessageReceivedHandlers(FixConnectorTestBase):
    """Test on_message_received for News, Logout, and TestRequest edge cases."""

    def setUp(self):
        super().setUp()
        self.connector._writer = self.mock_writer
        self.connector.is_connected = True

    def _make_fix_msg(self, pairs):
        msg = FixMessage()
        for tag, val in pairs:
            msg.append_pair(tag, val)
        return msg

    async def test_news_message_triggers_schedule_restart(self):
        msg = self._make_fix_msg(
            [
                (35, FixMsgTypes.NEWS),
                (148, "Server will restart"),
            ]
        )
        with patch.object(self.connector, "schedule_restart", new_callable=AsyncMock) as mock_restart:
            await self.connector.on_message_received([msg])
            mock_restart.assert_called_once()

    async def test_news_message_no_restart_when_disabled(self):
        self.connector.restart = False
        msg = self._make_fix_msg(
            [
                (35, FixMsgTypes.NEWS),
                (148, "Server will restart"),
            ]
        )
        with patch.object(self.connector, "schedule_restart", new_callable=AsyncMock) as mock_restart:
            await self.connector.on_message_received([msg])
            mock_restart.assert_not_called()

    async def test_logout_message_disconnects_when_restart_disabled(self):
        self.connector.restart = False
        msg = self._make_fix_msg([(35, FixMsgTypes.LOGOUT)])
        with (
            patch.object(self.connector, "logout", new_callable=AsyncMock) as mock_logout,
            patch.object(self.connector, "disconnect", new_callable=AsyncMock) as mock_disconnect,
        ):
            await self.connector.on_message_received([msg])
            mock_logout.assert_called_once()
            mock_disconnect.assert_called_once()

    async def test_logout_suppressed_during_pending_restart(self):
        self.connector.restart = True
        self.connector._restart_flag = True
        msg = self._make_fix_msg([(35, FixMsgTypes.LOGOUT)])
        with (
            patch.object(self.connector, "logout", new_callable=AsyncMock) as mock_logout,
            patch.object(self.connector, "disconnect", new_callable=AsyncMock) as mock_disconnect,
        ):
            await self.connector.on_message_received([msg])
            mock_logout.assert_not_called()
            mock_disconnect.assert_not_called()

    async def test_logout_handled_when_restart_enabled_no_flag(self):
        self.connector.restart = True
        self.connector._restart_flag = False
        msg = self._make_fix_msg([(35, FixMsgTypes.LOGOUT)])
        with (
            patch.object(self.connector, "logout", new_callable=AsyncMock) as mock_logout,
            patch.object(self.connector, "disconnect", new_callable=AsyncMock) as mock_disconnect,
        ):
            await self.connector.on_message_received([msg])
            mock_logout.assert_called_once()
            mock_disconnect.assert_called_once()

    async def test_logout_ack_after_client_logout_only_disconnects(self):
        self.connector._logout_sent = True
        msg = self._make_fix_msg([(35, FixMsgTypes.LOGOUT)])
        with (
            patch.object(self.connector, "logout", new_callable=AsyncMock) as mock_logout,
            patch.object(self.connector, "disconnect", new_callable=AsyncMock) as mock_disconnect,
        ):
            await self.connector.on_message_received([msg])
            mock_logout.assert_not_called()
            mock_disconnect.assert_called_once()

    async def test_test_request_missing_id_continues(self):
        """Verify that a TestRequest without TestReqID doesn't prevent processing of subsequent messages."""
        bad_test_req = self._make_fix_msg([(35, FixMsgTypes.TEST_REQUEST)])
        news_msg = self._make_fix_msg(
            [
                (35, FixMsgTypes.NEWS),
                (148, "Restart notice"),
            ]
        )
        with (
            patch.object(self.connector, "heartbeat", new_callable=AsyncMock) as mock_hb,
            patch.object(self.connector, "schedule_restart", new_callable=AsyncMock) as mock_restart,
        ):
            await self.connector.on_message_received([bad_test_req, news_msg])
            mock_hb.assert_not_called()
            mock_restart.assert_called_once()

    async def test_test_request_with_id_sends_heartbeat(self):
        msg = self._make_fix_msg(
            [
                (35, FixMsgTypes.TEST_REQUEST),
                (112, "REQ123"),
            ]
        )
        with patch.object(self.connector, "heartbeat", new_callable=AsyncMock) as mock_hb:
            await self.connector.on_message_received([msg])
            mock_hb.assert_called_once_with("REQ123")


class TestRetrieveMessagesClOrdId(FixConnectorTestBase):
    """Test retrieve_messages_until with message_cl_ord_id matching."""

    async def test_match_by_cl_ord_id(self):
        target_id = "ORD_001"
        msg_other = FixMessage()
        msg_other.append_pair(35, "8")
        msg_other.append_pair(11, "ORD_999")

        msg_target = FixMessage()
        msg_target.append_pair(35, "8")
        msg_target.append_pair(11, target_id)

        await self.connector.on_message_received([msg_other])
        await self.connector.on_message_received([msg_target])

        result = await self.connector.retrieve_messages_until(
            message_type="8",
            message_cl_ord_id=target_id,
            timeout_seconds=1,
        )
        self.assertEqual(len(result), 2)
        cl_ord = result[-1].get(11)
        self.assertIsNotNone(cl_ord)
        self.assertEqual(cl_ord.decode("utf-8"), target_id)

    async def test_cl_ord_id_timeout_returns_partial(self):
        msg = FixMessage()
        msg.append_pair(35, "8")
        msg.append_pair(11, "ORD_WRONG")
        await self.connector.on_message_received([msg])

        result = await self.connector.retrieve_messages_until(
            message_type="8",
            message_cl_ord_id="ORD_NONEXISTENT",
            timeout_seconds=1,
        )
        self.assertEqual(len(result), 1)

    async def test_cl_ord_id_still_requires_message_type_match(self):
        target_id = "ORD_001"
        cancel_reject = FixMessage()
        cancel_reject.append_pair(35, "9")
        cancel_reject.append_pair(11, target_id)

        execution_report = FixMessage()
        execution_report.append_pair(35, "8")
        execution_report.append_pair(11, target_id)

        await self.connector.on_message_received([cancel_reject])
        await self.connector.on_message_received([execution_report])

        result = await self.connector.retrieve_messages_until(
            message_type="8",
            message_cl_ord_id=target_id,
            timeout_seconds=1,
        )
        self.assertEqual(result[-1], execution_report)


class TestLogonGuards(FixConnectorTestBase):
    """Test logon guard conditions."""

    def setUp(self):
        super().setUp()
        self.connector._writer = self.mock_writer
        self.connector.is_connected = True

    async def test_logon_blocked_when_restart_flag_set(self):
        self.connector._restart_flag = True
        initial_seq = self.connector.msg_seq_num

        await self.connector.logon()

        self.assertEqual(self.connector.msg_seq_num, initial_seq)
        self.mock_writer.write.assert_not_called()

    async def test_logon_raises_when_not_connected(self):
        self.connector.is_connected = False
        with self.assertRaises(ConnectionError):
            await self.connector.logon()

    async def test_logon_raises_when_writer_none(self):
        self.connector._writer = None
        with self.assertRaises(ConnectionError):
            await self.connector.logon()


class TestProtocolMessageContent(FixConnectorTestBase):
    """Test that protocol messages contain the correct FIX tags."""

    def setUp(self):
        super().setUp()
        self.connector._writer = self.mock_writer
        self.connector.is_connected = True

    def _get_sent_bytes(self):
        return self.mock_writer.write.call_args[0][0]

    def _parse_tags(self, raw_bytes):
        """Parse raw FIX bytes into a dict of tag->value."""
        parts = raw_bytes.decode("utf-8").replace("\x01", "|").split("|")
        tags = {}
        for part in parts:
            if "=" in part:
                tag, _, val = part.partition("=")
                tags[tag] = val
        return tags

    @patch.object(BinanceFixConnector, "current_utc_time")
    async def test_logon_contains_required_tags(self, mock_time):
        mock_time.return_value = "20250301-01:00:00.000000"
        await self.connector.logon(recv_window="5000")

        tags = self._parse_tags(self._get_sent_bytes())
        self.assertEqual(tags["35"], "A")
        self.assertIn("98", tags)
        self.assertIn("108", tags)
        self.assertEqual(tags["108"], str(self.connector.heart_bt_int))
        self.assertIn("96", tags)
        self.assertIn("553", tags)
        self.assertEqual(tags["553"], self.connector.api_key)
        self.assertIn("141", tags)
        self.assertIn("25035", tags)

    @patch.object(BinanceFixConnector, "current_utc_time")
    async def test_logout_contains_text(self, mock_time):
        mock_time.return_value = "20250301-01:00:00.000000"
        await self.connector.logout(text="Goodbye")

        tags = self._parse_tags(self._get_sent_bytes())
        self.assertEqual(tags["35"], "5")
        self.assertEqual(tags["58"], "Goodbye")
        self.assertTrue(self.connector._logout_sent)

    @patch.object(BinanceFixConnector, "current_utc_time")
    async def test_heartbeat_echoes_test_req_id(self, mock_time):
        mock_time.return_value = "20250301-01:00:00.000000"
        await self.connector.heartbeat(test_req_id="PING42")

        tags = self._parse_tags(self._get_sent_bytes())
        self.assertEqual(tags["35"], "0")
        self.assertEqual(tags["112"], "PING42")

    @patch.object(BinanceFixConnector, "current_utc_time")
    async def test_test_request_contains_id(self, mock_time):
        mock_time.return_value = "20250301-01:00:00.000000"
        await self.connector.test_request(test_req_id="TR_001")

        tags = self._parse_tags(self._get_sent_bytes())
        self.assertEqual(tags["35"], "1")
        self.assertEqual(tags["112"], "TR_001")


class TestConnectionLeakOnLogonFailure(FixConnectorTestBase):
    """Verify that _create_session cleans up on logon failure."""

    @patch("binance_fix_connector_async.fix_connector.asyncio.open_connection")
    @patch("binance_fix_connector_async.fix_connector.asyncio.create_task")
    async def test_failed_logon_disconnects(self, mock_create_task, mock_open_conn):
        mock_reader = AsyncMock()
        mock_writer = MagicMock()
        mock_writer.drain = AsyncMock()
        mock_writer.close = MagicMock()
        mock_writer.wait_closed = AsyncMock()
        mock_writer.write = MagicMock(side_effect=ConnectionResetError("reset"))
        mock_open_conn.return_value = (mock_reader, mock_writer)

        mock_task = _mock_task(done=True, cancelled=False)
        mock_create_task.side_effect = _close_created_coroutine_and_return(mock_task)

        from binance_fix_connector_async.fix_connector import create_order_entry_session

        with self.assertRaises(ConnectionResetError):
            await create_order_entry_session(
                api_key=self.valid_api_key,
                private_key=self.valid_private_key,
                endpoint=self.valid_endpoint,
            )

        mock_writer.close.assert_called()


class TestSendMessageWriterRace(FixConnectorTestBase):
    """Test that send_message is safe against concurrent disconnect."""

    async def test_send_message_no_writer_raises(self):
        connector = self.create_connector()
        connector._writer = None
        msg = self.create_fix_message()

        with self.assertRaises(ConnectionError):
            await connector.send_message(msg)
        self.assertEqual(len(connector.messages_sent), 0)

    async def test_send_message_captures_writer_under_lock(self):
        connector = self.create_connector()
        connector._writer = self.mock_writer
        connector.is_connected = True
        msg = self.create_fix_message()

        await connector.send_message(msg)

        self.mock_writer.write.assert_called_once()
        self.assertEqual(len(connector.messages_sent), 1)

    async def test_send_message_failed_write_does_not_commit_sequence(self):
        connector = self.create_connector()
        mock_writer = MagicMock()
        mock_writer.write = MagicMock(side_effect=OSError("write failed"))
        mock_writer.drain = AsyncMock()
        connector._writer = mock_writer
        connector.is_connected = True
        msg = await connector.create_fix_message_with_basic_header("D")
        initial_seq = connector.msg_seq_num
        initial_header_seq = msg.get(FixTags.MSG_SEQ_NUM).decode("utf-8")

        with self.assertRaises(OSError):
            await connector.send_message(msg)

        self.assertEqual(connector.msg_seq_num, initial_seq)
        self.assertEqual(msg.get(FixTags.MSG_SEQ_NUM).decode("utf-8"), initial_header_seq)
        self.assertEqual(len(connector.messages_sent), 0)


class TestScheduleRestart(FixConnectorTestBase):
    """Test schedule_restart idempotency and task management."""

    async def test_schedule_restart_idempotent(self):
        with patch("binance_fix_connector_async.fix_connector.asyncio.create_task") as mock_task:
            task = _mock_task()
            mock_task.side_effect = _close_created_coroutine_and_return(task)

            await self.connector.schedule_restart()
            self.assertTrue(self.connector._restart_flag)
            self.assertEqual(mock_task.call_count, 1)

            await self.connector.schedule_restart()
            self.assertEqual(mock_task.call_count, 1)

    async def test_schedule_restart_sets_flag_and_time(self):
        with patch("binance_fix_connector_async.fix_connector.asyncio.create_task") as mock_task:
            task = _mock_task()
            mock_task.side_effect = _close_created_coroutine_and_return(task)

            await self.connector.schedule_restart()
            self.assertTrue(self.connector._restart_flag)

    async def test_disconnect_cancels_pending_restart(self):
        self.connector.reconnect = AsyncMock()

        await self.connector.schedule_restart()
        await self.connector.disconnect()
        await asyncio.sleep(0)

        self.connector.reconnect.assert_not_awaited()
        self.assertFalse(self.connector._restart_flag)
        self.assertIsNone(self.connector._restart_task)


class TestReconnect(FixConnectorTestBase):
    """Test reconnect error handling and cleanup."""

    async def test_reconnect_noop_when_no_restart_flag(self):
        self.connector._restart_flag = False
        await self.connector.reconnect()
        self.assertFalse(self.connector.is_connected)

    async def test_reconnect_clears_flag_on_failure(self):
        self.connector._restart_flag = True

        with (
            patch.object(self.connector, "disconnect", new_callable=AsyncMock),
            patch("binance_fix_connector_async.fix_connector.BinanceFixConnector") as MockConn,
        ):
            mock_session = AsyncMock()
            mock_session.connect = AsyncMock(side_effect=ConnectionError("fail"))
            mock_session.disconnect = AsyncMock()
            mock_session._connection_params = MagicMock(return_value=self.connector._connection_params())
            MockConn.return_value = mock_session

            with self.assertRaises(ConnectionError):
                await self.connector.reconnect()

        self.assertFalse(self.connector._restart_flag)

    async def test_reconnect_logs_on_new_session(self):
        self.connector._restart_flag = True
        logon_msg = FixMessage()
        logon_msg.append_pair(FixTags.MSG_TYPE, FixMsgTypes.LOGON)

        new_session = self.create_connector()

        async def logon_side_effect():
            self.assertFalse(new_session._restart_flag)
            await new_session.on_message_received([logon_msg])

        new_session.connect = AsyncMock()
        new_session.logon = AsyncMock(side_effect=logon_side_effect)
        new_session.disconnect = AsyncMock()
        new_session.is_connected = True

        with (
            patch.object(self.connector, "disconnect", new_callable=AsyncMock),
            patch("binance_fix_connector_async.fix_connector.asyncio.sleep", new_callable=AsyncMock),
            patch("binance_fix_connector_async.fix_connector.BinanceFixConnector", return_value=new_session),
        ):
            await self.connector.reconnect()

        new_session.logon.assert_awaited_once()
        self.assertFalse(self.connector._restart_flag)


class TestAPIStructure(FixConnectorTestBase):
    """Test that the module exposes the expected public API surface."""

    def test_module_constants_exist(self):
        import binance_fix_connector_async.fix_connector as mod

        for name in [
            "MAX_BUFFER_SIZE",
            "MAX_SENDER_ID_LENGTH",
            "MIN_FIX_MESSAGE_LENGTH",
            "TRAILER_SIZE",
            "FIX_MD_URL",
            "FIX_OE_URL",
            "FIX_DC_URL",
        ]:
            with self.subTest(constant=name):
                self.assertTrue(hasattr(mod, name), f"Missing constant: {name}")

    def test_fix_classes_exist(self):
        import binance_fix_connector_async.fix_connector as mod

        for msg_type in ["LOGON", "LOGOUT", "HEARTBEAT", "NEW_ORDER_SINGLE"]:
            with self.subTest(msg_type=msg_type):
                self.assertTrue(hasattr(mod.FixMsgTypes, msg_type))
        for tag in ["MSG_TYPE", "SENDER_COMP_ID", "TARGET_COMP_ID", "MSG_SEQ_NUM"]:
            with self.subTest(tag=tag):
                self.assertTrue(hasattr(mod.FixTags, tag))

    def test_factory_function_signatures(self):
        import inspect

        import binance_fix_connector_async.fix_connector as mod

        for func_name in ["create_market_data_session", "create_order_entry_session", "create_drop_copy_session"]:
            with self.subTest(function=func_name):
                sig = inspect.signature(getattr(mod, func_name))
                for param in ["api_key", "private_key"]:
                    self.assertIn(param, sig.parameters)

    def test_connector_public_methods_exist(self):
        for method_name in [
            "connect",
            "disconnect",
            "logon",
            "logout",
            "heartbeat",
            "send_message",
            "get_all_new_messages_received",
            "retrieve_messages_until",
            "current_utc_time",
            "generate_signature",
            "parse_server_response",
        ]:
            with self.subTest(method=method_name):
                self.assertTrue(hasattr(BinanceFixConnector, method_name))
                self.assertTrue(callable(getattr(BinanceFixConnector, method_name)))


class TestFixSchemaCompliance(FixConnectorTestBase):
    """Test declared message-type and encoded FIX envelope contracts."""

    def assert_valid_encoded_envelope(self, encoded: bytes, msg_type: str, seq_num: str) -> None:
        pairs = _encoded_pairs(encoded)
        tags = dict(pairs)

        self.assertGreaterEqual(len(pairs), 8)
        self.assertEqual([tag for tag, _value in pairs[:3]], ["8", "9", "35"])
        self.assertEqual(pairs[-1][0], "10")
        self.assertEqual(tags[FixTags.BEGIN_STRING], self.connector.fix_version)
        self.assertEqual(tags[FixTags.MSG_TYPE], msg_type)
        self.assertEqual(tags[FixTags.SENDER_COMP_ID], self.valid_sender_comp_id)
        self.assertEqual(tags[FixTags.TARGET_COMP_ID], self.valid_target_comp_id)
        self.assertEqual(tags[FixTags.MSG_SEQ_NUM], seq_num)
        datetime.strptime(tags[FixTags.SENDING_TIME], "%Y%m%d-%H:%M:%S.%f")
        self.assertEqual(int(tags[FixTags.BODY_LENGTH]), _fix_body_length(encoded))
        self.assertRegex(tags[FixTags.CHECKSUM], r"^\d{3}$")
        self.assertEqual(tags[FixTags.CHECKSUM], _fix_checksum(encoded))

    def test_declared_fix_message_types_are_pinned_unique_ascii_values(self):
        actual = {name: value for name, value in vars(FixMsgTypes).items() if name.isupper()}

        self.assertEqual(actual, EXPECTED_FIX_MSG_TYPES)
        self.assertEqual(len(set(actual.values())), len(actual))
        for name, value in actual.items():
            with self.subTest(message_type=name):
                self.assertIsInstance(value, str)
                self.assertTrue(value)
                self.assertTrue(value.isascii())

    async def test_all_declared_message_types_encode_valid_basic_header_envelope(self):
        for name, msg_type in EXPECTED_FIX_MSG_TYPES.items():
            with self.subTest(message_type=name):
                msg = await self.connector.create_fix_message_with_basic_header(msg_type, "5000")
                encoded = msg.encode()
                tags = _encoded_tag_values(encoded)

                self.assert_valid_encoded_envelope(encoded, msg_type, "0")
                self.assertEqual(tags[FixTags.RECV_WINDOW], "5000")

    async def test_all_declared_message_types_send_with_valid_sequence_envelope(self):
        mock_writer = MagicMock()
        mock_writer.write = MagicMock()
        mock_writer.drain = AsyncMock()
        self.connector._writer = mock_writer
        self.connector.is_connected = True

        for expected_seq, (name, msg_type) in enumerate(EXPECTED_FIX_MSG_TYPES.items(), start=2):
            with self.subTest(message_type=name):
                msg = await self.connector.create_fix_message_with_basic_header(msg_type)
                await self.connector.send_message(msg)
                encoded = mock_writer.write.call_args[0][0]

                self.assert_valid_encoded_envelope(encoded, msg_type, str(expected_seq))
                self.assertEqual(msg.get(FixTags.MSG_SEQ_NUM).decode("utf-8"), str(expected_seq))

        self.assertEqual(self.connector.msg_seq_num, len(EXPECTED_FIX_MSG_TYPES) + 1)
        self.assertEqual(len(self.connector.messages_sent), len(EXPECTED_FIX_MSG_TYPES))

    @patch.object(BinanceFixConnector, "current_utc_time")
    async def test_logon_message_auth_schema_is_consistent(self, mock_time):
        mock_time.return_value = "20250301-01:00:00.000000"
        self.connector._writer = self.mock_writer
        self.connector.is_connected = True

        await self.connector.logon(recv_window="5000")

        tags = _encoded_tag_values(self.mock_writer.write.call_args[0][0])
        for tag in [
            FixTags.ENCRYPT_METHOD,
            FixTags.HEART_BT_INT,
            FixTags.RAW_DATA_LENGTH,
            FixTags.RAW_DATA,
            FixTags.RESET_SEQ_NUM_FLAG,
            FixTags.USERNAME,
            FixTags.MESSAGE_HANDLING,
            FixTags.RESPONSE_MODE,
            FixTags.RECV_WINDOW,
        ]:
            with self.subTest(tag=tag):
                self.assertIn(tag, tags)
        self.assertEqual(tags[FixTags.MSG_TYPE], FixMsgTypes.LOGON)
        self.assertEqual(tags[FixTags.RECV_WINDOW], "5000")
        self.assertEqual(int(tags[FixTags.RAW_DATA_LENGTH]), len(tags[FixTags.RAW_DATA]))


if __name__ == "__main__":
    pytest.main([__file__])
