#!/usr/bin/env python3
"""
Binance FIX Connector - High-Performance Async Implementation.

This module provides async/await support for high-performance FIX protocol
communication with Binance SPOT trading services.
"""

from __future__ import annotations

import asyncio
import base64
import logging
import ssl
import time
from collections import deque
from contextlib import suppress
from datetime import UTC, datetime
from typing import TYPE_CHECKING, NamedTuple
from urllib.parse import urlparse

from simplefix import FixMessage

from .utils import SessionType, check_fix_api_permissions, validate_fix_permissions_for_session

if TYPE_CHECKING:
    from cryptography.hazmat.primitives.asymmetric import ed25519

_SOH_ = "\x01"
MAX_BUFFER_SIZE = 4096
MAX_SENDER_ID_LENGTH = 8
MIN_FIX_MESSAGE_LENGTH = 37
TRAILER_SIZE = 6
FIX_MD_URL = "tcp+tls://fix-md.binance.com:9000"
FIX_OE_URL = "tcp+tls://fix-oe.binance.com:9000"
FIX_DC_URL = "tcp+tls://fix-dc.binance.com:9000"

SENSITIVE_TAGS = frozenset({"96", "553", "554"})

DEFAULT_HEARTBEAT_INTERVAL = 30
MIN_HEARTBEAT_INTERVAL = 5
MAX_HEARTBEAT_INTERVAL = 60
POST_DISCONNECT_DELAY_SECONDS = 1
RECONNECT_LOGON_TIMEOUT_SECONDS = 30
SESSION_CONNECT_TIMEOUT_SECONDS = 30
MAX_SENT_MESSAGES_HISTORY = 10_000
MAX_MESSAGE_HISTORY_SIZE = 10_000

logger = logging.getLogger("binance_fix_connector_async")
logger.addHandler(logging.NullHandler())


class FixMsgTypes:
    # Admin
    HEARTBEAT = "0"
    TEST_REQUEST = "1"
    REJECT = "3"
    LOGOUT = "5"
    LOGON = "A"
    NEWS = "B"
    # Order Entry
    NEW_ORDER_SINGLE = "D"
    NEW_ORDER_LIST = "E"
    ORDER_CANCEL_REQUEST = "F"
    LIST_STATUS = "N"
    EXECUTION_REPORT = "8"
    ORDER_CANCEL_REJECT = "9"
    ORDER_MASS_CANCEL_REQUEST = "q"
    ORDER_MASS_CANCEL_REPORT = "r"
    # Market Data
    MARKET_DATA_REQUEST = "V"
    MARKET_DATA_SNAPSHOT = "W"
    MARKET_DATA_INCREMENTAL_REFRESH = "X"
    MARKET_DATA_REQUEST_REJECT = "Y"
    INSTRUMENT_LIST_REQUEST = "x"
    INSTRUMENT_LIST = "y"
    # Binance Extensions
    LIMIT_QUERY = "XLQ"
    LIMIT_RESPONSE = "XLR"
    ORDER_CANCEL_REPLACE_REQUEST = "XCN"
    ORDER_AMEND_KEEP_PRIORITY_REQUEST = "XAK"
    ORDER_AMEND_REJECT = "XAR"


class FixTags:
    # Header / Admin (sorted by numeric value)
    BEGIN_STRING = "8"
    BODY_LENGTH = "9"
    CHECKSUM = "10"
    CL_ORD_ID = "11"
    MSG_SEQ_NUM = "34"
    MSG_TYPE = "35"
    ORDER_ID = "37"
    QUANTITY = "38"
    ORD_STATUS = "39"
    ORD_TYPE = "40"
    ORIG_CL_ORD_ID = "41"
    PRICE = "44"
    SENDER_COMP_ID = "49"
    SENDING_TIME = "52"
    SIDE = "54"
    SYMBOL = "55"
    TARGET_COMP_ID = "56"
    TEXT = "58"
    TIME_IN_FORCE = "59"
    TRANSACT_TIME = "60"
    LIST_ID = "66"
    LIST_SEQ_NO = "67"
    TOT_NO_ORDERS = "68"
    RAW_DATA_LENGTH = "95"
    RAW_DATA = "96"
    ENCRYPT_METHOD = "98"
    STOP_PX = "99"
    ORD_REJ_REASON = "103"
    HEART_BT_INT = "108"
    TEST_REQ_ID = "112"
    RESET_SEQ_NUM_FLAG = "141"
    NEWS_TEXT = "148"
    EXEC_TYPE = "150"
    LEAVES_QTY = "151"
    # Market Data
    MD_REQ_ID = "262"
    SUBSCRIPTION_REQUEST_TYPE = "263"
    MARKET_DEPTH = "264"
    NO_MD_ENTRY_TYPES = "267"
    NO_MD_ENTRIES = "268"
    MD_ENTRY_TYPE = "269"
    SECURITY_LIST_REQUEST_ID = "320"
    # List / Order
    LIST_ORDER_STATUS = "394"
    LIST_STATUS_TYPE = "431"
    USERNAME = "553"
    DROP_COPY_FLAG = "9406"
    # Binance Extensions
    ERROR_CODE = "25016"
    RECV_WINDOW = "25000"
    MESSAGE_HANDLING = "25035"
    RESPONSE_MODE = "25036"


def _build_sender_comp_id(prefix: str, sender_comp_id: str) -> str:
    return (prefix + sender_comp_id)[:MAX_SENDER_ID_LENGTH]


def _sanitize_fix_message(raw: str) -> str:
    parts = raw.replace(_SOH_, "|").split("|")
    sanitized = []
    for part in parts:
        if "=" in part:
            tag, _, _ = part.partition("=")
            if tag in SENSITIVE_TAGS:
                sanitized.append(f"{tag}=***")
                continue
        sanitized.append(part)
    return "|".join(sanitized)


def _append_bounded_message_history(
    message_history: list[FixMessage],
    msg: FixMessage,
    get_all_cursor: int,
    retrieve_cursor: int,
) -> tuple[int, int]:
    message_history.append(msg)
    if len(message_history) <= MAX_MESSAGE_HISTORY_SIZE:
        return get_all_cursor, retrieve_cursor

    message_history.pop(0)
    return max(0, get_all_cursor - 1), max(0, retrieve_cursor - 1)


class _SessionConfig(NamedTuple):
    default_endpoint: str
    prefix: str
    default_response_mode: int | None
    drop_copy_flag: str | None


_SESSION_DEFAULTS: dict[SessionType, _SessionConfig] = {
    SessionType.MARKET_DATA: _SessionConfig(FIX_MD_URL, "BMD", None, None),
    SessionType.ORDER_ENTRY: _SessionConfig(FIX_OE_URL, "BOE", 1, "N"),
    SessionType.DROP_COPY: _SessionConfig(FIX_DC_URL, "BDC", 1, "Y"),
}


async def _create_session(
    session_type: SessionType,
    api_key: str,
    private_key: ed25519.Ed25519PrivateKey,
    endpoint: str | None = None,
    sender_comp_id: str = "",
    target_comp_id: str = "SPOT",
    fix_version: str = "FIX.4.4",
    heart_bt_int: int = DEFAULT_HEARTBEAT_INTERVAL,
    message_handling: int = 2,
    response_mode: int | None = None,
    recv_window: int | None = None,
) -> BinanceFixConnector:
    default_endpoint, prefix, default_response_mode, drop_copy_flag = _SESSION_DEFAULTS[session_type]
    session = BinanceFixConnector(
        endpoint=endpoint or default_endpoint,
        api_key=api_key,
        private_key=private_key,
        sender_comp_id=_build_sender_comp_id(prefix, sender_comp_id),
        target_comp_id=target_comp_id,
        fix_version=fix_version,
        heart_bt_int=heart_bt_int,
        socket_buffer_size=MAX_BUFFER_SIZE,
        reset_seq_num_flag="Y",
        encrypt_method=0,
        message_handling=message_handling,
        response_mode=response_mode if response_mode is not None else default_response_mode,
        drop_copy_flag=drop_copy_flag,
        restart=True,
    )
    try:
        await asyncio.wait_for(session.connect(), timeout=SESSION_CONNECT_TIMEOUT_SECONDS)
        await asyncio.wait_for(
            session.logon(recv_window=str(recv_window) if recv_window is not None else None),
            timeout=SESSION_CONNECT_TIMEOUT_SECONDS,
        )
        responses = await asyncio.wait_for(
            session.retrieve_messages_until(
                message_type=[
                    FixMsgTypes.LOGON,
                    FixMsgTypes.REJECT,
                    FixMsgTypes.LOGOUT,
                ],
                timeout_seconds=SESSION_CONNECT_TIMEOUT_SECONDS,
            ),
            timeout=SESSION_CONNECT_TIMEOUT_SECONDS + 1,
        )
        msg_type = next(
            (
                session._decode(msg.get(FixTags.MSG_TYPE))
                for msg in responses
                if session._decode(msg.get(FixTags.MSG_TYPE))
                in {FixMsgTypes.LOGON, FixMsgTypes.REJECT, FixMsgTypes.LOGOUT}
            ),
            None,
        )
        session._retrieve_cursor = 0
        session._get_all_cursor = len(session._message_history)
        if msg_type != FixMsgTypes.LOGON:
            raise ConnectionError(f"FIX logon failed: received {msg_type or 'no response'}")
    except Exception:
        await session.disconnect()
        raise
    return session


async def create_market_data_session(
    api_key: str,
    private_key: ed25519.Ed25519PrivateKey,
    endpoint: str = FIX_MD_URL,
    sender_comp_id: str = "WATCH",
    target_comp_id: str = "SPOT",
    fix_version: str = "FIX.4.4",
    heart_bt_int: int = DEFAULT_HEARTBEAT_INTERVAL,
    message_handling: int = 2,
    recv_window: int | None = None,
) -> BinanceFixConnector:
    """
    Create an async session to the FIX market data service.

    Message handling:   1->UNORDERED
                        2->SEQUENTIAL
    """
    return await _create_session(
        SessionType.MARKET_DATA,
        api_key,
        private_key,
        endpoint,
        sender_comp_id,
        target_comp_id,
        fix_version,
        heart_bt_int,
        message_handling,
        recv_window=recv_window,
    )


async def create_order_entry_session(
    api_key: str,
    private_key: ed25519.Ed25519PrivateKey,
    endpoint: str = FIX_OE_URL,
    sender_comp_id: str = "TRADE",
    target_comp_id: str = "SPOT",
    fix_version: str = "FIX.4.4",
    heart_bt_int: int = DEFAULT_HEARTBEAT_INTERVAL,
    message_handling: int = 2,
    response_mode: int = 1,
    recv_window: int | None = None,
) -> BinanceFixConnector:
    """
    Create an async session to the FIX order-entry service.

    Response mode:  1->EVERYTHING
                    2->ONLY_ACKS
    Message handling:   1->UNORDERED
                        2->SEQUENTIAL
    """
    return await _create_session(
        SessionType.ORDER_ENTRY,
        api_key,
        private_key,
        endpoint,
        sender_comp_id,
        target_comp_id,
        fix_version,
        heart_bt_int,
        message_handling,
        response_mode,
        recv_window,
    )


async def create_drop_copy_session(
    api_key: str,
    private_key: ed25519.Ed25519PrivateKey,
    endpoint: str = FIX_DC_URL,
    sender_comp_id: str = "TECH",
    target_comp_id: str = "SPOT",
    fix_version: str = "FIX.4.4",
    heart_bt_int: int = DEFAULT_HEARTBEAT_INTERVAL,
    message_handling: int = 2,
    response_mode: int = 1,
    recv_window: int | None = None,
    check_permissions: bool = False,
    hmac_secret: str | None = None,
    permission_base_url: str = "https://api.binance.com",
) -> BinanceFixConnector:
    """
    Create an async session to the FIX drop-copy service.

    Response mode:  1->EVERYTHING
                    2->ONLY_ACKS
    Message handling:   1->UNORDERED
                        2->SEQUENTIAL
    """
    if check_permissions:
        if not hmac_secret:
            raise ValueError("hmac_secret is required when check_permissions=True")

        try:
            permissions = await check_fix_api_permissions(api_key, hmac_secret, permission_base_url)
            is_valid, error_msg = validate_fix_permissions_for_session(permissions, "drop_copy")

            if not is_valid:
                raise ValueError(
                    f"Permission check failed: {error_msg}. "
                    f"Permissions found: FIX_API={permissions['has_fix_api']}, "
                    f"FIX_API_READ_ONLY={permissions['has_fix_api_read_only']}"
                )
        except (KeyError, OSError) as e:
            logger.warning("Permission check failed: %s. Proceeding with connection attempt.", e)

    return await _create_session(
        SessionType.DROP_COPY,
        api_key,
        private_key,
        endpoint,
        sender_comp_id,
        target_comp_id,
        fix_version,
        heart_bt_int,
        message_handling,
        response_mode,
        recv_window,
    )


class BinanceFixConnector:
    """
    Binance FIX Connector.

    Provides async/await interfaces for FIX protocol communication
    with high-performance asyncio-based message handling.
    """

    def __init__(
        self,
        endpoint: str,
        api_key: str,
        private_key: ed25519.Ed25519PrivateKey,
        sender_comp_id: str,
        *,
        target_comp_id: str = "SPOT",
        fix_version: str = "FIX.4.4",
        socket_buffer_size: int = MAX_BUFFER_SIZE,
        heart_bt_int: int = DEFAULT_HEARTBEAT_INTERVAL,
        reset_seq_num_flag: str = "Y",
        encrypt_method: int = 0,
        message_handling: int = 2,
        response_mode: int | None = 1,
        drop_copy_flag: str | None = None,
        restart: bool = True,
    ) -> None:
        """
        Create an async FIX session.

        Args:
        ----
            endpoint (str): The server endpoint
            api_key (str): The api key registered for the user
            private_key (ed25519.Ed25519PrivateKey): the Ed25519 private key used to register the api key
            sender_comp_id (str): the sender id (client)
            target_comp_id (str, optional): The target id (server). Defaults to "SPOT".
            fix_version (str, optional): The fix version protocol used. Defaults to "FIX.4.4".
            socket_buffer_size (int, optional): The socket buffer when receiving messages from server. Defaults to 4096.
            heart_bt_int (int, optional): The heartbeat interval. Defaults to 30
            reset_seq_num_flag (str, optional): The reset seq num flag. Defaults to "Y".
            encrypt_method (int, optional): The encrypt method. Defaults to 0 (None).
            message_handling (int, optional): The message handling. Defaults to 2 (SEQUENTIAL).
            response_mode (int, optional): The response mode. Defaults to 1 (EVERYTHING).
            drop_copy_flag (str, optional): The drop copy flag. Defaults to None.
            restart (bool, optional): Whether to enable automatic session restart upon server notification. Defaults to True.

        Raises:
        ------
            ValueError: Raised when some mandatory arguments are not sent
        """
        self._validate_init_params(endpoint, api_key, private_key, sender_comp_id)
        self._validate_protocol_params(heart_bt_int, reset_seq_num_flag, encrypt_method)

        self.endpoint = endpoint
        self.api_key = api_key
        self.private_key = private_key
        self.sender_comp_id = str(sender_comp_id)
        self.target_comp_id = str(target_comp_id)
        self.fix_version = str(fix_version)

        self.heart_bt_int = heart_bt_int
        self.reset_seq_num_flag = reset_seq_num_flag
        self.encrypt_method = encrypt_method
        self.message_handling = message_handling
        self.response_mode = response_mode
        self.drop_copy_flag = drop_copy_flag

        self.socket_buffer_size: int = socket_buffer_size

        self._lock = asyncio.Lock()
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._receive_task: asyncio.Task | None = None
        self._liveness_task: asyncio.Task | None = None
        self.is_connected: bool = False
        self._logout_sent: bool = False

        self.msg_seq_num: int = 1
        self._message_history: list[FixMessage] = []
        self._get_all_cursor: int = 0
        self._retrieve_cursor: int = 0
        self._message_event = asyncio.Event()
        self.messages_sent: deque[FixMessage] = deque(maxlen=MAX_SENT_MESSAGES_HISTORY)

        self.restart: bool = restart
        self._restart_flag: bool = False
        self._restart_task: asyncio.Task | None = None

        self.logger = logging.getLogger(f"BinanceFixConnector.{self.sender_comp_id}")
        self._receive_buffer: bytes = b""
        self._last_received_at: float = time.monotonic()

    @staticmethod
    def _validate_init_params(
        endpoint: str,
        api_key: str,
        private_key: object,
        sender_comp_id: str,
    ) -> None:
        error_message = ""
        if not endpoint:
            error_message += "endpoint can not be None or empty\n"
        if not api_key:
            error_message += "api_key can not be None or empty\n"
        if not private_key:
            error_message += "private_key can not be None or empty\n"
        if not sender_comp_id:
            error_message += "sender_comp_id can not be None or empty\n"
        elif len(str(sender_comp_id)) > MAX_SENDER_ID_LENGTH:
            error_message += "sender_comp_id can not be longer than 8 characters\n"
        if error_message:
            raise ValueError(error_message)

    @staticmethod
    def _validate_protocol_params(
        heart_bt_int: int,
        reset_seq_num_flag: str,
        encrypt_method: int,
    ) -> None:
        error_message = ""
        if not isinstance(heart_bt_int, int) or not (MIN_HEARTBEAT_INTERVAL <= heart_bt_int <= MAX_HEARTBEAT_INTERVAL):
            error_message += "heart_bt_int must be an integer between 5 and 60\n"
        if str(reset_seq_num_flag) != "Y":
            error_message += "reset_seq_num_flag must be 'Y'\n"
        if encrypt_method != 0:
            error_message += "encrypt_method must be 0\n"
        if error_message:
            raise ValueError(error_message)

    @staticmethod
    def current_utc_time() -> str:
        """
        Return the current utc time which will be used for signature and fix message header.

        Returns
        -------
            - datetime in string format YYYYmmdd-HH:MM:SS.ffffff
        """
        return datetime.now(UTC).strftime("%Y%m%d-%H:%M:%S.%f")

    def generate_signature(
        self,
        sender_comp_id: str,
        target_comp_id: str,
        msg_seq_num: int,
        sending_time: str,
    ) -> str:
        """
        Generate the signature required to login in the server.

        Args:
        ----
            sender_comp_id (str): the sender comp id
            target_comp_id (str): the target comp id
            msg_seq_num (int): the msq seq num
            sending_time (str): the sending time

        Raises:
        ------
            ValueError: When the private key is not provided

        Returns:
        -------
            signed_signature: signature ready to be used.
        """
        if not self.private_key:
            msg = "Please provide an Ed25519 key"
            raise ValueError(msg)
        signed_headers = f"{FixMsgTypes.LOGON}{_SOH_}{sender_comp_id}{_SOH_}{target_comp_id}{_SOH_}{msg_seq_num}{_SOH_}{sending_time}"
        signature = self.private_key.sign(bytes(signed_headers, "ASCII"))
        return base64.b64encode(signature).decode("ASCII")

    @staticmethod
    def _try_parse_raw_message(raw_msg: bytes, fix_version: str) -> FixMessage | None:
        """Parse one raw FIX message into a FixMessage, or return None if incomplete."""
        tag_values = [x for x in raw_msg.split(b"\x01") if b"=" in x and not x.startswith(b"=")]
        if not tag_values or not (tag_values[-1].startswith(b"10=") and len(tag_values[-1]) >= TRAILER_SIZE):
            return None
        fix_msg = FixMessage()
        for tag_value in tag_values:
            tag, _, value = tag_value.partition(b"=")
            fix_msg.append_pair(int(tag), value)
        return fix_msg

    def parse_server_response(self) -> list[FixMessage]:
        """
        Parse the response from the server and create a fix message for every message server has sent.

        Returns
        -------
            list[FixMessage]: The list of (FIX) messages server has sent.
        """
        if len(self._receive_buffer) < MIN_FIX_MESSAGE_LENGTH:
            return []
        raw_messages = self._receive_buffer.split(b"\x018=")
        if self._receive_buffer.startswith(b"8="):
            raw_messages[1:] = [b"8=" + x for x in raw_messages[1:] if x]
        else:
            raw_messages = [b"8=" + x for x in raw_messages if x]
        messages: list[FixMessage] = []
        for i, raw_msg in enumerate(raw_messages):
            fix_msg = self._try_parse_raw_message(raw_msg, self.fix_version)
            if fix_msg is not None:
                messages.append(fix_msg)
            else:
                self._receive_buffer = b"\x01".join(raw_messages[i:])
                return messages

        self._receive_buffer = b""
        return messages

    async def connect(self) -> None:
        """Create an async socket connection between the client and the server."""
        self._receive_buffer = b""
        try:
            if self._writer:
                self._writer.close()
                await self._writer.wait_closed()
                self._writer = None
                self._reader = None

            url = urlparse(self.endpoint)
            context = ssl.create_default_context()

            self._reader, self._writer = await asyncio.open_connection(url.hostname, url.port, ssl=context)

            self.logger.info("FIX Client: Connected to %s", self.endpoint)
            self.logger.info("LOGIN (A)")
            self.is_connected = True
            self._logout_sent = False

            if self._receive_task is None or self._receive_task.done():
                self._receive_task = asyncio.create_task(self._receive_messages())
                self._receive_task.add_done_callback(self._handle_task_exception)
            if self._liveness_task is None or self._liveness_task.done():
                self._last_received_at = time.monotonic()
                self._liveness_task = asyncio.create_task(self._monitor_liveness())
                self._liveness_task.add_done_callback(self._handle_task_exception)

        except (TimeoutError, OSError, ssl.SSLError):
            self.logger.exception("Error connecting")
            raise

    def _log_received_messages(self, messages: list[FixMessage]) -> None:
        if not self.logger.isEnabledFor(logging.INFO):
            return
        for msg in messages:
            try:
                clean_message = _sanitize_fix_message(msg.encode().decode("utf-8"))
            except ValueError:
                self.logger.warning("Message decoded but could not be logged (missing MsgType: 35)")
                continue
            self.logger.info("Server=>Client: %s", clean_message)

    async def _receive_messages(self) -> None:
        """Async task to read data from server and process messages."""
        while self.is_connected and self._reader:
            try:
                data = await self._reader.read(self.socket_buffer_size)
                if not data:
                    self.logger.warning("Server closed the connection (EOF)")
                    self.is_connected = False
                    self._writer = None
                    self._reader = None
                    break
                self._last_received_at = time.monotonic()
                self._receive_buffer += data
                messages = self.parse_server_response()
                if messages:
                    self._log_received_messages(messages)
                    await self.on_message_received(messages)

            except asyncio.CancelledError:
                break
            except (OSError, ConnectionError):
                self.logger.exception("Error receiving message")
                await self.disconnect()
                raise

    def _handle_task_exception(self, task: asyncio.Task) -> None:
        """Handle exceptions from background tasks to prevent 'Task exception was never retrieved' warnings."""
        if task.cancelled():
            return

        exception = task.exception()
        if exception:
            self.logger.debug("Background task completed with exception: %s", exception)

    async def on_message_received(self, messages: list[FixMessage]) -> None:
        """
        Process every message received from server.

        Args:
        ----
            messages (list[FixMessage]): The messages to be processed
        """
        for msg in messages:
            self._remember_received_message(msg)
            msg_type = self._decode(msg.get(FixTags.MSG_TYPE))
            if msg_type == FixMsgTypes.TEST_REQUEST:
                await self._handle_test_request(msg)
            elif msg_type == FixMsgTypes.NEWS:
                await self._handle_news(msg)
            elif msg_type == FixMsgTypes.LOGOUT:
                if self._restart_flag:
                    self.logger.info("Logout received during pending restart, suppressing disconnect")
                else:
                    await self._handle_logout()

    @staticmethod
    def _decode(val: bytes | None, default: str | None = None) -> str | None:
        return val.decode("utf-8") if val else default

    def _remember_received_message(self, msg: FixMessage) -> None:
        self._get_all_cursor, self._retrieve_cursor = _append_bounded_message_history(
            self._message_history,
            msg,
            self._get_all_cursor,
            self._retrieve_cursor,
        )
        self._message_event.set()

    def _history_since(self, cursor: int) -> tuple[list[FixMessage], int]:
        messages = self._message_history[cursor:]
        return messages, len(self._message_history)

    async def _handle_test_request(self, message: FixMessage) -> None:
        test_req_resp_id = self._decode(message.get(FixTags.TEST_REQ_ID))
        if test_req_resp_id is None:
            self.logger.error("Error: TestReqID (112) not found in the message.")
            return
        self.logger.debug("Sending a heartbeat message as we received a TestRequest message from server")
        await self.heartbeat(test_req_resp_id)

    async def _monitor_liveness(self) -> None:
        """Send TestRequest when inbound traffic is quiet and close stale sessions."""
        while self.is_connected:
            try:
                await asyncio.sleep(self.heart_bt_int)
                if not self.is_connected or not self._writer:
                    return
                if time.monotonic() - self._last_received_at < self.heart_bt_int:
                    continue

                test_req_id = f"TR_{int(time.time() * 1000)}"
                await self.test_request(test_req_id)
                deadline = time.monotonic() + self.heart_bt_int
                while self.is_connected and time.monotonic() < deadline:
                    if self._has_heartbeat_response(test_req_id):
                        break
                    await asyncio.sleep(0.1)
                else:
                    self.logger.warning("Heartbeat response not received for TestReqID %s; disconnecting", test_req_id)
                    await self.disconnect()
                    return
            except asyncio.CancelledError:
                return
            except (OSError, ConnectionError):
                self.logger.exception("Liveness check failed")
                await self.disconnect()
                return

    def _has_heartbeat_response(self, test_req_id: str) -> bool:
        for msg in self._message_history:
            if (
                self._decode(msg.get(FixTags.MSG_TYPE)) == FixMsgTypes.HEARTBEAT
                and self._decode(msg.get(FixTags.TEST_REQ_ID)) == test_req_id
            ):
                return True
        return False

    async def _handle_news(self, message: FixMessage) -> None:
        self.logger.info("News message received from server.")
        news_text = self._decode(message.get(FixTags.NEWS_TEXT))
        self.logger.info("NewsText: %s", news_text)
        if self.restart:
            await self.schedule_restart()

    async def _handle_logout(self) -> None:
        self.logger.info("Logout message received from server. Closing connection.")
        if not self._logout_sent:
            await self.logout()
        await self.disconnect()

    async def get_all_new_messages_received(self) -> list[FixMessage]:
        """
        Return all the FIX messages received from the server until now.
        If no new messages received, it returns [].

        Returns
        -------
            list[FixMessage]: The list of fix messages received from server.
        """
        if self._get_all_cursor >= len(self._message_history):
            return []
        messages, self._get_all_cursor = self._history_since(self._get_all_cursor)
        return messages

    @staticmethod
    def _matches_target(
        msg: FixMessage,
        message_types: list[str],
        cl_ord_id: str | None,
    ) -> bool:
        """Check if a message matches the target criteria."""
        msg_type = BinanceFixConnector._decode(msg.get(FixTags.MSG_TYPE))
        if not (message_types and msg_type and msg_type in message_types):
            return False
        if cl_ord_id:
            return BinanceFixConnector._decode(msg.get(FixTags.CL_ORD_ID)) == cl_ord_id
        return True

    @staticmethod
    def _set_message_seq_num(message: FixMessage, seq_num: int) -> tuple[int, tuple[bytes, bytes]] | None:
        seq_bytes = str(seq_num).encode()
        for i, (tag, val) in enumerate(message.pairs):
            if tag == b"34":
                original_pair = (tag, val)
                message.pairs[i] = (tag, seq_bytes)
                return i, original_pair
        return None

    @staticmethod
    def _restore_message_pair(message: FixMessage, replacement: tuple[int, tuple[bytes, bytes]] | None) -> None:
        if replacement is None:
            return
        index, original_pair = replacement
        message.pairs[index] = original_pair

    async def retrieve_messages_until(
        self,
        message_type: str | list[str],
        message_cl_ord_id: str | None = None,
        timeout_seconds: float = 3,
    ) -> list[FixMessage]:
        """
        Return all the FIX messages received from the server until message of desired type is received.

        Args:
        ----
            message_type: Single message type string or list of message types to match.
            message_cl_ord_id: If provided, match on ClOrdID (tag 11) instead of message type.
            timeout_seconds: Maximum seconds to wait for matching message.

        Returns:
        -------
            list[FixMessage]: All messages received up to and including the matching message.
        """
        if isinstance(message_type, str):
            message_type = [message_type]
        messages: list[FixMessage] = []
        cursor = self._retrieve_cursor
        loop = asyncio.get_running_loop()
        end_time = loop.time() + timeout_seconds

        def advance_shared_cursor() -> None:
            self._retrieve_cursor = max(self._retrieve_cursor, cursor)

        while loop.time() < end_time:
            cursor = min(cursor, len(self._message_history))
            while cursor < len(self._message_history):
                msg = self._message_history[cursor]
                cursor += 1
                messages.append(msg)
                if self._matches_target(msg, message_type, message_cl_ord_id):
                    advance_shared_cursor()
                    return messages

            remaining = end_time - loop.time()
            if remaining <= 0:
                break

            try:
                await asyncio.wait_for(self._message_event.wait(), timeout=remaining)
                self._message_event.clear()
            except TimeoutError:
                break

        advance_shared_cursor()
        return messages

    async def send_message(self, message: FixMessage, *, raw: bool = False) -> None:
        """
        Send the Fix Message to the server.

        Unless 'raw' is set, this function will calculate and
        correctly set the BodyLength (9) and Checksum (10) fields, and
        ensure that the BeginString (8), Body Length (9), Message Type
        (35) and Checksum (10) fields are in the right positions.

        This function does no further validation of the message content.

        Note: The message's MsgSeqNum (tag 34) is updated in-place to the
        actual sequence number before sending.

        Args:
        ----
            message (FixMessage): The message
            raw (bool, optional): If True, encode pairs exactly as provided.

        Raises:
        ------
            ConnectionError: If no active connection exists.
        """
        async with self._lock:
            if not self._writer:
                raise ConnectionError("No connection established - call connect() first")

            next_seq_num = self.msg_seq_num + 1
            sequence_replacement = self._set_message_seq_num(message, next_seq_num)

            try:
                encoded = message.encode(raw)
                self._writer.write(encoded)
                await self._writer.drain()
            except ConnectionResetError as e:
                self.logger.error(
                    "ConnectionResetError: Connection lost while sending message. "
                    "This may indicate permission issues or incorrect endpoint configuration."
                )
                self._restore_message_pair(message, sequence_replacement)
                raise ConnectionResetError("Connection lost") from e
            except (OSError, ConnectionError):
                self.logger.exception("Error sending message")
                self._restore_message_pair(message, sequence_replacement)
                raise
            except Exception:
                self._restore_message_pair(message, sequence_replacement)
                raise

            self.msg_seq_num = next_seq_num
            self.messages_sent.append(message)
            if self.logger.isEnabledFor(logging.INFO):
                clean_message = _sanitize_fix_message(encoded.decode("utf-8"))
                self.logger.info("Client=>Server: %s", clean_message)

    async def create_fix_message_with_basic_header(
        self,
        msg_type: str,
        recv_window: str | None = None,
    ) -> FixMessage:
        """
        Return a basic FixMessage with the mandatory headers required for a valid message.

        Args:
        ----
            msg_type (str): The msg type
            recv_window (str | None, optional): The recv window.

        Returns:
        -------
            FixMessage: the fix message ready to be filled with the body tags
        """
        msg = FixMessage()

        msg.append_pair(FixTags.BEGIN_STRING, self.fix_version, header=True)
        msg.append_pair(FixTags.MSG_TYPE, msg_type, header=True)
        msg.append_pair(FixTags.SENDER_COMP_ID, self.sender_comp_id, header=True)
        msg.append_pair(FixTags.TARGET_COMP_ID, self.target_comp_id, header=True)
        msg.append_pair(FixTags.MSG_SEQ_NUM, "0", header=True)
        msg.append_pair(FixTags.SENDING_TIME, self.current_utc_time(), header=True)
        if recv_window is not None:
            msg.append_pair(FixTags.RECV_WINDOW, recv_window, header=True)

        return msg

    async def logon(
        self,
        recv_window: str | None = None,
    ) -> None:
        """
        Logon method.

        Args:
        ----
            recv_window (str | None, optional): The recv window. Defaults to None.

        Raises:
            ConnectionError: If no active connection exists
            ConnectionResetError: If connection is reset during logon (often due to permission issues)
        """
        if self._restart_flag:
            self.logger.info("The Server will soon restart. Can't start any new connections")
            return

        if not self._writer or not self.is_connected:
            self.logger.error("Cannot send logon - no active connection. Call connect() first.")
            raise ConnectionError("No active connection for logon")

        self.msg_seq_num = 0
        msg = await self.create_fix_message_with_basic_header(FixMsgTypes.LOGON, recv_window)
        sending_time = self._decode(msg.get(FixTags.SENDING_TIME), "") or ""

        try:
            signature = self.generate_signature(
                self.sender_comp_id,
                self.target_comp_id,
                self.msg_seq_num + 1,
                sending_time,
            )
        except (TypeError, ValueError, AttributeError) as e:
            self.logger.error("Failed to generate signature: %s", e)
            raise

        msg.append_pair(FixTags.ENCRYPT_METHOD, self.encrypt_method, header=False)
        msg.append_pair(FixTags.HEART_BT_INT, self.heart_bt_int, header=False)
        msg.append_data(FixTags.RAW_DATA_LENGTH, FixTags.RAW_DATA, signature, header=False)

        msg.append_pair(FixTags.RESET_SEQ_NUM_FLAG, self.reset_seq_num_flag, header=False)

        msg.append_pair(FixTags.USERNAME, self.api_key, header=False)
        msg.append_pair(FixTags.MESSAGE_HANDLING, self.message_handling, header=False)
        if self.response_mode is not None:
            msg.append_pair(FixTags.RESPONSE_MODE, self.response_mode, header=False)
        if self.drop_copy_flag is not None:
            msg.append_pair(FixTags.DROP_COPY_FLAG, self.drop_copy_flag, header=False)

        try:
            await self.send_message(msg)
        except ConnectionResetError as e:
            error_msg = (
                "Connection reset during logon. Common causes:\n"
                "1. Missing FIX_API or FIX_API_READ_ONLY permissions\n"
                "2. Using HMAC key instead of Ed25519 key\n"
                "3. Invalid API key or signature\n"
                "4. Wrong endpoint URL"
            )
            if self.drop_copy_flag == "Y":
                error_msg += "\n5. Drop Copy requires FIX_API or FIX_API_READ_ONLY permission"
            self.logger.error(error_msg)
            raise ConnectionResetError(error_msg) from e

    async def _send_simple_message(
        self, msg_type: str, tag: str, value: str | None, recv_window: str | None = None
    ) -> None:
        msg = await self.create_fix_message_with_basic_header(msg_type, recv_window)
        msg.append_pair(tag, value, header=False)
        await self.send_message(msg)

    async def logout(self, text: str | None = None, recv_window: str | None = None) -> None:
        """
        Logout method.

        Args:
        ----
            text (str | None, optional): The reason to logout. Defaults to None.
            recv_window (str | None, optional): The recv window. Defaults to None.
        """
        await self._send_simple_message(FixMsgTypes.LOGOUT, FixTags.TEXT, text, recv_window)
        self._logout_sent = True

    async def heartbeat(self, test_req_id: str | None = None, recv_window: str | None = None) -> None:
        """
        Heartbeat method.

        Args:
        ----
            test_req_id (str | None, optional): The identifier for a test request. Defaults to None.
            recv_window (str | None, optional): The recv window. Defaults to None.
        """
        await self._send_simple_message(FixMsgTypes.HEARTBEAT, FixTags.TEST_REQ_ID, test_req_id, recv_window)

    async def test_request(self, test_req_id: str | None = None, recv_window: str | None = None) -> None:
        """
        Test request method.

        Args:
        ----
            test_req_id (str | None, optional): The identifier for a test request. Defaults to None.
            recv_window (str | None, optional): The recv window. Defaults to None.
        """
        await self._send_simple_message(FixMsgTypes.TEST_REQUEST, FixTags.TEST_REQ_ID, test_req_id, recv_window)

    async def disconnect(self) -> None:
        """Stop the connection with the server by closing the async connection."""
        self.is_connected = False
        current_task = asyncio.current_task()

        if self._restart_task and not self._restart_task.done() and self._restart_task is not current_task:
            try:
                self._restart_task.cancel()
                await self._restart_task
            except (asyncio.CancelledError, OSError, RuntimeError) as exc:
                self.logger.debug("Restart task cleanup during disconnect: %s", exc)
        if self._restart_task is not current_task:
            self._restart_task = None
        self._restart_flag = False

        if self._receive_task and not self._receive_task.done():
            try:
                self._receive_task.cancel()
                await self._receive_task
            except (asyncio.CancelledError, OSError, RuntimeError) as exc:
                self.logger.debug("Receive task cleanup during disconnect: %s", exc)

        if self._liveness_task and not self._liveness_task.done() and self._liveness_task is not current_task:
            try:
                self._liveness_task.cancel()
                await self._liveness_task
            except (asyncio.CancelledError, OSError, RuntimeError) as exc:
                self.logger.debug("Liveness task cleanup during disconnect: %s", exc)

        if self._writer:
            self._writer.close()
            try:
                await self._writer.wait_closed()
            except (asyncio.CancelledError, OSError, RuntimeError) as exc:
                self.logger.debug("Writer cleanup during disconnect: %s", exc)
            self._writer = None
            self._reader = None

    def _transfer_connection_state(self, other: BinanceFixConnector) -> None:
        """Transfer mutable connection state from another connector into this one.

        Used during reconnect to adopt the new session's live connection
        while keeping the same object identity for callers.
        """
        self._reader = other._reader
        self._writer = other._writer
        self.is_connected = other.is_connected
        self._logout_sent = other._logout_sent
        self.msg_seq_num = other.msg_seq_num
        self._message_history = other._message_history
        self._get_all_cursor = other._get_all_cursor
        self._retrieve_cursor = other._retrieve_cursor
        self._message_event = other._message_event
        self.messages_sent = other.messages_sent
        self._receive_buffer = other._receive_buffer

        other._reader = None
        other._writer = None
        other._receive_task = None
        other._liveness_task = None

    def _connection_params(self) -> dict:
        """Return constructor kwargs to clone this connector's configuration."""
        return {
            "endpoint": self.endpoint,
            "api_key": self.api_key,
            "private_key": self.private_key,
            "sender_comp_id": self.sender_comp_id,
            "target_comp_id": self.target_comp_id,
            "fix_version": self.fix_version,
            "socket_buffer_size": self.socket_buffer_size,
            "heart_bt_int": self.heart_bt_int,
            "reset_seq_num_flag": self.reset_seq_num_flag,
            "encrypt_method": self.encrypt_method,
            "message_handling": self.message_handling,
            "response_mode": self.response_mode,
            "drop_copy_flag": self.drop_copy_flag,
        }

    async def schedule_restart(self) -> None:
        """Schedule an immediate replacement session after maintenance news."""
        if self._restart_flag:
            self.logger.debug("Restart already scheduled, ignoring duplicate")
            return

        self._restart_flag = True
        self.logger.info("Session restart scheduled")

        if self._restart_task is None or self._restart_task.done():
            self._restart_task = asyncio.create_task(self._restart_timer())
            self._restart_task.add_done_callback(self._handle_task_exception)

    async def _restart_timer(self) -> None:
        """Perform the scheduled restart."""
        if not self._restart_flag:
            return

        self.logger.info("Performing scheduled restart...")
        try:
            await self.reconnect()
        except (TimeoutError, OSError, ConnectionError):
            self.logger.exception("Scheduled restart failed")

    async def reconnect(self) -> None:
        """Perform the actual reconnection using a fresh session."""
        if not self._restart_flag:
            self.logger.warning("No restart scheduled")
            return

        new_session = BinanceFixConnector(**self._connection_params())
        try:
            self.logger.info("Connecting replacement session...")
            await new_session.connect()
            await new_session.logon()

            await asyncio.wait_for(
                new_session.retrieve_messages_until(message_type=FixMsgTypes.LOGON),
                timeout=RECONNECT_LOGON_TIMEOUT_SECONDS,
            )

            self.logger.info("Disconnecting old session...")
            with suppress(ConnectionError, OSError):
                await self.logout()
            await self.disconnect()
            await asyncio.sleep(POST_DISCONNECT_DELAY_SECONDS)

            if new_session._receive_task and not new_session._receive_task.done():
                new_session._receive_task.cancel()
                with suppress(asyncio.CancelledError):
                    await new_session._receive_task
            if new_session._liveness_task and not new_session._liveness_task.done():
                new_session._liveness_task.cancel()
                with suppress(asyncio.CancelledError):
                    await new_session._liveness_task

            self._transfer_connection_state(new_session)

            self._receive_task = asyncio.create_task(self._receive_messages())
            self._receive_task.add_done_callback(self._handle_task_exception)
            self._liveness_task = asyncio.create_task(self._monitor_liveness())
            self._liveness_task.add_done_callback(self._handle_task_exception)

            self.logger.info("Restart completed successfully")

        except (TimeoutError, OSError, ConnectionError):
            self.logger.exception("Error during restart")
            try:
                await new_session.disconnect()
            except (OSError, ConnectionError):
                self.logger.debug("Cleanup of failed restart session raised", exc_info=True)
            raise

        finally:
            self._restart_flag = False
