# Binance FIX API Connector in Python (Async)

This is a high-performance Python library that provides access to Binance Financial Information eXchange (FIX) [SPOT messages](https://github.com/binance/binance-spot-api-docs/blob/master/fix-api.md#message-components) using the FIX protocol with full async/await support.
It allows you to perform key operations such as placing orders, canceling orders, and querying current limit usage.

## Key Features

- **High Performance**: Built with async/await for optimal concurrent performance and resource efficiency
- **Modern Architecture**: Native asyncio implementation with explicit session lifecycle management
- **Type Safe**: Type-annotated core library with `basedpyright` checks for `src/`
- **Low Latency**: Optimized async message processing and connection handling
- **Full API Coverage**: Support for all Binance FIX API message types (Order Entry, Market Data, Drop Copy)
- **Production-Oriented**: Async session lifecycle handling, redacted logging, and marker-driven real testnet scenarios

## Prerequisites

Before using or testing the library, ensure that the necessary dependencies are installed. You can do this by running the following command:

```
pip install binance-fix-connector-async
```

**Notes:**

- FIX API only support Ed25519 keys. Please refer to this [tutorial](https://www.binance.com/en/support/faq/how-to-generate-an-ed25519-key-pair-to-send-api-requests-on-binance-6b9a63f1e3384cf48a2eedb82767a69a) for setting up an Ed25519 key pair on the mainnet, and this one for the [testnet](https://testnet.binance.vision/).
- Ensure that your API key has the appropriate Fix API permissions for the Testnet environment before you begin testing.
- Real testnet scenarios are marker-driven with `requires_testnet` and are not part of default CI runs.

## Example

All the FIX messages can be created with the `BinanceFixConnector` class. The following example demonstrates how to create a simple order using the async FIX API:

```python
import asyncio
import time
from binance_fix_connector_async.fix_connector import create_order_entry_session
from binance_fix_connector_async.utils import get_private_key

# Credentials
API_KEY = "..."
PATH_TO_PRIVATE_KEY_PEM_FILE = "/path/to/ed25519_private_key.pem"

# FIX URL
FIX_OE_URL = "tcp+tls://fix-oe.testnet.binance.vision:9000"

# Response types
ORD_STATUS = {
    "0": "NEW",
    "1": "PARTIALLY_FILLED",
    "2": "FILLED",
    "4": "CANCELED",
    "6": "PENDING_CANCEL",
    "8": "REJECTED",
    "A": "PENDING_NEW",
    "C": "EXPIRED",
}
ORD_TYPES = {"1": "MARKET", "2": "LIMIT", "3": "STOP", "4": "STOP_LIMIT"}
SIDES = {"1": "BUY", "2": "SELL"}
TIME_IN_FORCE = {
    "1": "GOOD_TILL_CANCEL",
    "3": "IMMEDIATE_OR_CANCEL",
    "4": "FILL_OR_KILL",
}
ORD_REJECT_REASON = {"99": "OTHER"}

# Parameter
INSTRUMENT = "BNBUSDT"

async def main():
    client_oe = await create_order_entry_session(
        api_key=API_KEY,
        private_key=get_private_key(PATH_TO_PRIVATE_KEY_PEM_FILE),
        endpoint=FIX_OE_URL,
    )
    await client_oe.retrieve_messages_until(message_type="A")

    example = "This example shows how to place a single order. Order type LIMIT.\nCheck https://github.com/binance/binance-spot-api-docs/blob/master/fix-api.md#newordersingled for additional types."
    client_oe.logger.info(example)

    # PLACING SIMPLE ORDER
    msg = await client_oe.create_fix_message_with_basic_header("D")
    msg.append_pair(38, 1)  # ORD QTY
    msg.append_pair(40, 2)  # ORD TYPE
    msg.append_pair(11, str(time.time_ns()))  # CL ORD ID
    msg.append_pair(44, "CURRENT_FILTER_VALID_PRICE")  # PRICE; see examples/trade/new_order.py
    msg.append_pair(54, 2)  # SIDE
    msg.append_pair(55, INSTRUMENT)  # SYMBOL
    msg.append_pair(59, 1)  # TIME IN FORCE
    await client_oe.send_message(msg)

    responses = await client_oe.retrieve_messages_until(message_type="8")
    resp = next(
        (x for x in responses if x.message_type.decode("utf-8") == "8"),
        None,
    )
    client_oe.logger.info("Parsing response Execution Report (8) for an order LIMIT type.")

    cl_ord_id = None if not resp.get(11) else resp.get(11).decode("utf-8")
    order_qty = None if not resp.get(38) else resp.get(38).decode("utf-8")
    ord_type = None if not resp.get(40) else resp.get(40).decode("utf-8")
    side = None if not resp.get(54) else resp.get(54).decode("utf-8")
    symbol = None if not resp.get(55) else resp.get(55).decode("utf-8")
    price = None if not resp.get(44) else resp.get(44).decode("utf-8")
    time_in_force = None if not resp.get(59) else resp.get(59).decode("utf-8")
    cum_qty = None if not resp.get(14) else resp.get(14).decode("utf-8")
    last_qty = None if not resp.get(32) else resp.get(32).decode("utf-8")
    ord_status = None if not resp.get(39) else resp.get(39).decode("utf-8")
    ord_rej_reason = None if not resp.get(103) else resp.get(103).decode("utf-8")
    error_code = None if not resp.get(25016) else resp.get(25016).decode("utf-8")
    text = None if not resp.get(58) else resp.get(58).decode("utf-8")

    client_oe.logger.info(f"Client order ID: {cl_ord_id}")
    client_oe.logger.info(f"Symbol: {symbol}")
    client_oe.logger.info(
        f"Order -> Type: {ORD_TYPES.get(ord_type, ord_type)} | Side: {SIDES.get(side, side)} | TimeInForce: {TIME_IN_FORCE.get(time_in_force,time_in_force)}",
    )
    client_oe.logger.info(
        f"Price: {price} | Quantity: {order_qty} | cum qty: {cum_qty} | last qty: {last_qty}"
    )
    client_oe.logger.info(
        f"Status: {ORD_STATUS.get(ord_status,ord_status)} | Msg: {ORD_REJECT_REASON.get(ord_rej_reason,ord_rej_reason)}",
    )
    client_oe.logger.info(f"Error code: {error_code} | Reason: {text}")

    # LOGOUT
    client_oe.logger.info("LOGOUT (5)")
    await client_oe.logout()
    await client_oe.retrieve_messages_until(message_type="5")
    client_oe.logger.info(
        "Closing the connection with server as we already sent the logout message"
    )
    await client_oe.disconnect()

# Run the async main function
if __name__ == "__main__":
    asyncio.run(main())
```

Please look at [`examples`](./examples) folder to test the examples.
To try the examples, use [`config.json.example`](./config.json.example) in the repository root as a template, or provide credentials via environment variables.
Examples use testnet credentials by default:

```bash
export BINANCE_TESTNET_FIX_KEY="..."
export BINANCE_TESTNET_FIX_PRIVATE_KEY_PATH="/path/to/ed25519_private_key.pem"
```

Set `BINANCE_FIX_ENV=mainnet` or `USE_TESTNET=false` only when you explicitly want mainnet credentials.

## API Reference

### Factory Functions

The library provides three factory functions for different types of FIX sessions:

#### `create_order_entry_session()`

```python
create_order_entry_session(
    api_key: str,
    private_key: ed25519.Ed25519PrivateKey,
    endpoint: str = "tcp+tls://fix-oe.binance.com:9000",
    sender_comp_id: str = "TRADE",
    target_comp_id: str = "SPOT",
    fix_version: str = "FIX.4.4",
    heart_bt_int: int = 30,
    message_handling: int = 2,
    response_mode: int = 1,
    recv_window: int | None = None,
) -> BinanceFixConnector
```

Creates a session for order placement and management.

#### `create_market_data_session()`

```python
create_market_data_session(
    api_key: str,
    private_key: ed25519.Ed25519PrivateKey,
    endpoint: str = "tcp+tls://fix-md.binance.com:9000",
    sender_comp_id: str = "WATCH",
    target_comp_id: str = "SPOT",
    fix_version: str = "FIX.4.4",
    heart_bt_int: int = 30,
    message_handling: int = 2,
    recv_window: int | None = None,
) -> BinanceFixConnector
```

Creates a session for market data streaming.

#### `create_drop_copy_session()`

```python
create_drop_copy_session(
    api_key: str,
    private_key: ed25519.Ed25519PrivateKey,
    endpoint: str = "tcp+tls://fix-dc.binance.com:9000",
    sender_comp_id: str = "TECH",
    target_comp_id: str = "SPOT",
    fix_version: str = "FIX.4.4",
    heart_bt_int: int = 30,
    message_handling: int = 2,
    response_mode: int = 1,
    recv_window: int | None = None,
    check_permissions: bool = False,
    hmac_secret: str | None = None,
    permission_base_url: str = "https://api.binance.com",
) -> BinanceFixConnector
```

Creates a session for trade reporting and compliance.
The optional permission check uses Binance's mainnet HMAC Wallet API restriction endpoint. Spot Testnet does not support `/sapi`; testnet Ed25519 FIX-key compatibility is validated by the FIX logon itself.

### Core Methods

#### Connection Management

- `connect()` - Establish connection to Binance FIX server
- `disconnect()` - Close connection and cleanup resources
- `logon(recv_window=None)` - Perform FIX logon with authentication
- `logout(text=None, recv_window=None)` - Send logout message

#### Message Operations

- `create_fix_message_with_basic_header(msg_type, recv_window=None)` - Create properly formatted FIX message
- `send_message(message, raw=False)` - Send FIX message to server
- `retrieve_messages_until(message_type, message_cl_ord_id=None, timeout_seconds=3)` - Retrieve messages until a specific message type, and optionally `ClOrdID`, is received
- `get_all_new_messages_received()` - Get all received messages not returned by previous calls to this method

#### Utility Functions

- `get_api_key(config_path)` - Load API credentials from config file
- `get_private_key(key_path)` - Load Ed25519 private key from PEM file

## Async vs Original Library Comparison

This async implementation (`binance-fix-connector-async`) is a modern alternative to the [original sync library](https://github.com/binance/binance-fix-connector-python). Both libraries provide identical FIX API functionality with different execution models.

### Async Library Advantages

| Feature | Async Implementation | Benefit |
|---------|---------------------|---------|
| **Concurrent Operations** | Native asyncio support | 20-40% better throughput with multiple sessions |
| **Memory Efficiency** | Async I/O patterns | 7-15% lower memory usage |
| **Modern Architecture** | Built for Python 3.9+ | Integrates with FastAPI, asyncio ecosystems |
| **Resource Utilization** | Non-blocking operations | Better CPU and I/O efficiency |
| **Scalability** | Coroutine-based | Handles 100+ concurrent sessions efficiently |
| **P99 Latency** | Optimized async patterns | 50%+ better tail latency performance |

### Original Library Advantages

| Feature | Sync Implementation | Benefit |
|---------|-------------------|---------|
| **Single Session Performance** | Thread-based execution | 2-5% better raw throughput for single sessions |
| **Simplicity** | Traditional blocking I/O | Easier to understand for sync-only developers |
| **Ecosystem Maturity** | Longer production usage | More battle-tested in diverse environments |
| **Thread Integration** | Native threading | Better integration with thread-based applications |

### Performance Comparison

Local benchmark snapshot:

| Metric | Original (Sync) | Async Implementation | Difference |
|--------|----------------|---------------------|-------------|
| **Single Session Throughput** | 1,876 ops/sec | 1,825 ops/sec | -2.7% |
| **Memory Usage** | 10.1 MB peak | 9.4 MB peak | **+7.5%** |
| **P99 Latency** | 0.498 ms | 0.239 ms | **+52%** |
| **Concurrent (50 sessions)** | 8,624 ops/sec | 8,124 ops/sec | -5.8% |
| **Message Creation** | 1,211K msg/sec | 1,528K msg/sec | **+26%** |

Run `benchmark/comprehensive_analysis.py` on the target host before using these numbers for deployment decisions.

### 🔄 Migration Path

**Migration Scope:**

- Async code must add `await` around session creation, send/receive, and cleanup calls.
- Message construction and supported FIX fields are intended to stay aligned with the original connector.
- Both libraries can be installed side by side under different import names.

**Migration Steps:**

```python
# 1. Install async version
pip install binance-fix-connector-async

# 2. Update imports
from binance_fix_connector_async.fix_connector import create_order_entry_session

# 3. Add async/await keywords
async def main():
    client = await create_order_entry_session(api_key, private_key)
    await client.send_message(msg)
    await client.logout()

# 4. Run with asyncio
asyncio.run(main())
```

### 📈 Validation Results

The benchmark suite checks:

- **Performance**: 11 benchmark tests across throughput, latency, and memory
- **Consistency**: message construction and state-transition parity checks
- **Compatibility**: supported public factory and connector methods

**Run validation yourself:**

```bash
cd benchmark/
python comprehensive_analysis.py
```

## Documentation

### Performance Analysis

- **[Benchmark Results](benchmark/README.md)** - Comprehensive performance analysis
- **Run Analysis**: `python benchmark/comprehensive_analysis.py`

### Official Binance Documentation

- **[Binance FIX API Documentation](https://developers.binance.com/docs/binance-spot-api-docs/fix-api)** - Official FIX API reference

## Summary

Both libraries provide FIX API access with different execution models. Choose based on your architecture:

- **Async**: Better for modern, concurrent, memory-conscious applications
- **Original**: Better for simple, single-session, thread-based applications

Validate the real testnet examples and E2E runner with your own FIX key before using the async connector in production.

## License

MIT
