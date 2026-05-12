#!/usr/bin/env python3
"""
Async New Order Example

Demonstrates placing a limit order using the async Binance FIX Connector.
Uses testnet with safe parameters to avoid accidental executions.
"""

import asyncio
import functools
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from common import (
    ORD_STATUS,
    ORD_TYPES,
    SIDES,
    TESTNET_OE_URL,
    TIME_IN_FORCE,
    cancel_order_by_cl_ord_id,
    get_field,
    get_safe_limit_order_params,
    graceful_logout,
    is_order_open,
    load_credentials,
    wait_for_logon,
)

API_KEY, PRIVATE_KEY = load_credentials()

from binance_fix_connector_async import create_order_entry_session

INSTRUMENT = "BNBUSDT"
SIDE = "1"
QUANTITY = "1"


async def main():
    client_oe = await create_order_entry_session(
        api_key=API_KEY,
        private_key=PRIVATE_KEY,
        endpoint=TESTNET_OE_URL,
    )

    try:
        if not await wait_for_logon(client_oe):
            return

        params = await get_safe_limit_order_params(INSTRUMENT, SIDE, target_quantity=QUANTITY)
        msg = await client_oe.create_fix_message_with_basic_header("D")
        cl_ord_id = str(time.time_ns())
        msg.append_pair(38, params.quantity)
        msg.append_pair(40, 2)
        msg.append_pair(11, cl_ord_id)
        msg.append_pair(44, params.price)
        msg.append_pair(54, SIDE)
        msg.append_pair(55, INSTRUMENT)
        msg.append_pair(59, 1)

        await client_oe.send_message(msg)

        responses = await client_oe.retrieve_messages_until(message_type="8", timeout_seconds=10)
        resp = next(
            (x for x in responses if x.message_type.decode("utf-8") == "8"),
            None,
        )

        if resp:
            f = functools.partial(get_field, resp)
            client_oe.logger.info("Client order ID: %s", f(11))
            client_oe.logger.info("Symbol: %s", f(55))
            client_oe.logger.info(
                "Order -> Type: %s | Side: %s | TimeInForce: %s",
                ORD_TYPES.get(f(40), f(40)),
                SIDES.get(f(54), f(54)),
                TIME_IN_FORCE.get(f(59), f(59)),
            )
            client_oe.logger.info(
                "Price: %s | Quantity: %s | cum qty: %s | last qty: %s",
                f(44),
                f(38),
                f(14),
                f(32),
            )
            client_oe.logger.info(
                "Status: %s | Reject reason: %s",
                ORD_STATUS.get(f(39), f(39)),
                f(103),
            )
            client_oe.logger.info("Error code: %s | Reason: %s", f(25016), f(58))
            if is_order_open(f(39)):
                await cancel_order_by_cl_ord_id(client_oe, cl_ord_id, INSTRUMENT)
        else:
            client_oe.logger.warning("No execution report received")

    finally:
        await graceful_logout(client_oe)


if __name__ == "__main__":
    asyncio.run(main())
