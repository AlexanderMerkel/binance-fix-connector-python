#!/usr/bin/env python3
"""
Async Cancel Order Example

Demonstrates placing a limit order then cancelling it using the async
Binance FIX Connector.  Uses testnet with safe parameters.
"""

import asyncio
import functools
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from common import (
    ORD_STATUS,
    TESTNET_OE_URL,
    get_field,
    get_safe_limit_order_params,
    graceful_logout,
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
        cl_ord_id = str(time.time_ns())
        msg = await client_oe.create_fix_message_with_basic_header("D")
        msg.append_pair(38, params.quantity)
        msg.append_pair(40, 2)
        msg.append_pair(11, cl_ord_id)
        msg.append_pair(44, params.price)
        msg.append_pair(54, SIDE)
        msg.append_pair(55, INSTRUMENT)
        msg.append_pair(59, 1)

        await client_oe.send_message(msg)

        responses = await client_oe.retrieve_messages_until(message_type="8", timeout_seconds=10)
        exec_report = next(
            (x for x in responses if x.message_type.decode("utf-8") == "8"),
            None,
        )

        if not exec_report:
            client_oe.logger.warning("No execution report received for new order")
            return

        f = functools.partial(get_field, exec_report)
        status = f(39)
        client_oe.logger.info(f"New order -> ClOrdID: {f(11)} | Status: {ORD_STATUS.get(status, status)}")

        if status not in ("0", "A"):
            client_oe.logger.warning(f"Order not in cancelable state (status={status}), skipping cancel")
            return

        cancel_cl_ord_id = str(time.time_ns())
        cancel_msg = await client_oe.create_fix_message_with_basic_header("F")
        cancel_msg.append_pair(11, cancel_cl_ord_id)
        cancel_msg.append_pair(41, cl_ord_id)
        cancel_msg.append_pair(55, INSTRUMENT)

        await client_oe.send_message(cancel_msg)

        cancel_responses = await client_oe.retrieve_messages_until(message_type=["8", "9"], timeout_seconds=10)
        cancel_resp = next(
            (x for x in cancel_responses if x.message_type.decode("utf-8") in ("8", "9")),
            None,
        )

        if not cancel_resp:
            client_oe.logger.warning("No response received for cancel request")
            return

        resp_type = cancel_resp.message_type.decode("utf-8")
        fc = functools.partial(get_field, cancel_resp)

        if resp_type == "9":
            client_oe.logger.warning(f"Cancel rejected -> Reason: {fc(58)} | Error code: {fc(25016)}")
        else:
            cancel_status = fc(39)
            client_oe.logger.info(
                f"Cancel result -> ClOrdID: {fc(11)} | "
                f"OrigClOrdID: {fc(41)} | "
                f"Status: {ORD_STATUS.get(cancel_status, cancel_status)}"
            )

    finally:
        await graceful_logout(client_oe)


if __name__ == "__main__":
    asyncio.run(main())
