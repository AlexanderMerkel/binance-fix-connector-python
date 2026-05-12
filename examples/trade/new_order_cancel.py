#!/usr/bin/env python3
"""
Async Order Cancel Request Example

Demonstrates sending a cancel request using the async Binance FIX Connector.
Uses testnet and handles expected reject responses for unknown orders.
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
    graceful_logout,
    load_credentials,
    wait_for_logon,
)

API_KEY, PRIVATE_KEY = load_credentials()

from binance_fix_connector_async import create_order_entry_session

INSTRUMENT = "BNBUSDT"


async def main():
    client_oe = await create_order_entry_session(
        api_key=API_KEY,
        private_key=PRIVATE_KEY,
        endpoint=TESTNET_OE_URL,
    )

    try:
        if not await wait_for_logon(client_oe):
            return

        cancel_cl_ord_id = str(time.time_ns())
        orig_cl_ord_id = f"UNKNOWN_{cancel_cl_ord_id}"
        msg = await client_oe.create_fix_message_with_basic_header("F")
        msg.append_pair(11, cancel_cl_ord_id)
        msg.append_pair(41, orig_cl_ord_id)
        msg.append_pair(55, INSTRUMENT)

        await client_oe.send_message(msg)

        responses = await client_oe.retrieve_messages_until(
            message_type=["3", "8", "9"],
            timeout_seconds=10,
        )
        resp = next(
            (x for x in responses if get_field(x, 35) in ("3", "8", "9")),
            None,
        )

        if not resp:
            client_oe.logger.warning("No response received for cancel request")
            return

        f = functools.partial(get_field, resp)
        if f(35) == "3":
            client_oe.logger.warning(
                "Cancel request rejected -> Reason: %s | RefSeqNum: %s",
                f(58),
                f(45),
            )
            return

        if f(35) == "9":
            client_oe.logger.info(
                "Cancel rejected -> ClOrdID: %s | OrigClOrdID: %s | Reason: %s | Error code: %s",
                f(11),
                f(41),
                f(58),
                f(25016),
            )
            return

        status = f(39)
        client_oe.logger.info(
            "Cancel result -> ClOrdID: %s | OrigClOrdID: %s | Status: %s",
            f(11),
            f(41),
            ORD_STATUS.get(status, status),
        )

    finally:
        await graceful_logout(client_oe)


if __name__ == "__main__":
    asyncio.run(main())
