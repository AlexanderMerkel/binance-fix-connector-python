#!/usr/bin/env python3
"""
Async New List OTO Order Example

Demonstrates placing a One-Takes-Other (OTO) order using the async
Binance FIX Connector. Uses safe parameters to avoid accidental executions.
"""

import asyncio
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from common import (
    ORD_STATUS,
    SIDES,
    TESTNET_OE_URL,
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

        msg = await client_oe.create_fix_message_with_basic_header("E")
        identifier = f"{time.time_ns()}"
        working_leg_id = f"w{identifier}"
        pending_leg_id = f"p{identifier}"
        params = await get_safe_limit_order_params(INSTRUMENT, SIDE, target_quantity=QUANTITY)
        client_oe.logger.info("Creating OTO order list with ClListID: %s", identifier)

        msg.append_pair(73, 2)

        msg.append_pair(11, working_leg_id)
        msg.append_pair(55, INSTRUMENT)
        msg.append_pair(54, SIDE)
        msg.append_pair(38, params.quantity)
        msg.append_pair(40, 2)
        msg.append_pair(44, params.price)
        msg.append_pair(59, 1)

        msg.append_pair(11, pending_leg_id)
        msg.append_pair(55, INSTRUMENT)
        msg.append_pair(54, SIDE)
        msg.append_pair(38, params.quantity)
        msg.append_pair(40, 2)
        msg.append_pair(44, params.price)
        msg.append_pair(59, 1)
        msg.append_pair(25010, 1)
        msg.append_pair(25011, 3)
        msg.append_pair(25012, 0)
        msg.append_pair(25013, 1)
        msg.append_pair(1385, 2)
        msg.append_pair(25014, identifier)

        client_oe.logger.info(
            "Working leg: %s %s %s @ %s (LIMIT)",
            SIDES.get(SIDE, SIDE),
            params.quantity,
            INSTRUMENT,
            params.price,
        )
        client_oe.logger.info(
            "Pending leg: %s %s %s @ %s (LIMIT)",
            SIDES.get(SIDE, SIDE),
            params.quantity,
            INSTRUMENT,
            params.price,
        )

        await client_oe.send_message(msg)

        responses = await client_oe.retrieve_messages_until(message_type="N", timeout_seconds=10)
        resp = next(
            (x for x in responses if x.message_type.decode("utf-8") == "N"),
            None,
        )

        if resp:
            client_oe.logger.info("Client List ID: %s", get_field(resp, 25014))
            client_oe.logger.info("List Status Type: %s", get_field(resp, 429))
            client_oe.logger.info("List Order Status: %s", get_field(resp, 431))
            reject = get_field(resp, 1386)
            if reject:
                client_oe.logger.info("Rejection Reason: %s", reject)

            await asyncio.sleep(1)
            for exec_msg in await client_oe.get_all_new_messages_received():
                if get_field(exec_msg, 35) == "8":
                    cl_ord_id = get_field(exec_msg, 11) or "N/A"
                    ord_status = get_field(exec_msg, 39) or "N/A"
                    client_oe.logger.info(
                        "Execution Report - Order ID: %s, Status: %s",
                        cl_ord_id,
                        ORD_STATUS.get(ord_status, ord_status),
                    )
                    if is_order_open(ord_status) and cl_ord_id != "N/A":
                        await cancel_order_by_cl_ord_id(client_oe, cl_ord_id, INSTRUMENT)
        else:
            client_oe.logger.warning("No list status response received")

    finally:
        await graceful_logout(client_oe)


if __name__ == "__main__":
    asyncio.run(main())
