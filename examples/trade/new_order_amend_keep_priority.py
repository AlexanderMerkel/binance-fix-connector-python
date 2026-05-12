#!/usr/bin/env python3
"""
Async Order Amend Keep Priority Example

Demonstrates placing a limit order and then sending an OrderAmendKeepPriority
request using the async Binance FIX Connector. Uses testnet with
non-marketable parameters and skips the amend if the seed order is rejected.
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
AMENDED_QUANTITY = "0.9"


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
        new_order = await client_oe.create_fix_message_with_basic_header("D")
        new_order.append_pair(38, params.quantity)
        new_order.append_pair(40, 2)
        new_order.append_pair(11, cl_ord_id)
        new_order.append_pair(44, params.price)
        new_order.append_pair(54, SIDE)
        new_order.append_pair(55, INSTRUMENT)
        new_order.append_pair(59, 1)

        await client_oe.send_message(new_order)

        responses = await client_oe.retrieve_messages_until(
            message_type="8",
            message_cl_ord_id=cl_ord_id,
            timeout_seconds=10,
        )
        exec_report = next((x for x in responses if get_field(x, 35) == "8"), None)

        if not exec_report:
            client_oe.logger.warning("No execution report received for seed order")
            return

        f = functools.partial(get_field, exec_report)
        status = f(39)
        order_id = f(37)
        client_oe.logger.info(
            "Seed order -> ClOrdID: %s | OrderID: %s | Status: %s",
            f(11),
            order_id,
            ORD_STATUS.get(status, status),
        )

        if status not in ("0", "A") or not order_id:
            client_oe.logger.warning(
                "Order not amendable (status=%s, order_id=%s), skipping amend",
                status,
                order_id,
            )
            if is_order_open(status):
                await cancel_order_by_cl_ord_id(client_oe, cl_ord_id, INSTRUMENT)
            return

        amend_cl_ord_id = str(time.time_ns())
        amend = await client_oe.create_fix_message_with_basic_header("XAK")
        amend.append_pair(11, amend_cl_ord_id)
        amend.append_pair(37, order_id)
        amend.append_pair(41, cl_ord_id)
        amend.append_pair(55, INSTRUMENT)
        amend.append_pair(38, AMENDED_QUANTITY)

        await client_oe.send_message(amend)

        amend_responses = await client_oe.retrieve_messages_until(
            message_type=["3", "8", "XAR"],
            timeout_seconds=10,
        )
        amend_resp = next(
            (x for x in amend_responses if get_field(x, 35) in ("3", "8", "XAR")),
            None,
        )

        if not amend_resp:
            client_oe.logger.warning("No response received for amend request")
            return

        a = functools.partial(get_field, amend_resp)
        if a(35) == "3":
            client_oe.logger.warning(
                "Amend request rejected -> Reason: %s | RefSeqNum: %s",
                a(58),
                a(45),
            )
            await cancel_order_by_cl_ord_id(client_oe, cl_ord_id, INSTRUMENT)
            return

        if a(35) == "XAR":
            client_oe.logger.warning(
                "Amend rejected -> ClOrdID: %s | Reason: %s | Error code: %s",
                a(11),
                a(58),
                a(25016),
            )
            await cancel_order_by_cl_ord_id(client_oe, cl_ord_id, INSTRUMENT)
            return

        amend_status = a(39)
        client_oe.logger.info(
            "Amend result -> ClOrdID: %s | Status: %s | Quantity: %s",
            a(11),
            ORD_STATUS.get(amend_status, amend_status),
            a(38),
        )
        if is_order_open(amend_status):
            await cancel_order_by_cl_ord_id(client_oe, a(11) or cl_ord_id, INSTRUMENT)

    finally:
        await graceful_logout(client_oe)


if __name__ == "__main__":
    asyncio.run(main())
