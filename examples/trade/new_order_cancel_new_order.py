#!/usr/bin/env python3
"""
Async Order Cancel/New Order Example

Demonstrates placing a limit order and then sending an
OrderCancelRequestAndNewOrderSingle using the async Binance FIX Connector.
Uses testnet with non-marketable parameters and skips the follow-up request if
the seed order is rejected.
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
                "Order not replaceable (status=%s, order_id=%s), skipping cancel/new",
                status,
                order_id,
            )
            return

        replace_cl_ord_id = str(time.time_ns())
        replace = await client_oe.create_fix_message_with_basic_header("XCN")
        replace.append_pair(25033, 1)
        replace.append_pair(25034, cl_ord_id)
        replace.append_pair(11, replace_cl_ord_id)
        replace.append_pair(37, order_id)
        replace.append_pair(38, params.quantity)
        replace.append_pair(40, 2)
        replace.append_pair(44, params.price)
        replace.append_pair(54, SIDE)
        replace.append_pair(55, INSTRUMENT)
        replace.append_pair(59, 1)

        await client_oe.send_message(replace)

        replace_responses = await client_oe.retrieve_messages_until(
            message_type=["3", "8", "9"],
            timeout_seconds=10,
        )
        replace_resp = next(
            (x for x in replace_responses if get_field(x, 35) in ("3", "8", "9")),
            None,
        )

        if not replace_resp:
            client_oe.logger.warning("No response received for cancel/new request")
            return

        r = functools.partial(get_field, replace_resp)
        if r(35) == "3":
            client_oe.logger.warning(
                "Cancel/new request rejected -> Reason: %s | RefSeqNum: %s",
                r(58),
                r(45),
            )
            await cancel_order_by_cl_ord_id(client_oe, cl_ord_id, INSTRUMENT)
            return

        if r(35) == "9":
            client_oe.logger.warning(
                "Cancel/new rejected -> ClOrdID: %s | Reason: %s | Error code: %s",
                r(11),
                r(58),
                r(25016),
            )
            await cancel_order_by_cl_ord_id(client_oe, cl_ord_id, INSTRUMENT)
            return

        replace_status = r(39)
        client_oe.logger.info(
            "Cancel/new result -> ClOrdID: %s | Status: %s | Price: %s",
            r(11),
            ORD_STATUS.get(replace_status, replace_status),
            r(44),
        )
        if is_order_open(replace_status):
            await cancel_order_by_cl_ord_id(client_oe, r(11) or replace_cl_ord_id, INSTRUMENT)

    finally:
        await graceful_logout(client_oe)


if __name__ == "__main__":
    asyncio.run(main())
