#!/usr/bin/env python3
"""
Async Instrument List Example

Retrieves the list of available instruments using the async Binance FIX Connector.
"""

import asyncio
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from common import TESTNET_MD_URL, get_field, graceful_logout, load_credentials, wait_for_logon

API_KEY, PRIVATE_KEY = load_credentials()

from binance_fix_connector_async import create_market_data_session

SYMBOL = "BNBUSDT"


async def main():
    session = await create_market_data_session(api_key=API_KEY, private_key=PRIVATE_KEY, endpoint=TESTNET_MD_URL)

    try:
        if not await wait_for_logon(session):
            return

        msg = await session.create_fix_message_with_basic_header("x", "5000")
        msg.append_pair("320", f"SEC_{int(time.time())}")
        msg.append_pair("559", "0")
        msg.append_pair("55", SYMBOL)
        await session.send_message(msg)
        session.logger.info("Sent security list request")

        responses = await session.retrieve_messages_until(message_type="y", timeout_seconds=30)
        if responses:
            resp = responses[-1]
            total = get_field(resp, 146)
            if total:
                count = int(total)
                session.logger.info("Total securities: %s", count)
                symbol = get_field(resp, 55)
                if symbol:
                    session.logger.info("  First symbol: %s", symbol)
            else:
                session.logger.warning("No security count in response")
        else:
            session.logger.warning("No security list response received")

    finally:
        await graceful_logout(session)


if __name__ == "__main__":
    asyncio.run(main())
