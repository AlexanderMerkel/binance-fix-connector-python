#!/usr/bin/env python3
"""
Async Market Data Ticker Stream Example

Subscribes to ticker data (24hr statistics) using the async Binance FIX Connector.
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from common import (
    TESTNET_MD_URL,
    create_market_data_request,
    graceful_logout,
    load_credentials,
    run_stream_loop,
    wait_for_logon,
)

API_KEY, PRIVATE_KEY = load_credentials()

from binance_fix_connector_async import create_market_data_session

SYMBOL = "BTCUSDT"
MD_REQ_ID = f"BOOK_TICKER_STREAM_{SYMBOL}"


async def main():
    session = await create_market_data_session(
        api_key=API_KEY,
        private_key=PRIVATE_KEY,
        endpoint=TESTNET_MD_URL,
    )

    try:
        if not await wait_for_logon(session):
            return

        session.logger.info("Subscribing to %s ticker data...", SYMBOL)
        await session.send_message(
            await create_market_data_request(
                session,
                SYMBOL,
                "1",
                md_req_id=MD_REQ_ID,
                prefix="TICKER",
                depth="1",
                entry_types=["0", "1"],
            )
        )

        def handle_ticker(msg):
            msg_type = msg.get("35")
            if not (msg_type and msg_type.decode() in ["W", "X"]):
                return False
            symbol = msg.get("55")
            if symbol:
                session.logger.info("Ticker for %s", symbol.decode())
                entry_px = msg.get("270")
                entry_size = msg.get("271")
                if entry_px:
                    session.logger.info("  Price: %s", entry_px.decode())
                if entry_size:
                    session.logger.info("  Size: %s", entry_size.decode())
            return True

        message_count = await run_stream_loop(session, handle_ticker)

        await session.send_message(
            await create_market_data_request(
                session,
                SYMBOL,
                "2",
                md_req_id=MD_REQ_ID,
                prefix="TICKER",
                depth="1",
                entry_types=["0", "1"],
            )
        )
        session.logger.info("Received %s ticker messages", message_count)

    finally:
        await graceful_logout(session)


if __name__ == "__main__":
    asyncio.run(main())
