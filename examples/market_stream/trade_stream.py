#!/usr/bin/env python3
"""
Async Market Data Trade Stream Example

Subscribes to individual trade data using the async Binance FIX Connector.
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
MD_REQ_ID = f"TRADE_STREAM_{SYMBOL}"


async def main():
    session = await create_market_data_session(
        api_key=API_KEY,
        private_key=PRIVATE_KEY,
        endpoint=TESTNET_MD_URL,
    )

    try:
        if not await wait_for_logon(session):
            return

        session.logger.info("Subscribing to %s trade data...", SYMBOL)
        await session.send_message(
            await create_market_data_request(
                session,
                SYMBOL,
                "1",
                md_req_id=MD_REQ_ID,
                prefix="TRADE",
                depth="1",
                entry_types=["2"],
            )
        )

        total_volume = 0.0

        def handle_trade(msg):
            nonlocal total_volume
            msg_type = msg.get("35")
            if not (msg_type and msg_type.decode() in ["W", "X"]):
                return False
            entry_type = msg.get("269")
            if not (entry_type and entry_type.decode() == "2"):
                return False
            symbol = msg.get("55")
            if symbol:
                price = msg.get("270")
                size = msg.get("271")
                trade_time = msg.get("273")
                price_s = price.decode() if price else "N/A"
                size_s = size.decode() if size else "N/A"
                time_s = trade_time.decode() if trade_time else "N/A"
                session.logger.info(
                    "Trade - %s | Price: %s | Size: %s | Time: %s",
                    symbol.decode(),
                    price_s,
                    size_s,
                    time_s,
                )
                if size:
                    try:
                        total_volume += float(size_s)
                    except ValueError:
                        session.logger.debug("Could not parse trade size: %s", size_s)
            return True

        message_count = await run_stream_loop(session, handle_trade)

        await session.send_message(
            await create_market_data_request(
                session,
                SYMBOL,
                "2",
                md_req_id=MD_REQ_ID,
                prefix="TRADE",
                depth="1",
                entry_types=["2"],
            )
        )
        session.logger.info(
            "Received %s trades, total volume: %.6f",
            message_count,
            total_volume,
        )

    finally:
        await graceful_logout(session)


if __name__ == "__main__":
    asyncio.run(main())
