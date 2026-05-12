#!/usr/bin/env python3
"""
Drop copy session with permission checking.

Demonstrates using the permission checking feature when creating a drop copy
session to ensure the API key has the required permissions before connecting.
"""

import asyncio
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from common import TESTNET_DC_URL, graceful_logout, load_credentials

API_KEY, PRIVATE_KEY = load_credentials()

from binance_fix_connector_async import FIX_DC_URL, create_drop_copy_session


async def main():
    api_secret = os.getenv("BINANCE_API_SECRET")
    use_testnet = os.getenv("USE_TESTNET", "true").lower() == "true"
    endpoint = TESTNET_DC_URL if use_testnet else FIX_DC_URL
    permission_base_url = os.getenv("BINANCE_API_URL", "https://api.binance.com")

    print(f"Creating drop copy session (endpoint: {endpoint})")

    try:
        kwargs = {"api_key": API_KEY, "private_key": PRIVATE_KEY, "endpoint": endpoint}
        if api_secret and os.getenv("CHECK_FIX_PERMISSIONS", "false").lower() == "true":
            if use_testnet:
                print("Skipping REST permission check on testnet; FIX logon will validate key permissions.")
            else:
                kwargs.update(check_permissions=True, hmac_secret=api_secret, permission_base_url=permission_base_url)

        session = await create_drop_copy_session(**kwargs)

        try:
            messages = await session.retrieve_messages_until("A", timeout_seconds=5)
            if messages:
                for msg in messages:
                    msg_type = msg.get("35")
                    if msg_type:
                        print(f"  Message Type: {msg_type.decode()}")
                    if msg_type and msg_type.decode() == "3":
                        text = msg.get("58")
                        if text:
                            print(f"  Reject Reason: {text.decode()}")

            print("Drop copy session established. Waiting for execution reports...")
            await asyncio.sleep(2)

        except ConnectionResetError as e:
            print(f"Connection reset during logon: {e}")
            print("Check: FIX API permissions, Ed25519 key type, credentials")

        finally:
            await graceful_logout(session)

    except Exception as e:
        print(f"Error: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
