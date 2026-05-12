#!/usr/bin/env python3
"""
Example script to check if an API key has FIX API permissions.

This script demonstrates how to use the check_fix_api_permissions function
to verify that your API key has the necessary permissions for FIX API access.
"""

import asyncio
import os

from binance_fix_connector_async import (
    check_fix_api_permissions,
    validate_fix_permissions_for_session,
)


async def main():
    """Check FIX API permissions for a given API key."""
    # Get mainnet HMAC API credentials from environment variables
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")

    if not api_key or not api_secret:
        print("Error: Please set BINANCE_API_KEY and BINANCE_API_SECRET environment variables")
        print("Example:")
        print("  export BINANCE_API_KEY='your_api_key_here'")
        print("  export BINANCE_API_SECRET='your_api_secret_here'")
        return

    base_url = os.getenv("BINANCE_API_URL", "https://api.binance.com")
    if "testnet.binance.vision" in base_url:
        print("Error: Binance Spot Testnet does not support /sapi endpoints.")
        print("Use a real FIX logon to validate testnet Ed25519 FIX permissions.")
        return

    print(f"Checking FIX API permissions for API key: {api_key[:8]}...")
    print(f"Using API endpoint: {base_url}")
    print("-" * 50)

    try:
        # Check permissions
        permissions = await check_fix_api_permissions(api_key, api_secret, base_url)

        # Display results
        print("\nPermission Check Results:")
        print(f"  FIX_API enabled: {permissions['has_fix_api']}")
        print(f"  FIX_API_READ_ONLY enabled: {permissions['has_fix_api_read_only']}")
        print(f"  Can use Drop Copy: {permissions['can_use_drop_copy']}")

        print("\n" + "-" * 50)
        print("\nSession Type Compatibility:")

        # Check each session type
        session_types = ["order_entry", "market_data", "drop_copy"]
        for session_type in session_types:
            is_valid, error_msg = validate_fix_permissions_for_session(permissions, session_type)
            status = "✓ Compatible" if is_valid else f"✗ Incompatible - {error_msg}"
            print(f"  {session_type.replace('_', ' ').title()}: {status}")

        print("\n" + "-" * 50)
        print("\nFull API Response:")

        # Display all permission fields
        response = permissions["raw_response"]
        for key, value in response.items():
            print(f"  {key}: {value}")

        print("\n" + "-" * 50)
        print("\nRecommendations:")

        if not permissions["has_fix_api"] and not permissions["has_fix_api_read_only"]:
            print("  ⚠️  No FIX API permissions found!")
            print("  To enable FIX API access:")
            print("  1. Log in to your Binance account")
            print("  2. Go to API Management")
            print("  3. Edit your API key")
            print("  4. Enable 'FIX API' or 'FIX API Read Only' permission")
            print("  5. Make sure you're using an Ed25519 key (not HMAC)")
        elif not permissions["has_fix_api"] and permissions["has_fix_api_read_only"]:
            print("  ℹ️  You have read-only FIX API access.")
            print("  You can use Market Data and Drop Copy sessions.")
            print("  To place orders, enable the full 'FIX API' permission.")
        else:
            print("  ✅ Your API key has full FIX API access!")
            print("  You can use all FIX session types.")

    except Exception as e:
        print(f"\nError checking permissions: {e}")
        print("\nCommon issues:")
        print("  1. Invalid API key or secret")
        print("  2. IP restrictions preventing access")
        print("  3. API endpoint not accessible")
        print("  4. Network connectivity issues")


if __name__ == "__main__":
    asyncio.run(main())
