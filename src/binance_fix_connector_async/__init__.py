"""Async version of the Binance FIX Connector."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("binance-fix-connector-async")
except PackageNotFoundError:
    __version__ = "0.0.0"

from .fix_connector import (
    FIX_DC_URL,
    FIX_MD_URL,
    FIX_OE_URL,
    BinanceFixConnector,
    FixMsgTypes,
    FixTags,
    create_drop_copy_session,
    create_market_data_session,
    create_order_entry_session,
)
from .utils import (
    SessionType,
    check_fix_api_permissions,
    get_api_key,
    get_private_key,
    validate_fix_permissions_for_session,
)

__all__ = [
    "FIX_DC_URL",
    "FIX_MD_URL",
    "FIX_OE_URL",
    "BinanceFixConnector",
    "FixMsgTypes",
    "FixTags",
    "SessionType",
    "check_fix_api_permissions",
    "create_drop_copy_session",
    "create_market_data_session",
    "create_order_entry_session",
    "get_api_key",
    "get_private_key",
    "validate_fix_permissions_for_session",
]
