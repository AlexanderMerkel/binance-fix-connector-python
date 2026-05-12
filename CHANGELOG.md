# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.0] - 2026-02-12

### Added
- Automatic session restart on NEWS messages (`schedule_restart`, `reconnect`, `_restart_timer`)
- `restart` parameter on `BinanceFixConnector` constructor to control auto-restart behavior
- `retrieve_messages_until` now accepts `str | list[str]` for message_type matching
- `retrieve_messages_until` now accepts `message_cl_ord_id` parameter for ClOrdID-based filtering
- `FixMsgTypes.NEWS = "B"` constant
- `_sanitize_fix_message` helper to filter sensitive tags from log output
- `_build_sender_comp_id` helper to eliminate repeated prefix truncation logic
- `py.typed` PEP 561 marker file for type checker support
- `__version__` attribute in package `__init__.py`
- `CHANGELOG.md`
- `examples/common.py` shared credential loading utility
- `[project.urls]` section in `pyproject.toml`
- `[project.optional-dependencies] dev` group in `pyproject.toml`

### Fixed
- **Message parsing bugfix**: Filter changed from `if x != ""` to `if "=" in x and not x.startswith("=")` (ported from upstream v1.2.0, fixes ValueError on malformed messages)
- Typo in error message: "ed25219" corrected to "Ed25519"
- `drop_copy_flag` and `reset_seq_num_flag` type annotations normalized to `str` (were incorrectly typed as `bool`)
- Sensitive data (Ed25519 signatures, credentials) no longer logged in plaintext
- `logging.basicConfig()` moved from instance constructor to module level
- `__data` renamed to `_data` (single underscore) for consistent naming convention
- Event loop cached in `retrieve_messages_until` to avoid repeated `get_event_loop()` calls
- Redundant `import logging` removed from `create_drop_copy_session`

### Changed
- `black` moved from production dependencies to `[project.optional-dependencies] dev`
- Pytest configuration consolidated from `pytest.ini` into `pyproject.toml`
- CI workflow updated from `unittest` to `pytest`
- CI now installs dev dependencies via `pip install -e ".[dev]"`
- Release workflow actions updated to v4/v5
- `examples/maket_stream/` renamed to `examples/market_stream/` (typo fix)
- Logger names now include sender_comp_id (e.g., `BinanceFixConnector.BOETRDE`)

### Removed
- 7 empty test stub files (were just `pass` placeholders)
- `pytest.ini` (consolidated into `pyproject.toml`)

## [1.0.1] - 2025-06-10

### Added
- Initial async fork of [binance-fix-connector-python](https://github.com/binance/binance-fix-connector-python) v1.0.1
- Full async/await conversion of all I/O operations
- `asyncio.Lock`, `asyncio.Queue`, `asyncio.StreamReader/Writer` replacing threading primitives
- Permission checking utilities (`check_fix_api_permissions`, `validate_fix_permissions_for_session`)
- Enhanced error diagnostics on `ConnectionResetError` during logon
- `aiohttp` dependency for async HTTP calls
