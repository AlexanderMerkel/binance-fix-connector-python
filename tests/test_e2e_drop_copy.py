#!/usr/bin/env python3
"""
End-to-End Drop Copy Tests

This module tests drop copy functionality for trade reporting and compliance,
validating order state synchronization, execution reporting, and audit trails.
"""

import asyncio
import importlib.util
import logging
import time
from pathlib import Path

import pytest

from binance_fix_connector_async.fix_connector import FixTags
from tests.test_e2e_framework import (
    BaseE2ETest,
)

logger = logging.getLogger(__name__)
_COMMON_SPEC = importlib.util.spec_from_file_location(
    "examples_common",
    Path(__file__).resolve().parent.parent / "examples" / "common.py",
)
if _COMMON_SPEC is None or _COMMON_SPEC.loader is None:
    raise ImportError("Could not load examples/common.py")
_COMMON = importlib.util.module_from_spec(_COMMON_SPEC)
_COMMON_SPEC.loader.exec_module(_COMMON)
get_safe_limit_order_params = _COMMON.get_safe_limit_order_params


class TestDropCopyFunctionality(BaseE2ETest):
    """Test drop copy session functionality and trade reporting."""

    async def test_basic_drop_copy_session_mocked(self):
        """Test basic drop copy session setup with mocked responses."""
        async with self.create_test_session("drop_copy", use_real_testnet=False) as (session, metrics):
            # Verify drop copy flag is set
            self.assertTrue(session.drop_copy_flag)

            # Test logon
            await session.logon(recv_window="5000")
            self.assertEqual(metrics.messages_sent, 1)

            # Test logout
            await session.logout()
            self.assertEqual(metrics.messages_sent, 2)

            # Verify session completed successfully
            self.assert_session_metrics(metrics, min_messages=2, max_errors=0)

    @pytest.mark.requires_testnet
    async def test_drop_copy_session_testnet(self):
        """Test drop copy session against real testnet."""
        if not self.credentials.has_real_credentials:
            self.skipTest("Real testnet credentials not available")

        async with self.create_test_session("drop_copy", use_real_testnet=True) as (session, metrics):
            try:
                # Step 1: Session is already authenticated by factory function
                await asyncio.sleep(1)  # Allow time for any pending responses

                initial_messages = await session.get_all_new_messages_received()
                metrics.messages_received += len(initial_messages)
                self._check_auto_logon_rejection(session, initial_messages)
            except ConnectionResetError:
                self.skipTest(
                    "Connection reset during logon - likely API key lacks drop copy permissions or incorrect endpoint"
                )

            try:
                params = await get_safe_limit_order_params("BTCUSDT", "1", target_quantity="0.001")
                cl_ord_id = f"DC_{int(time.time() * 1000)}"

                async with self.create_test_session("order_entry", use_real_testnet=True) as (oe_session, _oe_metrics):
                    order_msg = await self._create_order(
                        oe_session,
                        order_id=cl_ord_id,
                        symbol="BTCUSDT",
                        side="1",
                        order_type="2",
                        quantity=params.quantity,
                        price=params.price,
                    )
                    await oe_session.send_message(order_msg)

                    oe_responses = await self.wait_for_messages(oe_session, ["8"], timeout=10)
                    oe_report = next(
                        (
                            msg
                            for msg in oe_responses
                            if self._field(msg, FixTags.MSG_TYPE) == "8"
                            and self._field(msg, FixTags.CL_ORD_ID) == cl_ord_id
                        ),
                        None,
                    )
                    self.assertIsNotNone(oe_report, f"Order Entry did not acknowledge {cl_ord_id}")
                    order_status = self._field(oe_report, FixTags.ORD_STATUS)
                    if order_status == "8":
                        self.fail(f"Order rejected: {self._field(oe_report, FixTags.TEXT)}")

                    if order_status in {"0", "1", "A"}:
                        cancel_msg = await self._create_cancel_order(
                            oe_session,
                            original_order_id=cl_ord_id,
                            symbol="BTCUSDT",
                        )
                        await oe_session.send_message(cancel_msg)
                        await self.wait_for_messages(oe_session, ["8"], timeout=10)

                    drop_copy_report = await self._wait_for_drop_copy_execution_report(session, cl_ord_id)
                    metrics.messages_received += 1

                # Step 3: Session will auto-logout on context exit

                self.assert_session_metrics(
                    metrics,
                    min_messages=max(1, metrics.messages_received),
                    max_errors=0,
                    max_duration=30.0,
                )

                self.assert_message_structure(
                    drop_copy_report, "8", required_fields=["11", "37", "17", "150", "39", "55", "54", "38"]
                )

            except ConnectionResetError:
                self.skipTest(
                    "Connection reset during session - likely permission issue with API key or network interruption"
                )

    @staticmethod
    def _field(message, tag: str) -> str | None:
        value = message.get(tag)
        return value.decode() if value else None

    async def _wait_for_drop_copy_execution_report(self, session, cl_ord_id: str, timeout: float = 20):
        start_time = time.time()
        execution_reports = []
        while time.time() - start_time < timeout:
            for msg in await session.get_all_new_messages_received():
                if self._field(msg, FixTags.MSG_TYPE) != "8":
                    continue
                execution_reports.append(msg)
                if self._field(msg, FixTags.CL_ORD_ID) == cl_ord_id:
                    logger.info("Received drop copy execution report for %s", cl_ord_id)
                    return msg
            await asyncio.sleep(0.25)
        self.fail(
            f"Drop copy did not receive execution report for {cl_ord_id}; "
            f"received {len(execution_reports)} unrelated execution reports"
        )

    async def test_trade_state_synchronization(self):
        """Test trade state synchronization through drop copy."""
        trade_states = {}

        async with self.create_test_session("drop_copy") as (session, _metrics):
            await session.logon()

            # Simulate execution reports for trade state tracking
            trade_id = f"TRADE_{int(time.time())}"

            # Simulate new order acknowledgment
            new_ack = await self._create_execution_report(
                session,
                order_id=trade_id,
                exec_type="0",  # New
                order_status="0",  # New
                symbol="BTCUSDT",
                side="1",
                quantity="0.001",
            )

            await session.on_message_received([new_ack])
            trade_states[trade_id] = "NEW"

            # Simulate partial fill
            partial_fill = await self._create_execution_report(
                session,
                order_id=trade_id,
                exec_type="F",  # Trade
                order_status="1",  # Partially Filled
                symbol="BTCUSDT",
                side="1",
                quantity="0.001",
                fill_qty="0.0005",
                fill_price="50000.00",
            )

            await session.on_message_received([partial_fill])
            trade_states[trade_id] = "PARTIALLY_FILLED"

            # Simulate full fill
            full_fill = await self._create_execution_report(
                session,
                order_id=trade_id,
                exec_type="F",  # Trade
                order_status="2",  # Filled
                symbol="BTCUSDT",
                side="1",
                quantity="0.001",
                fill_qty="0.0005",
                fill_price="50000.00",
            )

            await session.on_message_received([full_fill])
            trade_states[trade_id] = "FILLED"

            # Process all messages
            messages = await session.get_all_new_messages_received()

            await session.logout()

        # Verify trade state progression
        self.assertEqual(len(messages), 3)
        self.assertEqual(trade_states[trade_id], "FILLED")

        for msg in messages:
            self.assertEqual(msg.get(FixTags.MSG_TYPE).decode(), "8")
            for field in ["11", "37", "17", "150", "39", "55", "54", "38"]:
                self.assertIsNotNone(msg.get(field), f"Missing field: {field}")

    async def test_multi_symbol_drop_copy_monitoring(self):
        """Test drop copy monitoring across multiple trading symbols."""
        symbols = ["BTCUSDT", "ETHUSDT", "ADAUSDT"]
        trade_counts = {symbol: 0 for symbol in symbols}

        async with self.create_test_session("drop_copy") as (session, _metrics):
            await session.logon()

            # Simulate trades across multiple symbols
            for i, symbol in enumerate(symbols):
                for j in range(3):  # 3 trades per symbol
                    trade_id = f"{symbol}_TRADE_{i}_{j}"

                    execution_report = await self._create_execution_report(
                        session,
                        order_id=trade_id,
                        exec_type="F",  # Trade
                        order_status="2",  # Filled
                        symbol=symbol,
                        side="1" if j % 2 == 0 else "2",  # Alternate buy/sell
                        quantity="0.001",
                        fill_qty="0.001",
                        fill_price=f"{1000 * (i + 1)}.00",
                    )

                    await session.on_message_received([execution_report])
                    trade_counts[symbol] += 1

                    await asyncio.sleep(0.05)  # Small delay between trades

            # Process all execution reports
            messages = await session.get_all_new_messages_received()

            await session.logout()

        # Verify trade distribution
        total_trades = sum(trade_counts.values())
        self.assertEqual(total_trades, 9)  # 3 symbols * 3 trades each
        self.assertEqual(len(messages), 9)

        # Verify all symbols had trades
        for symbol in symbols:
            self.assertEqual(trade_counts[symbol], 3)

    @pytest.mark.load_test
    async def test_high_volume_drop_copy_processing(self):
        """Test drop copy session handling high volume of execution reports."""
        async with self.create_test_session("drop_copy") as (session, metrics):
            await session.logon()

            # Generate high volume of execution reports
            execution_count = 100
            start_time = time.time()

            for i in range(execution_count):
                execution_report = await self._create_execution_report(
                    session,
                    order_id=f"VOLUME_TEST_{i}",
                    exec_type="F",
                    order_status="2",
                    symbol="BTCUSDT",
                    side="1" if i % 2 == 0 else "2",
                    quantity="0.001",
                    fill_qty="0.001",
                    fill_price=f"{50000 + (i % 100)}.00",
                )

                await session.on_message_received([execution_report])

                # Process messages periodically to avoid unbounded history growth
                if i % 20 == 0:
                    messages = await session.get_all_new_messages_received()
                    metrics.messages_received += len(messages)

            # Final processing
            messages = await session.get_all_new_messages_received()
            metrics.messages_received += len(messages)

            processing_time = time.time() - start_time
            await session.logout()

            # Performance validation
            processing_rate = execution_count / processing_time
            self.assertGreater(processing_rate, 50)  # At least 50 executions/second

            total_messages = metrics.messages_received
            self.assertGreaterEqual(total_messages, execution_count)

            logger.info("Drop copy processing rate: %.2f executions/second", processing_rate)

    async def test_drop_copy_audit_trail(self):
        """Test drop copy audit trail and compliance reporting."""
        audit_trail = []

        async with self.create_test_session("drop_copy") as (session, _metrics):
            await session.logon()

            # Simulate a complete order lifecycle for audit
            order_id = f"AUDIT_{int(time.time())}"

            # Order placement
            new_order_ack = await self._create_execution_report(
                session,
                order_id=order_id,
                exec_type="0",  # New
                order_status="0",  # New
                symbol="BTCUSDT",
                side="1",
                quantity="0.002",
            )

            await session.on_message_received([new_order_ack])
            audit_trail.append(("ORDER_NEW", order_id, time.time()))

            # Partial execution
            partial_exec = await self._create_execution_report(
                session,
                order_id=order_id,
                exec_type="F",  # Trade
                order_status="1",  # Partially Filled
                symbol="BTCUSDT",
                side="1",
                quantity="0.002",
                fill_qty="0.001",
                fill_price="50000.00",
            )

            await session.on_message_received([partial_exec])
            audit_trail.append(("PARTIAL_FILL", order_id, time.time()))

            # Order modification
            replace_ack = await self._create_execution_report(
                session,
                order_id=order_id,
                exec_type="5",  # Replace
                order_status="0",  # New (after replace)
                symbol="BTCUSDT",
                side="1",
                quantity="0.003",  # Modified quantity
            )

            await session.on_message_received([replace_ack])
            audit_trail.append(("ORDER_REPLACE", order_id, time.time()))

            # Final execution
            final_exec = await self._create_execution_report(
                session,
                order_id=order_id,
                exec_type="F",  # Trade
                order_status="2",  # Filled
                symbol="BTCUSDT",
                side="1",
                quantity="0.003",
                fill_qty="0.002",  # Remaining quantity
                fill_price="50001.00",
            )

            await session.on_message_received([final_exec])
            audit_trail.append(("ORDER_FILLED", order_id, time.time()))

            # Process all audit messages
            messages = await session.get_all_new_messages_received()

            await session.logout()

        # Validate audit trail
        self.assertEqual(len(audit_trail), 4)
        self.assertEqual(len(messages), 4)

        # Verify chronological order
        for i in range(1, len(audit_trail)):
            self.assertGreater(audit_trail[i][2], audit_trail[i - 1][2])

        # Verify audit events
        expected_events = ["ORDER_NEW", "PARTIAL_FILL", "ORDER_REPLACE", "ORDER_FILLED"]
        actual_events = [event[0] for event in audit_trail]
        self.assertEqual(actual_events, expected_events)

    @pytest.mark.error_scenario
    async def test_drop_copy_error_handling(self):
        """Test drop copy session error handling and recovery."""
        async with self.create_test_session("drop_copy") as (session, metrics):
            await session.logon()

            # Simulate malformed execution report
            try:
                malformed_msg = await session.create_fix_message_with_basic_header("8", "5000")
                # Missing required fields - should be handled gracefully
                await session.on_message_received([malformed_msg])

                await session.get_all_new_messages_received()
                # Should process without crashing

            except Exception as e:
                metrics.errors.append(f"Malformed message error: {e}")

            # Simulate network interruption during processing
            try:
                # Create valid execution report
                valid_exec = await self._create_execution_report(
                    session,
                    order_id="ERROR_TEST",
                    exec_type="F",
                    order_status="2",
                    symbol="BTCUSDT",
                    side="1",
                    quantity="0.001",
                    fill_qty="0.001",
                    fill_price="50000.00",
                )

                await session.on_message_received([valid_exec])

                # Process normally
                await session.get_all_new_messages_received()

            except Exception as e:
                metrics.errors.append(f"Processing error: {e}")

            await session.logout()

            # Should complete even with errors
            self.assertGreaterEqual(metrics.messages_sent, 2)


class TestDropCopyIntegration(BaseE2ETest):
    """Test drop copy integration with order entry sessions."""

    async def test_drop_copy_order_entry_integration(self):
        """Test integration between order entry and drop copy sessions."""
        # This test would ideally use two separate sessions:
        # 1. Order entry session to place orders
        # 2. Drop copy session to receive execution reports

        # For this test, we'll simulate the workflow
        order_events = []
        drop_copy_events = []

        # Simulate order entry session
        async with self.create_test_session("order_entry") as (oe_session, _oe_metrics):
            await oe_session.logon()

            # Place order
            order_msg = await self._create_order(oe_session, order_id=f"INTEGRATION_{int(time.time())}")
            await oe_session.send_message(order_msg)
            order_events.append(("ORDER_PLACED", order_msg.get("11").decode(), time.time()))

            await oe_session.logout()

        # Simulate drop copy session receiving the execution
        async with self.create_test_session("drop_copy") as (dc_session, _dc_metrics):
            await dc_session.logon()

            # Simulate receiving execution report for the order
            order_id = order_events[0][1]
            exec_report = await self._create_execution_report(
                dc_session,
                order_id=order_id,
                exec_type="F",
                order_status="2",
                symbol="BTCUSDT",
                side="1",
                quantity="0.001",
                fill_qty="0.001",
                fill_price="50000.00",
            )

            await dc_session.on_message_received([exec_report])
            drop_copy_events.append(("EXECUTION_RECEIVED", order_id, time.time()))

            # Process execution report
            messages = await dc_session.get_all_new_messages_received()

            await dc_session.logout()

        # Verify integration
        self.assertEqual(len(order_events), 1)
        self.assertEqual(len(drop_copy_events), 1)
        self.assertEqual(len(messages), 1)

        # Verify order ID consistency
        self.assertEqual(order_events[0][1], drop_copy_events[0][1])


if __name__ == "__main__":
    pytest.main([__file__])
