#!/usr/bin/env python3
"""End-to-End Order Lifecycle Tests."""

import asyncio
import importlib.util
import logging
import time
from pathlib import Path

import pytest

from tests.test_e2e_framework import BaseE2ETest

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


class TestCompleteOrderLifecycle(BaseE2ETest):
    """Test complete order lifecycle scenarios."""

    async def test_basic_order_lifecycle_mocked(self):
        """Test basic order lifecycle with mocked responses."""
        async with self.create_test_session("order_entry", use_real_testnet=False) as (session, metrics):
            # Test logon
            await session.logon(recv_window="5000")
            self.assertEqual(metrics.messages_sent, 1)

            # Create new order message
            order_msg = await self._create_order(
                session,
                symbol="BTCUSDT",
                side="1",  # Buy
                order_type="2",  # Limit
                quantity="0.001",
                price="50000.00",
            )

            # Send order
            await session.send_message(order_msg)
            metrics.messages_sent += 1

            # Test logout
            await session.logout()
            metrics.messages_sent += 1

            # Verify metrics
            self.assert_session_metrics(metrics, min_messages=2, max_errors=0)

    @pytest.mark.requires_testnet
    async def test_complete_order_lifecycle_testnet(self):
        """Test complete order lifecycle against real testnet."""
        if not self.credentials.has_real_credentials:
            self.skipTest("Real testnet credentials not available")

        async with self.create_test_session("order_entry", use_real_testnet=True) as (session, metrics):
            # Step 1: Session is already authenticated by factory function
            await asyncio.sleep(1)  # Allow time for any pending responses

            initial_messages = await session.get_all_new_messages_received()
            self._check_auto_logon_rejection(session, initial_messages)

            # Session is already authenticated, proceed with order operations

            # Step 2: Place order
            params = await get_safe_limit_order_params("BTCUSDT", "1", target_quantity="0.001")
            order_msg = await self._create_order(
                session,
                symbol="BTCUSDT",
                side="1",  # Buy
                order_type="2",  # Limit
                quantity=params.quantity,
                price=params.price,
            )

            await session.send_message(order_msg)

            # Step 3: Wait for execution report
            try:
                execution_responses = await self.wait_for_messages(
                    session,
                    ["8"],
                    timeout=10,  # Execution Report
                )
                self.assertGreater(len(execution_responses), 0)
            except TimeoutError:
                all_messages = await session.get_all_new_messages_received()
                for msg in all_messages:
                    msg_type = msg.get("35")
                    if msg_type and msg_type.decode() == "3":  # Reject
                        reject_reason = msg.get("58", b"Unknown").decode()
                        self.fail(f"Order rejected: {reject_reason}")
                    elif msg_type and msg_type.decode() == "9":  # Order Cancel Reject
                        reject_reason = msg.get("58", b"Unknown").decode()
                        self.fail(f"Order cancel rejected: {reject_reason}")
                self.fail("No execution report received")
            execution_report = execution_responses[-1]
            exec_status = execution_report.get("39")
            if exec_status and exec_status.decode() == "8":
                reject_text = execution_report.get("58")
                reject_reason = reject_text.decode() if reject_text else "Unknown"
                self.fail(f"Order rejected: {reject_reason}")

            self.assert_message_structure(
                execution_report,
                "8",  # Execution Report
                required_fields=["11", "14", "17", "37", "39", "150"],
            )

            # Step 4: Cancel order if it's still open
            self.assertIsNotNone(exec_status, "Execution report missing OrdStatus (39)")
            self.assertIn(exec_status.decode(), ["0", "1"])
            cancel_msg = await self._create_cancel_order(
                session,
                original_order_id=order_msg.get("11").decode(),
                symbol="BTCUSDT",
            )

            await session.send_message(cancel_msg)

            cancel_responses = await self.wait_for_messages(session, ["8"], timeout=10)
            self.assertGreater(len(cancel_responses), 0)
            cancel_report = cancel_responses[-1]
            cancel_status = cancel_report.get("39")
            self.assertIsNotNone(cancel_status, "Cancel report missing OrdStatus (39)")
            self.assertEqual(cancel_status.decode(), "4")

            # Step 5: Session will auto-logout on context exit

            # Verify overall session metrics
            metrics.messages_sent = len(session.messages_sent)
            metrics.messages_received = len(session._message_history)
            self.assert_session_metrics(
                metrics,
                min_messages=4,
                max_errors=0,
                max_duration=60.0,
            )

    async def test_multiple_orders_sequential(self):
        """Test placing multiple orders sequentially."""
        async with self.create_test_session("order_entry", use_real_testnet=False) as (session, metrics):
            await session.logon()

            # Place 5 orders sequentially
            order_ids = []
            for i in range(5):
                order_msg = await self._create_order(
                    session,
                    order_id=f"SEQ_{int(time.time())}_{i}",
                    symbol="BTCUSDT",
                    side="1",
                    order_type="2",
                    quantity=f"0.00{i + 1}",
                    price=f"{25000 + i * 100}.00",
                )

                order_ids.append(order_msg.get("11").decode())
                await session.send_message(order_msg)

                # Small delay between orders
                await asyncio.sleep(0.1)

            await session.logout()

            # Verify all orders were sent
            self.assertEqual(len(order_ids), 5)
            self.assertEqual(len(set(order_ids)), 5)  # All unique
            self.assertGreaterEqual(metrics.messages_sent, 7)  # Logon + 5 orders + logout

    @pytest.mark.load_test
    async def test_high_frequency_order_submission(self):
        """Test high-frequency order submission to validate performance."""
        async with self.create_test_session("order_entry", use_real_testnet=False) as (session, metrics):
            await session.logon()

            # Submit orders rapidly
            start_time = time.time()
            order_count = 50

            for i in range(order_count):
                order_msg = await self._create_order(
                    session,
                    symbol="BTCUSDT",
                    side="1" if i % 2 == 0 else "2",  # Alternate buy/sell
                    order_type="2",
                    quantity="0.001",
                    price=f"{25000 + (i % 10) * 100}.00",
                )

                await session.send_message(order_msg)

            submission_time = time.time() - start_time
            await session.logout()

            # Performance assertions
            orders_per_second = order_count / submission_time
            self.assertGreater(orders_per_second, 10)  # At least 10 orders/second
            self.assertLessEqual(submission_time, 10.0)  # Complete within 10 seconds

            # Metrics validation
            self.assertGreaterEqual(metrics.messages_sent, order_count + 2)  # Orders + logon/logout

    @pytest.mark.error_scenario
    async def test_invalid_order_handling(self):
        """Test handling of invalid orders and error responses."""
        async with self.create_test_session("order_entry", use_real_testnet=False) as (session, metrics):
            await session.logon()

            # Create invalid order (negative quantity)
            invalid_order = await self._create_order(
                session,
                symbol="INVALIDPAIR",
                side="1",
                order_type="2",
                quantity="-0.001",  # Invalid negative quantity
                price="50000.00",
            )

            await session.send_message(invalid_order)

            # In real testnet, this would generate reject/error responses
            # For mocked tests, we just verify the message was sent
            await session.logout()

            # Should complete without exceptions
            self.assertGreaterEqual(metrics.messages_sent, 3)

    async def test_order_state_tracking(self):
        """Test order state transitions and tracking."""
        order_states = []

        async with self.create_test_session("order_entry", use_real_testnet=False) as (session, _metrics):
            await session.logon()

            # Create order with state tracking
            order_msg = await self._create_order(
                session,
                symbol="BTCUSDT",
                side="1",
                order_type="2",
                quantity="0.001",
                price="25000.00",
            )

            order_id = order_msg.get("11").decode()
            order_states.append(("NEW", order_id, time.time()))

            await session.send_message(order_msg)
            order_states.append(("SENT", order_id, time.time()))

            # Simulate cancel
            cancel_msg = await self._create_cancel_order(
                session,
                original_order_id=order_id,
                symbol="BTCUSDT",
            )

            await session.send_message(cancel_msg)
            order_states.append(("CANCEL_SENT", order_id, time.time()))

            await session.logout()

        # Verify state transitions
        self.assertEqual(len(order_states), 3)
        self.assertEqual(order_states[0][0], "NEW")
        self.assertEqual(order_states[1][0], "SENT")
        self.assertEqual(order_states[2][0], "CANCEL_SENT")

        # Verify timing sequence
        for i in range(1, len(order_states)):
            self.assertGreater(order_states[i][2], order_states[i - 1][2])

    @pytest.mark.error_scenario
    async def test_duplicate_order_id(self):
        """Test handling of duplicate order IDs."""
        async with self.create_test_session("order_entry") as (session, metrics):
            await session.logon()

            # Use same order ID for two orders
            duplicate_id = f"DUPLICATE_{int(time.time())}"

            # First order
            order1 = await session.create_fix_message_with_basic_header("D", "5000")
            order1.append_pair("11", duplicate_id)
            order1.append_pair("55", "BTCUSDT")
            order1.append_pair("54", "1")
            order1.append_pair("40", "2")
            order1.append_pair("38", "0.001")
            order1.append_pair("44", "25000.00")
            order1.append_pair("59", "1")
            order1.append_pair("60", session.current_utc_time())

            # Second order with same ID
            order2 = await session.create_fix_message_with_basic_header("D", "5000")
            order2.append_pair("11", duplicate_id)  # Same ID
            order2.append_pair("55", "ETHUSDT")
            order2.append_pair("54", "2")
            order2.append_pair("40", "2")
            order2.append_pair("38", "0.01")
            order2.append_pair("44", "2000.00")
            order2.append_pair("59", "1")
            order2.append_pair("60", session.current_utc_time())

            await session.send_message(order1)
            await session.send_message(order2)

            await session.logout()

            # Should complete without throwing exceptions
            self.assertGreaterEqual(metrics.messages_sent, 4)

    @pytest.mark.error_scenario
    async def test_cancel_non_existent_order(self):
        """Test canceling an order that doesn't exist."""
        async with self.create_test_session("order_entry") as (session, metrics):
            await session.logon()

            cancel_msg = await self._create_cancel_order(
                session,
                original_order_id=f"NONEXISTENT_{int(time.time())}",
            )
            await session.send_message(cancel_msg)

            await session.logout()
            self.assertGreaterEqual(metrics.messages_sent, 3)

    @pytest.mark.load_test
    async def test_order_throughput_measurement(self):
        """Measure order processing throughput."""
        async with self.create_test_session("order_entry") as (session, _metrics):
            await session.logon()

            # Measure throughput over time period
            test_duration = 5.0  # seconds
            start_time = time.time()
            order_count = 0

            while time.time() - start_time < test_duration:
                order_msg = await self._create_order(
                    session, f"PERF_{int(time.time())}_{order_count}", price="25000.00"
                )
                await session.send_message(order_msg)
                order_count += 1

                # Small delay to prevent overwhelming
                await asyncio.sleep(0.01)

            actual_duration = time.time() - start_time
            await session.logout()

            # Calculate metrics
            throughput = order_count / actual_duration

            # Performance assertions
            self.assertGreater(throughput, 5.0)  # At least 5 orders/second
            self.assertGreater(order_count, 20)  # At least 20 orders in test

            # Log performance results
            logger.info("Order throughput: %.2f orders/second", throughput)
            logger.info("Total orders: %s in %.2fs", order_count, actual_duration)

    @pytest.mark.load_test
    async def test_concurrent_order_submission(self):
        """Test concurrent order submission performance."""
        async with self.create_test_session("order_entry") as (session, _metrics):
            await session.logon()

            # Submit orders concurrently
            concurrent_count = 10

            async def submit_order(order_index: int):
                order_msg = await self._create_order(
                    session, f"PERF_{int(time.time())}_{order_index}", price="25000.00"
                )
                await session.send_message(order_msg)
                return order_index

            start_time = time.time()

            # Execute concurrent order submissions
            tasks = [submit_order(i) for i in range(concurrent_count)]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            submission_time = time.time() - start_time
            await session.logout()

            # Verify all orders submitted successfully
            successful_orders = [r for r in results if not isinstance(r, Exception)]
            self.assertEqual(len(successful_orders), concurrent_count)

            # Performance assertions
            self.assertLess(submission_time, 5.0)  # Should complete quickly

            concurrent_throughput = concurrent_count / submission_time
            logger.info("Concurrent throughput: %.2f orders/second", concurrent_throughput)


if __name__ == "__main__":
    pytest.main([__file__])
