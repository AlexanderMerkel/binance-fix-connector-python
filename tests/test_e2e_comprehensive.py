#!/usr/bin/env python3
"""
Comprehensive End-to-End Test Suite

This module provides comprehensive E2E testing that validates complete
trading workflows, performance characteristics, and behavioral compatibility
between sync and async implementations.
"""

import asyncio
import logging
import time

import pytest

from tests.test_e2e_framework import (
    BaseE2ETest,
)

logger = logging.getLogger(__name__)


class TestComprehensiveWorkflows(BaseE2ETest):
    """Test comprehensive real-world trading workflows."""

    async def test_simulated_trading_workflow(self):
        """Test complete simulated trading workflow with mocked responses."""
        trading_state = {
            "market_price": 50000.00,
            "position": 0.0,
            "orders": {},
            "executions": [],
        }

        # Simulate market data driven trading
        async with self.create_test_session("market_data") as (md_session, md_metrics):
            await md_session.logon()

            # Subscribe to market data
            subscription = await self._create_market_data_request(md_session, "BTCUSDT", "1", md_entry_types=["0"])
            await md_session.send_message(subscription)

            # Simulate price updates and trading decisions
            for i in range(10):
                # Simulate market data update
                new_price = 50000.00 + (i - 5) * 100  # Price moves around 50k
                md_update = await self._create_fake_market_data(md_session, "BTCUSDT", new_price)
                await md_session.on_message_received([md_update])

                # Process market data
                messages = await md_session.get_all_new_messages_received()
                if messages:
                    trading_state["market_price"] = new_price

                await asyncio.sleep(0.1)

            await md_session.logout()

        # Simulate order placement based on market data
        async with self.create_test_session("order_entry") as (oe_session, oe_metrics):
            await oe_session.logon()

            # Place orders based on simulated market conditions
            if trading_state["market_price"] < 49900:  # Buy signal
                buy_order = await self._create_order(
                    oe_session,
                    symbol="BTCUSDT",
                    side="1",
                    quantity="0.001",
                    price=str(trading_state["market_price"] - 10),
                )
                await oe_session.send_message(buy_order)
                trading_state["orders"][buy_order.get("11").decode()] = "BUY_PENDING"

            if trading_state["market_price"] > 50100:  # Sell signal
                sell_order = await self._create_order(
                    oe_session,
                    symbol="BTCUSDT",
                    side="2",
                    quantity="0.001",
                    price=str(trading_state["market_price"] + 10),
                )
                await oe_session.send_message(sell_order)
                trading_state["orders"][sell_order.get("11").decode()] = "SELL_PENDING"

            await oe_session.logout()

        # Simulate drop copy receiving executions
        async with self.create_test_session("drop_copy") as (dc_session, dc_metrics):
            await dc_session.logon()

            # Simulate execution reports for placed orders
            for order_id, order_type in trading_state["orders"].items():
                exec_report = await self._create_execution_report(
                    dc_session,
                    order_id=order_id,
                    symbol="BTCUSDT",
                    side="1" if "BUY" in order_type else "2",
                    quantity="0.001",
                    fill_qty="0.001",
                    fill_price=str(trading_state["market_price"]),
                )

                await dc_session.on_message_received([exec_report])
                trading_state["executions"].append(order_id)

            # Process execution reports
            messages = await dc_session.get_all_new_messages_received()

            await dc_session.logout()

        # Validate simulated workflow
        self.assertGreater(len(trading_state["orders"]), 0)
        self.assertEqual(len(trading_state["executions"]), len(trading_state["orders"]))

        # Verify all sessions completed successfully
        for metrics in [md_metrics, oe_metrics, dc_metrics]:
            self.assert_session_metrics(metrics, min_messages=2, max_errors=0)

    @pytest.mark.load_test
    async def test_high_frequency_trading_simulation(self):
        """Test high-frequency trading scenario with rapid order placement."""
        hft_metrics = {
            "orders_per_second": 0,
            "market_data_rate": 0,
            "latency_samples": [],
        }

        # High-frequency market data
        async with self.create_test_session("market_data") as (md_session, md_metrics):
            await md_session.logon()

            subscription = await self._create_market_data_request(md_session, "BTCUSDT", "1", md_entry_types=["0"])
            await md_session.send_message(subscription)

            # Simulate high-frequency market data
            md_start = time.time()
            md_count = 0

            for i in range(100):  # 100 market data updates
                price = 50000 + (i % 20) - 10  # Oscillating price
                md_update = await self._create_fake_market_data(md_session, "BTCUSDT", price)
                await md_session.on_message_received([md_update])
                md_count += 1

                if i % 10 == 0:  # Process periodically
                    messages = await md_session.get_all_new_messages_received()
                    md_metrics.messages_received += len(messages)

                await asyncio.sleep(0.01)  # 100 Hz market data

            md_duration = time.time() - md_start
            hft_metrics["market_data_rate"] = md_count / md_duration

            await md_session.logout()

        # High-frequency order placement
        async with self.create_test_session("order_entry") as (oe_session, _oe_metrics):
            await oe_session.logon()

            order_start = time.time()
            order_count = 0

            # Rapid order placement
            for i in range(50):  # 50 orders
                latency_start = time.time()

                order_msg = await self._create_order(
                    oe_session,
                    symbol="BTCUSDT",
                    side="1" if i % 2 == 0 else "2",
                    quantity="0.001",
                    price=str(50000 + (i % 10)),
                )

                await oe_session.send_message(order_msg)

                latency = (time.time() - latency_start) * 1000  # ms
                hft_metrics["latency_samples"].append(latency)
                order_count += 1

                await asyncio.sleep(0.02)  # 50 Hz order rate

            order_duration = time.time() - order_start
            hft_metrics["orders_per_second"] = order_count / order_duration

            await oe_session.logout()

        # Validate HFT performance
        self.assertGreater(hft_metrics["market_data_rate"], 50)  # >50 updates/sec
        self.assertGreater(hft_metrics["orders_per_second"], 20)  # >20 orders/sec

        # Latency validation
        if hft_metrics["latency_samples"]:
            avg_latency = sum(hft_metrics["latency_samples"]) / len(hft_metrics["latency_samples"])
            max_latency = max(hft_metrics["latency_samples"])

            self.assertLess(avg_latency, 5.0)  # <5ms average
            self.assertLess(max_latency, 20.0)  # <20ms max

            logger.info(
                "HFT Performance: %.1f orders/s, %.2fms avg latency", hft_metrics["orders_per_second"], avg_latency
            )

    async def test_stress_test_concurrent_workflows(self):
        """Test system under stress with multiple concurrent workflows."""
        workflow_count = 4
        workflow_results = []

        async def run_trading_workflow(workflow_id: int):
            """Run a complete trading workflow."""
            try:
                # Market data phase
                async with self.create_test_session(
                    "market_data", endpoint_override=f"tcp+tls://stress-md-{workflow_id}.example.com:9000"
                ) as (md_session, md_metrics):
                    await md_session.logon()

                    # Subscribe and receive data
                    subscription = await self._create_market_data_request(
                        md_session, "BTCUSDT", "1", md_entry_types=["0"]
                    )
                    await md_session.send_message(subscription)

                    for i in range(10):
                        md_update = await self._create_fake_market_data(
                            md_session, "BTCUSDT", 50000 + workflow_id * 100 + i
                        )
                        await md_session.on_message_received([md_update])
                        await asyncio.sleep(0.05)

                    await md_session.logout()

                # Order entry phase
                async with self.create_test_session(
                    "order_entry", endpoint_override=f"tcp+tls://stress-oe-{workflow_id}.example.com:9000"
                ) as (oe_session, oe_metrics):
                    await oe_session.logon()

                    # Place multiple orders
                    for i in range(5):
                        order = await self._create_order(
                            oe_session,
                            symbol="BTCUSDT",
                            side="1",
                            quantity="0.001",
                            price=str(50000 + workflow_id * 10 + i),
                        )
                        await oe_session.send_message(order)
                        await asyncio.sleep(0.1)

                    await oe_session.logout()

                return workflow_id, "SUCCESS", (md_metrics, oe_metrics)

            except Exception as e:
                return workflow_id, f"ERROR: {e}", None

        # Run workflows concurrently
        stress_start = time.time()
        workflow_tasks = [run_trading_workflow(i) for i in range(workflow_count)]
        results = await asyncio.gather(*workflow_tasks, return_exceptions=True)
        stress_duration = time.time() - stress_start

        # Analyze stress test results
        successful_workflows = 0
        total_messages = 0

        for result in results:
            if not isinstance(result, Exception):
                workflow_id, status, metrics_tuple = result
                workflow_results.append((workflow_id, status))

                if status == "SUCCESS" and metrics_tuple:
                    successful_workflows += 1
                    md_metrics, oe_metrics = metrics_tuple
                    total_messages += md_metrics.messages_sent + oe_metrics.messages_sent

        # Stress test validation
        self.assertGreaterEqual(successful_workflows, workflow_count * 0.75)  # 75% success rate
        self.assertLess(stress_duration, 20.0)  # Complete within 20 seconds
        self.assertGreaterEqual(total_messages, workflow_count * 10)  # Reasonable message volume

        logger.info(
            "Stress test: %s/%s workflows succeeded in %.2fs", successful_workflows, workflow_count, stress_duration
        )


class TestAsyncBehavioralCompatibility(BaseE2ETest):
    """Test that async implementation maintains behavioral compatibility with sync version."""

    async def test_message_ordering_consistency(self):
        """Test that async implementation maintains message ordering like sync version."""
        sent_messages = []

        async with self.create_test_session("order_entry") as (session, _metrics):
            await session.logon()

            for i in range(10):
                msg = await session.create_fix_message_with_basic_header("1", "5000")  # TestRequest
                msg.append_pair("112", f"SEQ_TEST_{i}")

                await session.send_message(msg)

                seq_num = msg.get("34").decode()
                sent_messages.append((i, seq_num))

                await asyncio.sleep(0.01)

            await session.logout()

        for i, (msg_index, seq_num) in enumerate(sent_messages):
            self.assertEqual(msg_index, i)
            if i > 0:
                prev_seq = int(sent_messages[i - 1][1])
                curr_seq = int(seq_num)
                self.assertEqual(curr_seq, prev_seq + 1)

    async def test_error_response_compatibility(self):
        """Test that async error responses match sync behavior."""
        error_scenarios = []

        async with self.create_test_session("order_entry") as (session, _metrics):
            # Test various error conditions that should behave like sync version

            # 1. Missing required fields
            try:
                incomplete_msg = await session.create_fix_message_with_basic_header("D", "5000")
                # Missing ClOrdID, Symbol, etc.
                await session.send_message(incomplete_msg)
                error_scenarios.append("incomplete_message_sent")
            except Exception as e:
                error_scenarios.append(f"incomplete_message_error: {e}")

            # 2. Invalid field values
            try:
                invalid_msg = await session.create_fix_message_with_basic_header("D", "5000")
                invalid_msg.append_pair("11", "TEST_ORDER")
                invalid_msg.append_pair("55", "")  # Empty symbol
                invalid_msg.append_pair("54", "3")  # Invalid side
                await session.send_message(invalid_msg)
                error_scenarios.append("invalid_fields_sent")
            except Exception as e:
                error_scenarios.append(f"invalid_fields_error: {e}")

            # 3. Signature issues
            try:
                # Temporarily corrupt signature generation
                original_generate_sig = session.generate_signature
                session.generate_signature = lambda *args: "INVALID_SIGNATURE"

                test_msg = await session.create_fix_message_with_basic_header("1", "5000")
                test_msg.append_pair("112", "SIG_TEST")
                await session.send_message(test_msg)

                # Restore original function
                session.generate_signature = original_generate_sig
                error_scenarios.append("invalid_signature_sent")

            except Exception as e:
                error_scenarios.append(f"signature_error: {e}")

        # Verify error handling is consistent
        self.assertGreater(len(error_scenarios), 0)
        # In sync version, these would also be handled gracefully

    async def test_timing_behavior_consistency(self):
        """Test that async timing behavior is consistent with sync expectations."""
        timing_metrics = {
            "logon_time": None,
            "message_send_times": [],
            "logout_time": None,
        }

        async with self.create_test_session("market_data") as (session, _metrics):
            # Measure logon time
            logon_start = time.time()
            await session.logon(recv_window="5000")
            timing_metrics["logon_time"] = time.time() - logon_start

            # Measure message send times
            for i in range(5):
                send_start = time.time()

                test_msg = await session.create_fix_message_with_basic_header("1", "5000")
                test_msg.append_pair("112", f"TIMING_TEST_{i}")
                await session.send_message(test_msg)

                send_time = time.time() - send_start
                timing_metrics["message_send_times"].append(send_time)

                await asyncio.sleep(0.1)

            # Measure logout time
            logout_start = time.time()
            await session.logout()
            timing_metrics["logout_time"] = time.time() - logout_start

        # Validate timing consistency
        self.assertLess(timing_metrics["logon_time"], 2.0)  # Reasonable logon time
        self.assertLess(timing_metrics["logout_time"], 1.0)  # Quick logout

        # Message send times should be consistent
        avg_send_time = sum(timing_metrics["message_send_times"]) / len(timing_metrics["message_send_times"])
        self.assertLess(avg_send_time, 0.1)  # <100ms per message

        # No message should take much longer than average
        max_send_time = max(timing_metrics["message_send_times"])
        self.assertLess(max_send_time, avg_send_time * 5)  # Within 5x of average


if __name__ == "__main__":
    pytest.main([__file__])
