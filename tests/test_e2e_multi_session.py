#!/usr/bin/env python3
"""
End-to-End Multi-Session Tests

This module tests concurrent multi-session scenarios with market data,
order entry, and drop copy sessions running simultaneously, validating
cross-session coordination and system behavior under load.
"""

import asyncio
import logging
import time

import pytest

from tests.test_e2e_framework import (
    BaseE2ETest,
    create_concurrent_sessions,
)

logger = logging.getLogger(__name__)


class TestMultiSessionConcurrency(BaseE2ETest):
    """Test concurrent multi-session operations."""

    async def test_concurrent_session_creation(self):
        """Test creating multiple sessions concurrently."""
        session_configs = [
            {"type": session_type, "use_real_testnet": False}
            for session_type in ["market_data", "order_entry", "drop_copy"]
        ]

        start_time = time.time()
        sessions = await create_concurrent_sessions(
            self.credentials,
            session_configs,
            max_concurrent=3,
        )
        creation_time = time.time() - start_time

        # Verify all sessions created successfully
        self.assertEqual(len(sessions), 3)

        # Verify creation time is reasonable
        self.assertLess(creation_time, 5.0)  # Should complete within 5 seconds

        # Clean up sessions
        cleanup_tasks = []
        for session, _metrics in sessions:
            cleanup_tasks.append(self._cleanup_session(session))

        await asyncio.gather(*cleanup_tasks, return_exceptions=True)

        logger.info("Created %s sessions in %.2fs", len(sessions), creation_time)

    async def test_concurrent_logon_logout(self):
        """Test concurrent logon and logout across multiple sessions."""
        session_types = ["market_data", "order_entry", "drop_copy"]
        session_tasks = [self._create_and_setup_session(session_type) for session_type in session_types]

        sessions_results = await asyncio.gather(*session_tasks, return_exceptions=True)

        # Filter successful sessions
        sessions_metrics = [result for result in sessions_results if not isinstance(result, Exception)]

        self.assertGreaterEqual(len(sessions_metrics), 2)  # At least 2 sessions should succeed

        # Concurrent logon
        logon_tasks = [session.logon(recv_window="5000") for session, _metrics in sessions_metrics]

        logon_start = time.time()
        await asyncio.gather(*logon_tasks, return_exceptions=True)
        logon_time = time.time() - logon_start

        # Concurrent logout
        logout_tasks = [session.logout() for session, _metrics in sessions_metrics]

        logout_start = time.time()
        await asyncio.gather(*logout_tasks, return_exceptions=True)
        logout_time = time.time() - logout_start

        # Cleanup
        cleanup_tasks = [self._cleanup_session(session) for session, _metrics in sessions_metrics]
        await asyncio.gather(*cleanup_tasks, return_exceptions=True)

        # Verify timing
        self.assertLess(logon_time, 10.0)
        self.assertLess(logout_time, 5.0)

        logger.info("Concurrent logon: %.2fs, logout: %.2fs", logon_time, logout_time)

    async def test_market_data_and_trading_coordination(self):
        """Test coordination between market data and trading sessions."""
        market_data_received = []
        orders_placed = []

        # Create market data and order entry sessions
        md_session_task = self._create_and_setup_session("market_data")
        oe_session_task = self._create_and_setup_session("order_entry")

        md_session, md_metrics = await md_session_task
        oe_session, oe_metrics = await oe_session_task

        try:
            # Start both sessions
            await asyncio.gather(md_session.logon(recv_window="5000"), oe_session.logon(recv_window="5000"))

            # Subscribe to market data
            md_request = await self._create_market_data_request(md_session, "BTCUSDT", md_req_type="1")
            await md_session.send_message(md_request)

            # Simulate receiving market data and making trading decisions
            for i in range(5):
                # Simulate market data
                fake_md = await self._create_fake_market_data(md_session, "BTCUSDT", f"{50000 + i * 10}")
                await md_session.on_message_received([fake_md])
                market_data_received.append(fake_md)

                # Process market data
                md_messages = await md_session.get_all_new_messages_received()

                # Place order based on "market data"
                if md_messages:
                    order = await self._create_order(oe_session, f"MD_TRIGGERED_{i}", price="25000.00")
                    await oe_session.send_message(order)
                    orders_placed.append(order)

                await asyncio.sleep(0.2)

            # Logout both sessions
            await asyncio.gather(md_session.logout(), oe_session.logout())

        finally:
            # Cleanup
            await asyncio.gather(
                self._cleanup_session(md_session), self._cleanup_session(oe_session), return_exceptions=True
            )

        # Verify coordination
        self.assertEqual(len(market_data_received), 5)
        self.assertGreaterEqual(len(orders_placed), 3)  # Should place most orders

        # Verify no errors in either session
        self.assertEqual(len(md_metrics.errors), 0)
        self.assertEqual(len(oe_metrics.errors), 0)

    @pytest.mark.load_test
    async def test_high_load_multi_session(self):
        """Test system behavior under high load with multiple concurrent sessions."""
        session_configs = [
            {"type": session_type, "use_real_testnet": False, "instance": i}
            for session_type in ["market_data", "order_entry", "drop_copy"]
            for i in range(2)
        ]

        # Create all sessions concurrently
        start_time = time.time()
        sessions = await create_concurrent_sessions(
            self.credentials,
            session_configs,
            max_concurrent=6,
        )

        self.assertGreaterEqual(len(sessions), 4)  # At least 4 should succeed

        try:
            # Concurrent operations on all sessions
            operation_tasks = [self._run_session_workload(session, metrics) for session, metrics in sessions]

            # Run workloads concurrently
            workload_start = time.time()
            results = await asyncio.gather(*operation_tasks, return_exceptions=True)
            workload_time = time.time() - workload_start

            # Analyze results
            successful_workloads = [r for r in results if not isinstance(r, Exception)]

            self.assertGreaterEqual(len(successful_workloads), len(sessions) * 0.8)  # 80% success rate

            total_time = time.time() - start_time
            self.assertLess(total_time, 30.0)  # Complete within 30 seconds

            logger.info("High load test: %s sessions, %.2fs workload time", len(sessions), workload_time)

        finally:
            # Cleanup all sessions
            cleanup_tasks = [self._cleanup_session(session) for session, metrics in sessions]
            await asyncio.gather(*cleanup_tasks, return_exceptions=True)

    async def test_session_isolation(self):
        """Test that sessions are properly isolated and don't interfere."""
        session_data = {}

        # Create three different session types
        session_types = ["market_data", "order_entry", "drop_copy"]
        sessions = []

        for session_type in session_types:
            session, metrics = await self._create_and_setup_session(session_type)
            sessions.append((session, metrics, session_type))
            session_data[session_type] = {
                "messages_sent": 0,
                "messages_received": 0,
                "errors": [],
            }

        try:
            # Start all sessions
            logon_tasks = []
            for session, metrics, session_type in sessions:
                logon_tasks.append(session.logon(recv_window="5000"))

            await asyncio.gather(*logon_tasks)

            # Perform different operations on each session type
            operation_tasks = []

            for session, metrics, session_type in sessions:
                operation_tasks.append(
                    self._perform_session_operations(session_type, session, session_data[session_type])
                )

            # Run operations concurrently
            await asyncio.gather(*operation_tasks, return_exceptions=True)

            # Logout all sessions
            logout_tasks = []
            for session, metrics, session_type in sessions:
                logout_tasks.append(session.logout())

            await asyncio.gather(*logout_tasks, return_exceptions=True)

        finally:
            # Cleanup
            cleanup_tasks = []
            for session, metrics, session_type in sessions:
                cleanup_tasks.append(self._cleanup_session(session))
            await asyncio.gather(*cleanup_tasks, return_exceptions=True)

        # Verify isolation - each session should have performed its specific operations
        self.assertGreater(session_data["market_data"]["messages_sent"], 0)
        self.assertGreater(session_data["order_entry"]["messages_sent"], 0)
        self.assertGreater(session_data["drop_copy"]["messages_sent"], 0)

        # Verify no cross-contamination of errors
        total_errors = sum(len(data["errors"]) for data in session_data.values())
        self.assertEqual(total_errors, 0)  # No errors expected in isolation test

    @pytest.mark.error_scenario
    async def test_session_failure_isolation(self):
        """Test that failure in one session doesn't affect others."""
        sessions = []

        # Create multiple sessions
        for session_type in ["market_data", "order_entry", "drop_copy"]:
            session, metrics = await self._create_and_setup_session(session_type)
            sessions.append((session, metrics, session_type))

        try:
            # Start all sessions
            logon_tasks = []
            for session, metrics, session_type in sessions:
                logon_tasks.append(session.logon(recv_window="5000"))

            await asyncio.gather(*logon_tasks, return_exceptions=True)

            # Simulate failure in one session (market_data)
            failed_session = sessions[0][0]  # market_data session

            # Cause deliberate failure
            try:
                # Send malformed message to cause error
                bad_msg = await failed_session.create_fix_message_with_basic_header("INVALID", "5000")
                await failed_session.send_message(bad_msg)
            except Exception:
                pass  # Expected to fail

            # Continue operations on other sessions
            operation_tasks = []
            for session, metrics, session_type in sessions[1:]:  # Skip failed session

                async def run_op(st=session_type, s=session):
                    try:
                        data = {"messages_sent": 0, "messages_received": 0, "errors": []}
                        await self._perform_session_operations(st, s, data)
                        return f"{st}_success"
                    except Exception as e:
                        return f"{st}_error: {e}"

                operation_tasks.append(run_op())

            # Other sessions should continue working
            results = await asyncio.gather(*operation_tasks, return_exceptions=True)

            # At least one other session should complete successfully
            successful_operations = [r for r in results if isinstance(r, str) and "success" in r]
            self.assertGreater(len(successful_operations), 0)

        finally:
            # Cleanup all sessions
            cleanup_tasks = []
            for session, metrics, session_type in sessions:
                cleanup_tasks.append(self._cleanup_session(session))
            await asyncio.gather(*cleanup_tasks, return_exceptions=True)

    async def _run_session_workload(self, session, metrics):
        """Run a standard workload on a session."""
        try:
            await session.logon(recv_window="5000")
            metrics.messages_sent += 1

            # Perform 10 operations
            for i in range(10):
                if hasattr(session, "drop_copy_flag") and session.drop_copy_flag:
                    # Drop copy operations
                    exec_msg = await self._create_execution_report(session, f"LOAD_{i}")
                    await session.on_message_received([exec_msg])
                else:
                    # Regular message
                    test_msg = await session.create_fix_message_with_basic_header("1", "5000")  # TestRequest
                    test_msg.append_pair("112", f"TEST_{i}")
                    await session.send_message(test_msg)
                    metrics.messages_sent += 1

                await asyncio.sleep(0.05)

            await session.logout()
            metrics.messages_sent += 1

            return True

        except Exception as e:
            metrics.errors.append(f"Workload error: {e}")
            return False

    async def _perform_session_operations(self, session_type, session, data):
        """Perform session-type-specific operations."""
        try:
            if session_type == "market_data":
                subscription = await self._create_market_data_request(session, "BTCUSDT", md_req_type="1")
                await session.send_message(subscription)
                data["messages_sent"] += 1
                for i in range(3):
                    msg = await self._create_fake_market_data(session, "BTCUSDT", f"{50000 + i}")
                    await session.on_message_received([msg])
                    await asyncio.sleep(0.1)
            elif session_type == "order_entry":
                for i in range(3):
                    order = await self._create_order(session, f"ISOLATION_{i}", price="25000.00")
                    await session.send_message(order)
                    data["messages_sent"] += 1
                    await asyncio.sleep(0.1)
                return
            else:
                status_req = await session.create_fix_message_with_basic_header("1", "5000")
                status_req.append_pair("112", "DROP_COPY_STATUS")
                await session.send_message(status_req)
                data["messages_sent"] += 1
                for i in range(3):
                    msg = await self._create_execution_report(session, f"ISOLATION_{i}")
                    await session.on_message_received([msg])
                    await asyncio.sleep(0.1)

            messages = await session.get_all_new_messages_received()
            data["messages_received"] += len(messages)

        except Exception as e:
            data["errors"].append(f"{session_type} error: {e}")


if __name__ == "__main__":
    pytest.main([__file__])
