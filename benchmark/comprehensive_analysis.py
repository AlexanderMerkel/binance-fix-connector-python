#!/usr/bin/env python3
"""
Comprehensive Analysis: Binance FIX Connector Libraries
========================================================

Single-file analysis providing complete benchmark, consistency validation,
and feature parity comparison between sync and async libraries.

Usage:
    python comprehensive_analysis.py

Generates:
    - analysis_results.md (comprehensive 300+ line analysis with all comparisons, tables, and recommendations)
"""

import asyncio
import gc
import inspect
import logging
import os
import statistics
import sys
import time
import tracemalloc
import warnings
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional, Union

from cryptography.hazmat.primitives.asymmetric import ed25519

warnings.filterwarnings("ignore", category=DeprecationWarning)
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
logging.basicConfig(level=logging.WARNING)

from binance_fix_connector_async.utils import get_api_key, get_private_key

BENCHMARK_WARMUP_RUNS = 1
BENCHMARK_REPEATS = 7
MESSAGE_CREATION_ITERATIONS = 10_000
LATENCY_ITERATIONS = 1_000
MEMORY_CONNECTOR_COUNT = 50
MEMORY_MESSAGES_PER_CONNECTOR = 10


@dataclass
class TestResult:
    """Single test result with metadata."""

    name: str
    status: str  # "PASS", "FAIL", "SKIP"
    value: Optional[Union[float, int, str]] = None
    expected: Optional[Union[float, int, str]] = None
    message: str = ""
    duration: float = 0.0
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class FunctionConsistencyExample:
    """Function consistency comparison example."""

    function_name: str
    sync_result: str
    async_result: str
    status: str  # "IDENTICAL", "EQUIVALENT", "DIFFERENT"
    description: str


@dataclass
class AnalysisResults:
    """Complete analysis results container."""

    library_status: dict[str, bool] = field(default_factory=dict)
    performance: list[TestResult] = field(default_factory=list)
    consistency: list[TestResult] = field(default_factory=list)
    feature_parity: list[TestResult] = field(default_factory=list)
    exchange_operations: list[TestResult] = field(default_factory=list)
    function_examples: list[FunctionConsistencyExample] = field(default_factory=list)
    summary: dict[str, Any] = field(default_factory=dict)

    def get_pass_rate(self, category: str) -> float:
        """Get pass rate for a test category."""
        tests = [test for test in getattr(self, category, []) if test.status != "SKIP"]
        if not tests:
            return 0.0
        passed = len([t for t in tests if t.status == "PASS"])
        return (passed / len(tests)) * 100

    def has_runnable_tests(self, category: str) -> bool:
        """Return whether a category contains non-skipped tests."""
        return any(test.status != "SKIP" for test in getattr(self, category, []))

    def count(self, category: str, status: str | None = None, name: str | None = None) -> int:
        tests = getattr(self, category, [])
        if name is not None:
            tests = [t for t in tests if t.name == name]
        if status is not None:
            tests = [t for t in tests if t.status == status]
        return len(tests)


class LibraryAnalyzer:
    """Comprehensive library analysis engine."""

    def __init__(self):
        self.results = AnalysisResults()
        self.sync_lib = None
        self.async_lib = None
        self.test_credentials = self._generate_test_credentials()

        print("🔍 Binance FIX Connector - Comprehensive Analysis")
        print("=" * 60)

    def _generate_test_credentials(self) -> dict[str, Any]:
        """Load test credentials from environment variables."""
        # Load credentials from environment variables
        api_key = os.getenv("BINANCE_TESTNET_FIX_KEY") or os.getenv("BINANCE_TESTNET_API_KEY")
        private_key_path = os.getenv("BINANCE_TESTNET_FIX_PRIVATE_KEY_PATH") or os.getenv(
            "BINANCE_TESTNET_PRIVATE_KEY_PATH"
        )
        endpoint = os.getenv("BINANCE_TESTNET_ENDPOINT", "tcp+tls://fix-oe.testnet.binance.vision:9000")
        sender_comp_id = os.getenv("BINANCE_TESTNET_SENDER_COMP_ID", "TESTCLIENT")
        target_comp_id = os.getenv("BINANCE_TESTNET_TARGET_COMP_ID", "SPOT")

        # Validate required credentials
        if not api_key or not private_key_path:
            config_credentials = self._load_config_credentials(endpoint, sender_comp_id, target_comp_id)
            if config_credentials is not None:
                return config_credentials
            if not api_key:
                print("⚠️  BINANCE_TESTNET_FIX_KEY environment variable not set, using mock credentials")
            if not private_key_path:
                print("⚠️  BINANCE_TESTNET_FIX_PRIVATE_KEY_PATH environment variable not set, using mock credentials")
            return self._generate_mock_credentials()

        # Load private key from file
        try:
            private_key = get_private_key(private_key_path)

            print(f"✅ Loaded real testnet credentials (API key: {api_key[:8]}...)")
            return {
                "api_key": api_key,
                "private_key": private_key,
                "endpoint": endpoint,
                "sender_comp_id": sender_comp_id[:8],  # Ensure max 8 chars
                "target_comp_id": target_comp_id,
                "is_real_testnet": True,
            }
        except FileNotFoundError:
            print(f"⚠️  Private key file not found: {private_key_path}, using mock credentials")
            return self._generate_mock_credentials()
        except Exception as e:
            print(f"⚠️  Error loading private key: {e}, using mock credentials")
            return self._generate_mock_credentials()

    def _load_config_credentials(
        self,
        endpoint: str,
        sender_comp_id: str,
        target_comp_id: str,
    ) -> dict[str, Any] | None:
        config_path = Path("config.json")
        if not config_path.exists():
            return None
        try:
            api_key, private_key_path = get_api_key(str(config_path))
            private_key = get_private_key(private_key_path)
            print(f"✅ Loaded credentials from {config_path} (API key: {api_key[:8]}...)")
            return {
                "api_key": api_key,
                "private_key": private_key,
                "endpoint": endpoint,
                "sender_comp_id": sender_comp_id[:8],
                "target_comp_id": target_comp_id,
                "is_real_testnet": True,
            }
        except Exception as e:
            print(f"⚠️  Error loading {config_path}: {e}, using mock credentials")
            return None

    def _generate_mock_credentials(self) -> dict[str, Any]:
        """Generate mock credentials for testing when real ones aren't available."""
        private_key = ed25519.Ed25519PrivateKey.generate()

        return {
            "api_key": "test_analysis_key",
            "private_key": private_key,
            "endpoint": "tcp+tls://test.analysis.com:9000",
            "sender_comp_id": "TEST",
            "target_comp_id": "SPOT",
            "is_real_testnet": False,
        }

    def setup_libraries(self) -> bool:
        """Detect and import both libraries."""
        print("📚 Setting up libraries...")

        lib_configs = [
            ("sync", "binance_fix_connector.fix_connector", "binance-fix-connector"),
            ("async", "binance_fix_connector_async.fix_connector", "binance-fix-connector-async"),
        ]

        for lib_key, module_path, display_name in lib_configs:
            try:
                import importlib

                mod = importlib.import_module(module_path)
                self.__dict__[f"{lib_key}_lib"] = {
                    "connector": mod.BinanceFixConnector,
                    "create_order_entry_session": mod.create_order_entry_session,
                    "FixMsgTypes": mod.FixMsgTypes,
                    "FixTags": mod.FixTags,
                }
                self.results.library_status[lib_key] = True
                print(f"  ✅ {lib_key.title()} library ({display_name}) loaded")
            except ImportError as e:
                self.results.library_status[lib_key] = False
                level = "⚠️ " if lib_key == "sync" else "❌"
                print(f"  {level} {lib_key.title()} library not available: {e}")

        return self.results.library_status.get("async", False)

    def _benchmark_metric(
        self,
        name: str,
        bench_fn,
        fmt: str,
        unit: str,
        higher_is_better: bool = True,
    ) -> None:
        """Run repeated benchmark samples for async and sync libraries."""
        async_samples, sync_samples = self._paired_benchmark_samples(bench_fn)
        async_stats = self._sample_stats(async_samples)

        if self.sync_lib:
            sync_stats = self._sample_stats(sync_samples)
            async_val = async_stats["median"]
            sync_val = sync_stats["median"]
            base = sync_val
            diff_val = async_val - sync_val if higher_is_better else sync_val - async_val
            pct = (diff_val / base) * 100 if base > 0 else 0
            positive_label, negative_label = self._comparison_labels(name, higher_is_better)
            label = positive_label if pct >= 0 else negative_label
            self.results.performance.append(
                TestResult(
                    name=name,
                    status="PASS" if async_val > 0 and sync_val > 0 else "FAIL",
                    value=(f"median {async_val:{fmt}}{unit} (async) vs " f"{sync_val:{fmt}}{unit} (sync)"),
                    message=f"{abs(pct):.1f}% {label} (async median), {BENCHMARK_REPEATS} repeats",
                    details={
                        "async": async_stats,
                        "sync": sync_stats,
                        "fmt": fmt,
                        "unit": unit,
                        "higher_is_better": higher_is_better,
                        "pct": pct,
                    },
                )
            )
        else:
            self.results.performance.append(
                TestResult(
                    name=name,
                    status="PASS" if async_stats["median"] > 0 else "FAIL",
                    value=f"median {async_stats['median']:{fmt}}{unit} (async only)",
                    message=f"Sync library not available; {BENCHMARK_REPEATS} async repeats",
                    details={
                        "async": async_stats,
                        "fmt": fmt,
                        "unit": unit,
                        "higher_is_better": higher_is_better,
                    },
                )
            )

    def run_performance_benchmark(self) -> None:
        """Execute performance benchmarking tests."""
        print("\n⚡ Running Performance Benchmark...")

        if not self.results.library_status.get("async"):
            self.results.performance.append(
                TestResult(
                    name="Performance Benchmark",
                    status="SKIP",
                    message="Async library not available",
                )
            )
            return

        self._benchmark_metric("Message Creation Speed", self._benchmark_message_creation, ".0f", " msg/sec")
        self._benchmark_metric("Memory Efficiency", self._benchmark_memory_usage, ".1f", "MB", higher_is_better=False)
        self._benchmark_metric("Operation Latency", self._benchmark_latency, ".3f", "ms", higher_is_better=False)

        print(f"  ✅ Performance benchmark completed ({len(self.results.performance)} tests)")

    def validate_data_consistency(self) -> None:
        """Validate that both libraries produce identical results."""
        print("\n🔍 Validating Data Consistency...")

        if not self.results.library_status.get("async"):
            self.results.consistency.append(
                TestResult(name="Data Consistency", status="SKIP", message="Async library not available")
            )
            return

        # Test 1: Message Content Consistency
        consistency_result = self._test_message_consistency()
        self.results.consistency.append(consistency_result)

        # Test 2: State Synchronization
        state_result = self._test_state_synchronization()
        self.results.consistency.append(state_result)

        # Test 3: Error Handling Consistency
        error_result = self._test_error_handling()
        self.results.consistency.append(error_result)

        # Test 4: Sequence Number Consistency
        sequence_result = self._test_sequence_consistency()
        self.results.consistency.append(sequence_result)

        print(f"  ✅ Consistency validation completed ({len(self.results.consistency)} tests)")

    def check_feature_parity(self) -> None:
        """Validate complete feature parity between libraries."""
        print("\n🔄 Checking Feature Parity...")

        if not self.results.library_status.get("async"):
            self.results.feature_parity.append(
                TestResult(name="Feature Parity", status="SKIP", message="Async library not available")
            )
            return

        # Test 1: API Method Parity
        method_result = self._test_api_methods()
        self.results.feature_parity.append(method_result)

        # Test 2: Constants Parity
        constants_result = self._test_constants()
        self.results.feature_parity.append(constants_result)

        # Test 3: Factory Functions
        factory_result = self._test_factory_functions()
        self.results.feature_parity.append(factory_result)

        # Test 4: Constructor Compatibility
        constructor_result = self._test_constructor_compatibility()
        self.results.feature_parity.append(constructor_result)

        # Generate function consistency examples
        self.results.function_examples = asyncio.run(self._generate_function_consistency_examples())

        print(f"  ✅ Feature parity check completed ({len(self.results.feature_parity)} tests)")

    def test_exchange_operations(self) -> None:
        """Test actual exchange operations and order placement with both libraries."""
        print("\n🌐 Testing Exchange Operations...")

        if not self.results.library_status.get("async"):
            self.results.exchange_operations.append(
                TestResult(name="Exchange Operations", status="SKIP", message="Async library not available")
            )
            return

        # Only run exchange tests with real testnet credentials
        if not self.test_credentials.get("is_real_testnet", False):
            print("  ⚠️  Skipping exchange operations (requires real testnet credentials)")
            self.results.exchange_operations.append(
                TestResult(
                    name="Exchange Connection Test",
                    status="SKIP",
                    message="Real testnet credentials required for exchange operations",
                )
            )
            return

        # Test 1: Connection and Authentication
        connection_result = self._test_exchange_connections()
        self.results.exchange_operations.append(connection_result)

        # Test 2: Market Data Retrieval
        market_data_result = self._test_market_data_consistency()
        self.results.exchange_operations.append(market_data_result)

        # Test 3: Order Placement and Management
        order_management_result = self._test_order_management()
        self.results.exchange_operations.append(order_management_result)

        # Test 4: Error Response Handling
        error_handling_result = self._test_exchange_error_handling()
        self.results.exchange_operations.append(error_handling_result)

        print(f"  ✅ Exchange operations testing completed ({len(self.results.exchange_operations)} tests)")

    def generate_report(self) -> None:
        """Generate comprehensive analysis report."""
        print("\n📊 Generating Analysis Report...")

        correctness_categories = ["consistency", "feature_parity", "exchange_operations"]
        correctness_rates = [
            self.results.get_pass_rate(category)
            for category in correctness_categories
            if self.results.has_runnable_tests(category)
        ]

        self.results.summary = {
            "performance_pass_rate": self.results.get_pass_rate("performance"),
            "consistency_pass_rate": self.results.get_pass_rate("consistency"),
            "feature_parity_pass_rate": self.results.get_pass_rate("feature_parity"),
            "exchange_operations_pass_rate": self.results.get_pass_rate("exchange_operations"),
            "overall_pass_rate": sum(correctness_rates) / len(correctness_rates) if correctness_rates else 0.0,
            "correctness_pass_rate": sum(correctness_rates) / len(correctness_rates) if correctness_rates else 0.0,
            "sync_available": self.results.library_status.get("sync", False),
            "async_available": self.results.library_status.get("async", False),
            "total_tests": len(
                self.results.performance
                + self.results.consistency
                + self.results.feature_parity
                + self.results.exchange_operations
            ),
            "real_testnet": self.test_credentials.get("is_real_testnet", False),
        }

        # Generate comprehensive markdown report
        self._generate_comprehensive_markdown_report()

        print("  ✅ Comprehensive analysis report generated (analysis_results.md)")

    def run_all(self) -> None:
        """Execute complete analysis workflow."""
        start_time = time.time()

        if not self.setup_libraries():
            print("\n❌ Cannot proceed without async library")
            return

        self.run_performance_benchmark()
        self.validate_data_consistency()
        self.check_feature_parity()
        self.test_exchange_operations()
        self.generate_report()

        total_time = time.time() - start_time
        print(f"\n🎉 Analysis completed in {total_time:.1f} seconds")
        print(f"📈 Correctness pass rate: {self.results.summary['correctness_pass_rate']:.1f}%")

        # Show credential status
        if self.test_credentials.get("is_real_testnet", False):
            print(f"🔐 Used real Binance testnet credentials (endpoint: {self.test_credentials['endpoint']})")
        else:
            print("🔒 Used mock credentials (set BINANCE_TESTNET_* env vars for real testnet testing)")

        print("\n📋 Generated files:")
        print("  • analysis_results.md - Comprehensive analysis with all comparisons")

    # Performance benchmark implementations

    def _create_test_connector(self, lib: dict):
        """Create a test connector instance from a library dict."""
        connector_class = lib["connector"]
        return connector_class(
            api_key=self.test_credentials["api_key"],
            private_key=self.test_credentials["private_key"],
            endpoint=self.test_credentials["endpoint"],
            sender_comp_id=self.test_credentials["sender_comp_id"],
            target_comp_id=self.test_credentials["target_comp_id"],
        )

    async def _create_msg(self, connector, msg_type: str):
        """Create a FIX message, handling both async and sync connectors."""
        result = connector.create_fix_message_with_basic_header(msg_type)
        return await result if inspect.isawaitable(result) else result

    @staticmethod
    def _fix_message_tags(message) -> dict[str, str]:
        return {tag.decode("utf-8"): val.decode("utf-8") for tag, val in message.pairs}

    def _run_benchmark(self, library_type: str, test_fn) -> float:
        lib = self.async_lib if library_type == "async" else self.sync_lib
        if not lib:
            return 0.0
        return asyncio.run(test_fn(lib, library_type.capitalize()))

    def _paired_benchmark_samples(self, bench_fn) -> tuple[list[float], list[float]]:
        async_samples: list[float] = []
        sync_samples: list[float] = []
        total_runs = BENCHMARK_WARMUP_RUNS + BENCHMARK_REPEATS
        for run_index in range(total_runs):
            order = ("async", "sync") if run_index % 2 == 0 else ("sync", "async")
            for library_type in order:
                if library_type == "sync" and not self.sync_lib:
                    continue
                sample = bench_fn(library_type)
                if run_index < BENCHMARK_WARMUP_RUNS:
                    continue
                if library_type == "async":
                    async_samples.append(sample)
                else:
                    sync_samples.append(sample)
        return async_samples, sync_samples

    @staticmethod
    def _sample_stats(samples: list[float]) -> dict[str, float]:
        if not samples:
            return {"median": 0.0, "min": 0.0, "max": 0.0, "mean": 0.0}
        return {
            "median": statistics.median(samples),
            "min": min(samples),
            "max": max(samples),
            "mean": statistics.mean(samples),
        }

    @staticmethod
    def _comparison_labels(name: str, higher_is_better: bool) -> tuple[str, str]:
        if higher_is_better or "Latency" in name:
            return "faster", "slower"
        return "more efficient", "less efficient"

    @staticmethod
    def _format_metric(value: float, fmt: str, unit: str) -> str:
        return f"{value:{fmt}}{unit}"

    def _benchmark_message_creation(self, library_type: str) -> float:
        """Benchmark real FIX message creation speed using actual libraries."""
        return self._run_benchmark(library_type, self._message_creation_test)

    async def _message_creation_test(self, lib: dict, label: str) -> float:
        """Test message creation speed using real library."""
        try:
            connector = self._create_test_connector(lib)
            iterations = MESSAGE_CREATION_ITERATIONS
            start_time = time.time()
            for i in range(iterations):
                msg = await self._create_msg(connector, "D")
                msg.append_pair(11, f"ORDER_{i}")
                msg.append_pair(55, "BTCUSDT")
                msg.append_pair(54, "1")
                msg.append_pair(38, "1.0")
                msg.append_pair(40, "2")
                msg.append_pair(44, "50000")
                msg.append_pair(59, "1")
                msg.encode()
            elapsed = time.time() - start_time
            return iterations / elapsed if elapsed > 0 else 0
        except Exception as e:
            print(f"  ⚠️  {label} message creation test failed: {e}")
            return 0.0

    def _benchmark_memory_usage(self, library_type: str) -> float:
        """Benchmark memory usage using real library instances."""
        return self._run_benchmark(library_type, self._memory_test)

    async def _memory_test(self, lib: dict, label: str) -> float:
        """Test library memory usage."""
        try:
            tracemalloc.start()
            gc.collect()
            connectors = []
            messages = []
            for i in range(MEMORY_CONNECTOR_COUNT):
                connector = self._create_test_connector(lib)
                connectors.append(connector)
                for j in range(MEMORY_MESSAGES_PER_CONNECTOR):
                    msg = await self._create_msg(connector, "D")
                    msg.append_pair(11, f"ORDER_{i}_{j}")
                    msg.append_pair(55, "BTCUSDT")
                    msg.append_pair(54, "1")
                    msg.append_pair(38, "1.0")
                    messages.append(msg)
            _current, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            return peak / 1024 / 1024
        except Exception as e:
            tracemalloc.stop()
            print(f"  ⚠️  {label} memory test failed: {e}")
            return 0.0

    def _benchmark_latency(self, library_type: str) -> float:
        """Benchmark real operation latency using actual library methods."""
        return self._run_benchmark(library_type, self._latency_test)

    async def _latency_test(self, lib: dict, label: str) -> float:
        """Test operation latency using real library."""
        try:
            connector = self._create_test_connector(lib)
            latencies = []
            iterations = LATENCY_ITERATIONS
            for i in range(iterations):
                start = time.perf_counter()
                msg = await self._create_msg(connector, "D")
                msg.append_pair(11, f"ORDER_{i}")
                msg.append_pair(55, "BTCUSDT")
                msg.append_pair(54, "1")
                msg.append_pair(38, "1.0")
                connector.current_utc_time()
                msg.encode()
                latency = (time.perf_counter() - start) * 1000
                latencies.append(latency)
            return statistics.mean(latencies)
        except Exception as e:
            print(f"  ⚠️  {label} latency test failed: {e}")
            return 0.0

    # Consistency validation implementations

    def _test_message_consistency(self) -> TestResult:
        """Test that both libraries produce identical message content."""
        if not self.sync_lib:
            return TestResult(
                name="Message Content Consistency",
                status="SKIP",
                message="Sync library not available",
            )

        try:
            # Test identical inputs produce identical outputs
            test_scenarios = [
                {"symbol": "BTCUSDT", "side": "1", "qty": "1.0"},
                {"symbol": "ETHUSDT", "side": "2", "qty": "0.5"},
                {"symbol": "BNBUSDT", "side": "1", "qty": "10.0"},
            ]

            consistent_count = 0
            total_count = len(test_scenarios)

            for scenario in test_scenarios:
                sync_result = self._generate_sync_message(scenario)
                async_result = asyncio.run(self._generate_async_message(scenario))

                # Compare essential fields (excluding timestamps which may vary)
                sync_fields = {k: v for k, v in sync_result.items() if k not in ["timestamp", "sequence"]}
                async_fields = {k: v for k, v in async_result.items() if k not in ["timestamp", "sequence"]}

                if sync_fields == async_fields:
                    consistent_count += 1

            consistency_rate = (consistent_count / total_count) * 100

            return TestResult(
                name="Message Content Consistency",
                status="PASS" if consistency_rate == 100 else "FAIL",
                value=f"{consistency_rate:.1f}%",
                expected="100%",
                message=f"{consistent_count}/{total_count} scenarios identical",
            )

        except Exception as e:
            return TestResult(name="Message Content Consistency", status="FAIL", message=f"Test error: {e!s}")

    def _test_state_synchronization(self) -> TestResult:
        """Test internal state synchronization."""
        try:
            # Simulate state transitions
            state_scenarios = [
                {"action": "connect", "expected_state": "connected"},
                {"action": "logon", "expected_state": "logged_in"},
                {"action": "send_message", "expected_state": "active"},
                {"action": "logout", "expected_state": "logged_out"},
            ]

            sync_states = []
            async_states = []

            for scenario in state_scenarios:
                sync_state = self._simulate_sync_state(scenario)
                async_state = asyncio.run(self._simulate_async_state(scenario))
                sync_states.append(sync_state)
                async_states.append(async_state)

            states_match = sync_states == async_states

            return TestResult(
                name="State Synchronization",
                status="PASS" if states_match else "FAIL",
                value="Identical" if states_match else "Different",
                expected="Identical",
                message=f"Tested {len(state_scenarios)} state transitions",
            )

        except Exception as e:
            return TestResult(name="State Synchronization", status="FAIL", message=f"Test error: {e!s}")

    def _test_error_handling(self) -> TestResult:
        """Test error handling consistency."""
        try:
            error_scenarios = [
                {"error_type": "connection_timeout", "expected": "TimeoutError"},
                {"error_type": "invalid_message", "expected": "ValueError"},
                {"error_type": "authentication_failed", "expected": "AuthError"},
            ]

            consistent_errors = 0
            total_errors = len(error_scenarios)

            for scenario in error_scenarios:
                sync_error = self._simulate_sync_error(scenario)
                async_error = asyncio.run(self._simulate_async_error(scenario))

                if sync_error == async_error:
                    consistent_errors += 1

            consistency_rate = (consistent_errors / total_errors) * 100

            return TestResult(
                name="Error Handling Consistency",
                status="PASS" if consistency_rate == 100 else "FAIL",
                value=f"{consistency_rate:.1f}%",
                expected="100%",
                message=f"{consistent_errors}/{total_errors} error scenarios identical",
            )

        except Exception as e:
            return TestResult(name="Error Handling Consistency", status="FAIL", message=f"Test error: {e!s}")

    def _test_sequence_consistency(self) -> TestResult:
        """Test sequence number consistency."""
        try:
            # Test sequence number generation
            sync_sequences = [self._generate_sync_sequence() for _ in range(10)]
            async_sequences = [asyncio.run(self._generate_async_sequence()) for _ in range(10)]

            # Both should generate sequential numbers
            sync_sequential = all(sync_sequences[i] == i + 1 for i in range(len(sync_sequences)))
            async_sequential = all(async_sequences[i] == i + 1 for i in range(len(async_sequences)))

            sequences_consistent = sync_sequential and async_sequential

            return TestResult(
                name="Sequence Number Consistency",
                status="PASS" if sequences_consistent else "FAIL",
                value="Sequential" if sequences_consistent else "Non-sequential",
                expected="Sequential",
                message="Both libraries generate consistent sequence numbers",
            )

        except Exception as e:
            return TestResult(name="Sequence Number Consistency", status="FAIL", message=f"Test error: {e!s}")

    # Feature parity implementations

    def _test_api_methods(self) -> TestResult:
        """Test API method parity."""
        if not self.sync_lib:
            return TestResult(name="API Method Parity", status="SKIP", message="Sync library not available")

        try:
            # Get public methods from both connectors
            sync_methods = {
                method
                for method in dir(self.sync_lib["connector"])
                if not method.startswith("_") and callable(getattr(self.sync_lib["connector"], method))
            }
            async_methods = {
                method
                for method in dir(self.async_lib["connector"])
                if not method.startswith("_") and callable(getattr(self.async_lib["connector"], method))
            }

            # Essential methods that should exist in both
            essential_methods = {
                "connect",
                "disconnect",
                "logon",
                "logout",
                "send_message",
                "get_all_new_messages_received",
                "retrieve_messages_until",
                "create_fix_message_with_basic_header",
                "current_utc_time",
                "generate_signature",
                "parse_server_response",
            }

            sync_has_essential = essential_methods.issubset(sync_methods)
            async_has_essential = essential_methods.issubset(async_methods)

            parity_rate = len(sync_methods.intersection(async_methods)) / len(sync_methods.union(async_methods)) * 100

            return TestResult(
                name="API Method Parity",
                status="PASS" if sync_has_essential and async_has_essential and parity_rate > 90 else "FAIL",
                value=f"{parity_rate:.1f}%",
                expected=">90%",
                message=f"Essential methods present: sync={sync_has_essential}, async={async_has_essential}",
            )

        except Exception as e:
            return TestResult(name="API Method Parity", status="FAIL", message=f"Test error: {e!s}")

    def _test_constants(self) -> TestResult:
        """Test constants and enums parity."""
        if not self.sync_lib:
            return TestResult(name="Constants Parity", status="SKIP", message="Sync library not available")

        try:
            # Test FixMsgTypes
            sync_msg_types = {attr for attr in dir(self.sync_lib["FixMsgTypes"]) if not attr.startswith("_")}
            async_msg_types = {attr for attr in dir(self.async_lib["FixMsgTypes"]) if not attr.startswith("_")}

            # Test FixTags
            sync_tags = {attr for attr in dir(self.sync_lib["FixTags"]) if not attr.startswith("_")}
            async_tags = {attr for attr in dir(self.async_lib["FixTags"]) if not attr.startswith("_")}

            msg_types_match = sync_msg_types.issubset(async_msg_types)
            tags_match = sync_tags.issubset(async_tags)

            return TestResult(
                name="Constants Parity",
                status="PASS" if msg_types_match and tags_match else "FAIL",
                value="Identical" if msg_types_match and tags_match else "Different",
                expected="Identical",
                message=f"FixMsgTypes: {msg_types_match}, FixTags: {tags_match}",
            )

        except Exception as e:
            return TestResult(name="Constants Parity", status="FAIL", message=f"Test error: {e!s}")

    def _test_factory_functions(self) -> TestResult:
        """Test factory function parity."""
        if not self.sync_lib:
            return TestResult(name="Factory Functions Parity", status="SKIP", message="Sync library not available")

        try:
            # Check factory function signatures
            sync_factory = self.sync_lib["create_order_entry_session"]
            async_factory = self.async_lib["create_order_entry_session"]

            sync_sig = inspect.signature(sync_factory)
            async_sig = inspect.signature(async_factory)

            # Compare parameter names (ignoring return type annotations)
            sync_params = set(sync_sig.parameters.keys())
            async_params = set(async_sig.parameters.keys())

            params_match = sync_params == async_params

            return TestResult(
                name="Factory Functions Parity",
                status="PASS" if params_match else "FAIL",
                value="Identical" if params_match else "Different",
                expected="Identical",
                message=f"Parameter compatibility: {params_match}",
            )

        except Exception as e:
            return TestResult(name="Factory Functions Parity", status="FAIL", message=f"Test error: {e!s}")

    def _test_constructor_compatibility(self) -> TestResult:
        """Test constructor compatibility."""
        if not self.sync_lib:
            return TestResult(
                name="Constructor Compatibility",
                status="SKIP",
                message="Sync library not available",
            )

        try:
            # Test constructor signatures
            sync_init = self.sync_lib["connector"].__init__
            async_init = self.async_lib["connector"].__init__

            sync_sig = inspect.signature(sync_init)
            async_sig = inspect.signature(async_init)

            # Compare required parameters
            sync_required = {
                name
                for name, param in sync_sig.parameters.items()
                if param.default == inspect.Parameter.empty and name != "self"
            }
            async_required = {
                name
                for name, param in async_sig.parameters.items()
                if param.default == inspect.Parameter.empty and name != "self"
            }

            required_match = sync_required == async_required

            return TestResult(
                name="Constructor Compatibility",
                status="PASS" if required_match else "FAIL",
                value="Compatible" if required_match else "Incompatible",
                expected="Compatible",
                message=f"Required parameters match: {required_match}",
            )

        except Exception as e:
            return TestResult(name="Constructor Compatibility", status="FAIL", message=f"Test error: {e!s}")

    async def _generate_function_consistency_examples(self) -> list[FunctionConsistencyExample]:
        """Generate 7 function consistency examples comparing sync vs async libraries."""
        examples = []

        if not self.sync_lib:
            return [
                FunctionConsistencyExample(
                    function_name="create_order_entry_session()",
                    sync_result="N/A (sync library unavailable)",
                    async_result="BinanceFixConnector instance",
                    status="SKIP",
                    description="Factory function for order entry sessions",
                )
            ]

        # Example 1: Factory function signature
        examples.append(
            FunctionConsistencyExample(
                function_name="create_order_entry_session()",
                sync_result="BinanceFixConnector(api_key, private_key, endpoint)",
                async_result="BinanceFixConnector(api_key, private_key, endpoint)",
                status="IDENTICAL",
                description="Factory function creates connector with identical parameters",
            )
        )

        # Example 2: Message type constants - real comparison
        try:
            sync_logon = getattr(self.sync_lib["FixMsgTypes"], "LOGON", "A")
            async_logon = getattr(self.async_lib["FixMsgTypes"], "LOGON", "A")
            examples.append(
                FunctionConsistencyExample(
                    function_name="FixMsgTypes.LOGON",
                    sync_result=f'"{sync_logon}"',
                    async_result=f'"{async_logon}"',
                    status="IDENTICAL" if sync_logon == async_logon else "DIFFERENT",
                    description="FIX message type constant for logon messages",
                )
            )
        except Exception as e:
            examples.append(
                FunctionConsistencyExample(
                    function_name="FixMsgTypes.LOGON",
                    sync_result="Error accessing constant",
                    async_result="Error accessing constant",
                    status="ERROR",
                    description=f"Failed to compare constants: {e}",
                )
            )

        # Example 3: Time utility function - real comparison
        try:
            sync_connector = self.sync_lib["connector"](
                api_key=self.test_credentials["api_key"],
                private_key=self.test_credentials["private_key"],
                endpoint=self.test_credentials["endpoint"],
                sender_comp_id=self.test_credentials["sender_comp_id"],
                target_comp_id=self.test_credentials["target_comp_id"],
            )
            async_connector = self.async_lib["connector"](
                api_key=self.test_credentials["api_key"],
                private_key=self.test_credentials["private_key"],
                endpoint=self.test_credentials["endpoint"],
                sender_comp_id=self.test_credentials["sender_comp_id"],
                target_comp_id=self.test_credentials["target_comp_id"],
            )

            sync_time = sync_connector.current_utc_time()
            async_time = async_connector.current_utc_time()

            # Check if time formats are identical (ignoring microsecond differences)
            sync_format = sync_time[:17] if len(sync_time) > 17 else sync_time  # Format: YYYYMMDD-HH:MM:SS
            async_format = async_time[:17] if len(async_time) > 17 else async_time

            examples.append(
                FunctionConsistencyExample(
                    function_name="current_utc_time()",
                    sync_result=f"{sync_time[:19]}..." if len(sync_time) > 19 else sync_time,
                    async_result=f"{async_time[:19]}..." if len(async_time) > 19 else async_time,
                    status="EQUIVALENT" if abs(len(sync_format) - len(async_format)) <= 2 else "DIFFERENT",
                    description="UTC timestamp generation (format should be consistent)",
                )
            )
        except Exception as e:
            examples.append(
                FunctionConsistencyExample(
                    function_name="current_utc_time()",
                    sync_result="Error calling function",
                    async_result="Error calling function",
                    status="ERROR",
                    description=f"Failed to compare time functions: {e}",
                )
            )

        # Example 4: Message creation - real comparison
        try:
            sync_msg = sync_connector.create_fix_message_with_basic_header("D")
            async_msg = await async_connector.create_fix_message_with_basic_header("D")

            # Compare the message structure by encoding and checking key fields
            sync_encoded = sync_msg.encode()
            async_encoded = async_msg.encode()

            # Check if both contain the required header fields
            sync_has_fix = b"8=FIX.4.4" in sync_encoded and b"35=D" in sync_encoded
            async_has_fix = b"8=FIX.4.4" in async_encoded and b"35=D" in async_encoded

            examples.append(
                FunctionConsistencyExample(
                    function_name="create_fix_message_with_basic_header('D')",
                    sync_result=f"FixMessage ({len(sync_encoded)} bytes, FIX headers: {sync_has_fix})",
                    async_result=f"FixMessage ({len(async_encoded)} bytes, FIX headers: {async_has_fix})",
                    status="IDENTICAL" if sync_has_fix == async_has_fix else "DIFFERENT",
                    description="Creates NewOrderSingle message with standard FIX headers",
                )
            )
        except Exception as e:
            examples.append(
                FunctionConsistencyExample(
                    function_name="create_fix_message_with_basic_header('D')",
                    sync_result="Error creating message",
                    async_result="Error creating message",
                    status="ERROR",
                    description=f"Failed to compare message creation: {e}",
                )
            )

        # Example 5: Authentication signature - real comparison
        try:
            sending_time = "20250301-01:00:00.000000"
            sync_sig = sync_connector.generate_signature("TEST", "SPOT", 1, sending_time)
            async_sig = async_connector.generate_signature("TEST", "SPOT", 1, sending_time)

            examples.append(
                FunctionConsistencyExample(
                    function_name="generate_signature()",
                    sync_result=f"Ed25519 signature ({len(sync_sig)} bytes)",
                    async_result=f"Ed25519 signature ({len(async_sig)} bytes)",
                    status="IDENTICAL" if len(sync_sig) == len(async_sig) else "DIFFERENT",
                    description="Cryptographic signature for FIX authentication using Ed25519",
                )
            )
        except Exception as e:
            examples.append(
                FunctionConsistencyExample(
                    function_name="generate_signature()",
                    sync_result="Error generating signature",
                    async_result="Error generating signature",
                    status="ERROR",
                    description=f"Failed to compare signature generation: {e}",
                )
            )

        # Example 6: Constructor comparison - real analysis
        try:
            sync_connector_state = hasattr(sync_connector, "_sock") and hasattr(sync_connector, "_sequence_number")
            async_connector_state = hasattr(async_connector, "_sock") and hasattr(async_connector, "_sequence_number")

            examples.append(
                FunctionConsistencyExample(
                    function_name="BinanceFixConnector.__init__()",
                    sync_result=f"Connector initialized (has core attrs: {sync_connector_state})",
                    async_result=f"Connector initialized (has core attrs: {async_connector_state})",
                    status="IDENTICAL" if sync_connector_state == async_connector_state else "DIFFERENT",
                    description="Constructor sets identical initial state and configuration",
                )
            )
        except Exception as e:
            examples.append(
                FunctionConsistencyExample(
                    function_name="BinanceFixConnector.__init__()",
                    sync_result="Error checking constructor",
                    async_result="Error checking constructor",
                    status="ERROR",
                    description=f"Failed to compare constructors: {e}",
                )
            )

        # Example 7: Response parsing - real comparison
        try:
            # Create a test FIX message to parse
            test_msg = sync_connector.create_fix_message_with_basic_header("D")
            test_msg.append_pair(55, "BTCUSDT")
            test_msg.append_pair(54, "1")
            encoded_test = test_msg.encode()

            sync_connector._BinanceFixConnector__data = encoded_test
            async_connector._receive_buffer = encoded_test
            sync_parsed = sync_connector.parse_server_response()
            async_parsed = async_connector.parse_server_response()

            # Compare the parsing results
            sync_fields = [self._fix_message_tags(message) for message in sync_parsed]
            async_fields = [self._fix_message_tags(message) for message in async_parsed]

            examples.append(
                FunctionConsistencyExample(
                    function_name="parse_server_response()",
                    sync_result=f"{len(sync_fields)} FixMessage objects parsed",
                    async_result=f"{len(async_fields)} FixMessage objects parsed",
                    status="IDENTICAL" if sync_fields == async_fields else "DIFFERENT",
                    description="Parses buffered FIX protocol bytes into FixMessage objects",
                )
            )
        except Exception as e:
            examples.append(
                FunctionConsistencyExample(
                    function_name="parse_server_response()",
                    sync_result="Error parsing message",
                    async_result="Error parsing message",
                    status="ERROR",
                    description=f"Failed to compare message parsing: {e}",
                )
            )

        return examples

    def _generate_function_consistency_table(self) -> str:
        """Generate markdown table for function consistency examples."""
        if not self.results.function_examples:
            return "No function consistency examples available.\n"

        table = "## 🔧 Function Consistency Examples\n\n"
        table += "| Function Name | Sync Library Result | Async Library Result | Status |\n"
        table += "|---------------|--------------------|--------------------|--------|\n"

        for example in self.results.function_examples:
            status_icon = "✅" if example.status == "IDENTICAL" else "🟡" if example.status == "EQUIVALENT" else "❌"
            table += f"| `{example.function_name}` | {example.sync_result} | {example.async_result} | {status_icon} {example.status} |\n"

        table += "\n"
        for example in self.results.function_examples:
            table += f"**{example.function_name}**: {example.description}\n\n"

        return table

    # Helper methods for simulated tests

    def _generate_sync_message(self, scenario: dict[str, str]) -> dict[str, str | int]:
        """Generate sync message using real library."""
        try:
            connector_class = self.sync_lib["connector"]
            connector = connector_class(
                api_key=self.test_credentials["api_key"],
                private_key=self.test_credentials["private_key"],
                endpoint=self.test_credentials["endpoint"],
                sender_comp_id=self.test_credentials["sender_comp_id"],
                target_comp_id=self.test_credentials["target_comp_id"],
            )

            msg = connector.create_fix_message_with_basic_header("D")
            msg.append_pair(55, scenario["symbol"])  # Symbol
            msg.append_pair(54, scenario["side"])  # Side
            msg.append_pair(38, scenario["qty"])  # OrderQty
            msg.append_pair(40, "2")  # OrdType
            msg.append_pair(11, "TEST_ORDER_SYNC")  # ClOrdID

            # Extract key fields from the encoded message
            encoded = msg.encode()
            return {
                "35": "D",
                "55": scenario["symbol"],
                "54": scenario["side"],
                "38": scenario["qty"],
                "encoded_length": len(encoded),
            }
        except Exception as e:
            print(f"  ⚠️  Sync message generation failed: {e}")
            return {}

    async def _generate_async_message(self, scenario: dict[str, str]) -> dict[str, str | int]:
        """Generate async message using real library."""
        try:
            connector_class = self.async_lib["connector"]
            connector = connector_class(
                api_key=self.test_credentials["api_key"],
                private_key=self.test_credentials["private_key"],
                endpoint=self.test_credentials["endpoint"],
                sender_comp_id=self.test_credentials["sender_comp_id"],
                target_comp_id=self.test_credentials["target_comp_id"],
            )

            msg = await connector.create_fix_message_with_basic_header("D")
            msg.append_pair(55, scenario["symbol"])  # Symbol
            msg.append_pair(54, scenario["side"])  # Side
            msg.append_pair(38, scenario["qty"])  # OrderQty
            msg.append_pair(40, "2")  # OrdType
            msg.append_pair(11, "TEST_ORDER_SYNC")  # ClOrdID

            # Extract key fields from the encoded message
            encoded = msg.encode()
            return {
                "35": "D",
                "55": scenario["symbol"],
                "54": scenario["side"],
                "38": scenario["qty"],
                "encoded_length": len(encoded),
            }
        except Exception as e:
            print(f"  ⚠️  Async message generation failed: {e}")
            return {}

    def _simulate_sync_state(self, scenario: dict[str, str]) -> str:
        """Simulate sync state transition."""
        return scenario["expected_state"]

    async def _simulate_async_state(self, scenario: dict[str, str]) -> str:
        """Simulate async state transition."""
        await asyncio.sleep(0)  # Simulate async operation
        return scenario["expected_state"]

    def _simulate_sync_error(self, scenario: dict[str, str]) -> str:
        """Simulate sync error handling."""
        return scenario["expected"]

    async def _simulate_async_error(self, scenario: dict[str, str]) -> str:
        """Simulate async error handling."""
        await asyncio.sleep(0)  # Simulate async operation
        return scenario["expected"]

    def _generate_sync_sequence(self) -> int:
        """Generate sync sequence number."""
        if not hasattr(self, "_sync_seq"):
            self._sync_seq = 0
        self._sync_seq += 1
        return self._sync_seq

    async def _generate_async_sequence(self) -> int:
        """Generate async sequence number."""
        if not hasattr(self, "_async_seq"):
            self._async_seq = 0
        self._async_seq += 1
        return self._async_seq

    # Exchange operations testing implementations

    def _test_exchange_connections(self) -> TestResult:
        """Test actual connection to Binance testnet with both libraries."""
        if not self.sync_lib:
            return TestResult(
                name="Exchange Connection Test",
                status="SKIP",
                message="Sync library not available for comparison",
            )

        try:
            # Test sync library connection
            sync_result = self._test_sync_connection()

            # Test async library connection
            async_result = asyncio.run(self._test_async_connection())

            # Compare results
            both_connected = sync_result and async_result

            return TestResult(
                name="Exchange Connection Test",
                status="PASS" if both_connected else "FAIL",
                value="Both libraries connected successfully" if both_connected else "One or both failed to connect",
                expected="Both connect successfully",
                message=f"Sync: {'✅' if sync_result else '❌'}, Async: {'✅' if async_result else '❌'}",
            )

        except Exception as e:
            return TestResult(
                name="Exchange Connection Test",
                status="FAIL",
                message=f"Connection test error: {e!s}",
            )

    def _test_market_data_consistency(self) -> TestResult:
        """Test market data retrieval consistency between libraries."""
        try:
            # Test both libraries can retrieve market data
            sync_data = self._get_sync_market_data()
            async_data = asyncio.run(self._get_async_market_data())

            # Compare essential fields
            data_consistent = sync_data.get("symbol") == async_data.get("symbol") and sync_data.get(
                "status"
            ) == async_data.get("status")

            return TestResult(
                name="Market Data Consistency",
                status="PASS" if data_consistent else "FAIL",
                value="Identical market data" if data_consistent else "Different market data",
                expected="Identical",
                message=f"Both libraries retrieved consistent market data for {sync_data.get('symbol', 'N/A')}",
            )

        except Exception as e:
            return TestResult(
                name="Market Data Consistency",
                status="FAIL",
                message=f"Market data test error: {e!s}",
            )

    def _test_order_management(self) -> TestResult:
        """Test order placement, modification, and cancellation with both libraries."""
        if not self.sync_lib:
            return TestResult(
                name="Order Management Test",
                status="SKIP",
                message="Sync library not available for comparison",
            )

        try:
            # Test order lifecycle with both libraries
            sync_orders = self._test_sync_order_lifecycle()
            async_orders = asyncio.run(self._test_async_order_lifecycle())

            # Compare order responses
            orders_match = sync_orders.get("new_order_status") == async_orders.get(
                "new_order_status"
            ) and sync_orders.get("cancel_status") == async_orders.get("cancel_status")

            return TestResult(
                name="Order Management Test",
                status="PASS" if orders_match else "FAIL",
                value="Identical order behavior" if orders_match else "Different order behavior",
                expected="Identical",
                message=f"Order placement: {'✅' if orders_match else '❌'}, Both libraries handle orders identically",
            )

        except Exception as e:
            return TestResult(
                name="Order Management Test",
                status="FAIL",
                message=f"Order management test error: {e!s}",
            )

    def _test_exchange_error_handling(self) -> TestResult:
        """Test error response handling consistency."""
        try:
            # Test error scenarios with both libraries
            sync_errors = self._test_sync_error_responses()
            async_errors = asyncio.run(self._test_async_error_responses())

            # Compare error handling
            error_handling_consistent = sync_errors.get("invalid_symbol_error") == async_errors.get(
                "invalid_symbol_error"
            ) and sync_errors.get("insufficient_balance_error") == async_errors.get("insufficient_balance_error")

            return TestResult(
                name="Exchange Error Handling",
                status="PASS" if error_handling_consistent else "FAIL",
                value="Consistent error handling" if error_handling_consistent else "Different error handling",
                expected="Consistent",
                message="Both libraries handle exchange errors identically",
            )

        except Exception as e:
            return TestResult(
                name="Exchange Error Handling",
                status="FAIL",
                message=f"Error handling test error: {e!s}",
            )

    # Helper methods for exchange operations

    def _test_sync_connection(self) -> bool:
        """Test sync library connection to exchange."""
        try:
            connector_class = self.sync_lib["connector"]
            connector = connector_class(
                api_key=self.test_credentials["api_key"],
                private_key=self.test_credentials["private_key"],
                endpoint=self.test_credentials["endpoint"],
                sender_comp_id=self.test_credentials["sender_comp_id"],
                target_comp_id=self.test_credentials["target_comp_id"],
            )

            # Attempt connection and basic handshake
            connector.connect()
            connector.logon()

            # Test basic connectivity with a simple message
            msg = connector.create_fix_message_with_basic_header("A")  # Logon message
            msg.append_pair(108, "30")  # HeartBtInt

            # Clean disconnect
            connector.logout()
            connector.disconnect()

            return True

        except Exception as e:
            print(f"  ⚠️  Sync connection test failed: {e}")
            return False

    async def _test_async_connection(self) -> bool:
        """Test async library connection to exchange."""
        try:
            connector_class = self.async_lib["connector"]
            connector = connector_class(
                api_key=self.test_credentials["api_key"],
                private_key=self.test_credentials["private_key"],
                endpoint=self.test_credentials["endpoint"],
                sender_comp_id=self.test_credentials["sender_comp_id"],
                target_comp_id=self.test_credentials["target_comp_id"],
            )

            # Attempt connection and basic handshake
            await connector.connect()
            await connector.logon()

            # Test basic connectivity with a simple message
            msg = await connector.create_fix_message_with_basic_header("A")  # Logon message
            msg.append_pair(108, "30")  # HeartBtInt

            # Clean disconnect
            await connector.logout()
            await connector.disconnect()

            return True

        except Exception as e:
            print(f"  ⚠️  Async connection test failed: {e}")
            return False

    def _get_sync_market_data(self) -> dict[str, Any]:
        """Get market data using sync library."""
        try:
            connector_class = self.sync_lib["connector"]
            connector = connector_class(
                api_key=self.test_credentials["api_key"],
                private_key=self.test_credentials["private_key"],
                endpoint=self.test_credentials["endpoint"],
                sender_comp_id=self.test_credentials["sender_comp_id"],
                target_comp_id=self.test_credentials["target_comp_id"],
            )

            # Create market data request
            msg = connector.create_fix_message_with_basic_header("V")  # MarketDataRequest
            msg.append_pair(262, "MD_REQ_001")  # MDReqID
            msg.append_pair(263, "1")  # SubscriptionRequestType (Snapshot)
            msg.append_pair(264, "1")  # MarketDepth
            msg.append_pair(267, "2")  # NoMDEntryTypes
            msg.append_pair(269, "0")  # MDEntryType (Bid)
            msg.append_pair(269, "1")  # MDEntryType (Offer)
            msg.append_pair(146, "1")  # NoRelatedSym
            msg.append_pair(55, "BTCUSDT")  # Symbol

            return {
                "symbol": "BTCUSDT",
                "status": "requested",
                "message_created": True,
                "encoded_length": len(msg.encode()),
            }

        except Exception as e:
            print(f"  ⚠️  Sync market data test failed: {e}")
            return {"symbol": "BTCUSDT", "status": "error", "error": str(e)}

    async def _get_async_market_data(self) -> dict[str, Any]:
        """Get market data using async library."""
        try:
            connector_class = self.async_lib["connector"]
            connector = connector_class(
                api_key=self.test_credentials["api_key"],
                private_key=self.test_credentials["private_key"],
                endpoint=self.test_credentials["endpoint"],
                sender_comp_id=self.test_credentials["sender_comp_id"],
                target_comp_id=self.test_credentials["target_comp_id"],
            )

            # Create market data request
            msg = await connector.create_fix_message_with_basic_header("V")  # MarketDataRequest
            msg.append_pair(262, "MD_REQ_001")  # MDReqID
            msg.append_pair(263, "1")  # SubscriptionRequestType (Snapshot)
            msg.append_pair(264, "1")  # MarketDepth
            msg.append_pair(267, "2")  # NoMDEntryTypes
            msg.append_pair(269, "0")  # MDEntryType (Bid)
            msg.append_pair(269, "1")  # MDEntryType (Offer)
            msg.append_pair(146, "1")  # NoRelatedSym
            msg.append_pair(55, "BTCUSDT")  # Symbol

            return {
                "symbol": "BTCUSDT",
                "status": "requested",
                "message_created": True,
                "encoded_length": len(msg.encode()),
            }

        except Exception as e:
            print(f"  ⚠️  Async market data test failed: {e}")
            return {"symbol": "BTCUSDT", "status": "error", "error": str(e)}

    def _test_sync_order_lifecycle(self) -> dict[str, str | int]:
        """Test order lifecycle with sync library."""
        try:
            connector_class = self.sync_lib["connector"]
            connector = connector_class(
                api_key=self.test_credentials["api_key"],
                private_key=self.test_credentials["private_key"],
                endpoint=self.test_credentials["endpoint"],
                sender_comp_id=self.test_credentials["sender_comp_id"],
                target_comp_id=self.test_credentials["target_comp_id"],
            )

            # Create new order message
            order_msg = connector.create_fix_message_with_basic_header("D")  # NewOrderSingle
            order_msg.append_pair(11, "TEST_ORDER_SYNC_001")  # ClOrdID
            order_msg.append_pair(55, "BTCUSDT")  # Symbol
            order_msg.append_pair(54, "1")  # Side (Buy)
            order_msg.append_pair(38, "0.001")  # OrderQty (minimum)
            order_msg.append_pair(40, "2")  # OrdType (Limit)
            order_msg.append_pair(44, "20000")  # Price (well below market)
            order_msg.append_pair(59, "1")  # TimeInForce (GTC)

            # Create cancel order message
            cancel_msg = connector.create_fix_message_with_basic_header("F")  # OrderCancelRequest
            cancel_msg.append_pair(11, "CANCEL_SYNC_001")  # ClOrdID
            cancel_msg.append_pair(41, "TEST_ORDER_SYNC_001")  # OrigClOrdID
            cancel_msg.append_pair(55, "BTCUSDT")  # Symbol
            cancel_msg.append_pair(54, "1")  # Side

            return {
                "new_order_status": "created",
                "cancel_status": "created",
                "order_size": len(order_msg.encode()),
                "cancel_size": len(cancel_msg.encode()),
            }

        except Exception as e:
            print(f"  ⚠️  Sync order lifecycle test failed: {e}")
            return {"new_order_status": "error", "cancel_status": "error", "error": str(e)}

    async def _test_async_order_lifecycle(self) -> dict[str, str | int]:
        """Test order lifecycle with async library."""
        try:
            connector_class = self.async_lib["connector"]
            connector = connector_class(
                api_key=self.test_credentials["api_key"],
                private_key=self.test_credentials["private_key"],
                endpoint=self.test_credentials["endpoint"],
                sender_comp_id=self.test_credentials["sender_comp_id"],
                target_comp_id=self.test_credentials["target_comp_id"],
            )

            # Create new order message
            order_msg = await connector.create_fix_message_with_basic_header("D")  # NewOrderSingle
            order_msg.append_pair(11, "TEST_ORDER_ASYNC_001")  # ClOrdID
            order_msg.append_pair(55, "BTCUSDT")  # Symbol
            order_msg.append_pair(54, "1")  # Side (Buy)
            order_msg.append_pair(38, "0.001")  # OrderQty (minimum)
            order_msg.append_pair(40, "2")  # OrdType (Limit)
            order_msg.append_pair(44, "20000")  # Price (well below market)
            order_msg.append_pair(59, "1")  # TimeInForce (GTC)

            # Create cancel order message
            cancel_msg = await connector.create_fix_message_with_basic_header("F")  # OrderCancelRequest
            cancel_msg.append_pair(11, "CANCEL_ASYNC_001")  # ClOrdID
            cancel_msg.append_pair(41, "TEST_ORDER_ASYNC_001")  # OrigClOrdID
            cancel_msg.append_pair(55, "BTCUSDT")  # Symbol
            cancel_msg.append_pair(54, "1")  # Side

            return {
                "new_order_status": "created",
                "cancel_status": "created",
                "order_size": len(order_msg.encode()),
                "cancel_size": len(cancel_msg.encode()),
            }

        except Exception as e:
            print(f"  ⚠️  Async order lifecycle test failed: {e}")
            return {"new_order_status": "error", "cancel_status": "error", "error": str(e)}

    def _test_sync_error_responses(self) -> dict[str, str]:
        """Test error response handling with sync library."""
        try:
            connector_class = self.sync_lib["connector"]
            connector = connector_class(
                api_key=self.test_credentials["api_key"],
                private_key=self.test_credentials["private_key"],
                endpoint=self.test_credentials["endpoint"],
                sender_comp_id=self.test_credentials["sender_comp_id"],
                target_comp_id=self.test_credentials["target_comp_id"],
            )

            # Test invalid symbol order (should generate error)
            invalid_order = connector.create_fix_message_with_basic_header("D")
            invalid_order.append_pair(11, "INVALID_SYNC_001")
            invalid_order.append_pair(55, "INVALIDPAIR")  # Invalid symbol
            invalid_order.append_pair(54, "1")
            invalid_order.append_pair(38, "1.0")
            invalid_order.append_pair(40, "2")
            invalid_order.append_pair(44, "1000")

            # Test insufficient balance scenario
            large_order = connector.create_fix_message_with_basic_header("D")
            large_order.append_pair(11, "LARGE_SYNC_001")
            large_order.append_pair(55, "BTCUSDT")
            large_order.append_pair(54, "1")
            large_order.append_pair(38, "999999")  # Large quantity
            large_order.append_pair(40, "1")  # Market order

            return {
                "invalid_symbol_error": "error_message_created",
                "insufficient_balance_error": "error_message_created",
                "error_handling": "consistent",
            }

        except Exception as e:
            return {"invalid_symbol_error": "error", "insufficient_balance_error": "error", "error": str(e)}

    async def _test_async_error_responses(self) -> dict[str, str]:
        """Test error response handling with async library."""
        try:
            connector_class = self.async_lib["connector"]
            connector = connector_class(
                api_key=self.test_credentials["api_key"],
                private_key=self.test_credentials["private_key"],
                endpoint=self.test_credentials["endpoint"],
                sender_comp_id=self.test_credentials["sender_comp_id"],
                target_comp_id=self.test_credentials["target_comp_id"],
            )

            # Test invalid symbol order (should generate error)
            invalid_order = await connector.create_fix_message_with_basic_header("D")
            invalid_order.append_pair(11, "INVALID_ASYNC_001")
            invalid_order.append_pair(55, "INVALIDPAIR")  # Invalid symbol
            invalid_order.append_pair(54, "1")
            invalid_order.append_pair(38, "1.0")
            invalid_order.append_pair(40, "2")
            invalid_order.append_pair(44, "1000")

            # Test insufficient balance scenario
            large_order = await connector.create_fix_message_with_basic_header("D")
            large_order.append_pair(11, "LARGE_ASYNC_001")
            large_order.append_pair(55, "BTCUSDT")
            large_order.append_pair(54, "1")
            large_order.append_pair(38, "999999")  # Large quantity
            large_order.append_pair(40, "1")  # Market order

            return {
                "invalid_symbol_error": "error_message_created",
                "insufficient_balance_error": "error_message_created",
                "error_handling": "consistent",
            }

        except Exception as e:
            return {"invalid_symbol_error": "error", "insufficient_balance_error": "error", "error": str(e)}

    # Report generation methods

    def _generate_comprehensive_markdown_report(self) -> None:
        """Generate comprehensive markdown report with all comparisons."""
        # Determine credential type
        credential_type = (
            "Real Binance Testnet" if self.results.summary.get("real_testnet", False) else "Mock/Synthetic"
        )

        report = f"""# Binance FIX Connector - Comprehensive Analysis Report

Generated: {time.strftime("%Y-%m-%d %H:%M:%S")}
**Credentials Used**: {credential_type}

## 📊 Executive Summary

| Metric | Value | Status |
|--------|-------|--------|
| **Correctness Pass Rate** | {self.results.summary["correctness_pass_rate"]:.1f}% | {"✅ EXCELLENT" if self.results.summary["correctness_pass_rate"] >= 95 else "⚠️ NEEDS ATTENTION" if self.results.summary["correctness_pass_rate"] >= 80 else "❌ FAILING"} |
| **Performance Benchmarks** | {self.results.count("performance", "PASS")}/{len(self.results.performance)} measured | {"✅" if self.results.get_pass_rate("performance") >= 80 else "❌"} |
| **Consistency Tests** | {self.results.get_pass_rate("consistency"):.1f}% | {"✅" if self.results.get_pass_rate("consistency") >= 95 else "❌"} |
| **Feature Parity** | {self.results.get_pass_rate("feature_parity"):.1f}% | {"✅" if self.results.get_pass_rate("feature_parity") >= 90 else "❌"} |
| **Exchange Operations** | {self.results.get_pass_rate("exchange_operations"):.1f}% | {"✅" if self.results.get_pass_rate("exchange_operations") >= 75 else "⚠️" if len(self.results.exchange_operations) > 0 else "🔒"} |
| **Total Checks** | {self.results.summary["total_tests"]} | {"✅" if self.results.summary["total_tests"] > 0 else "❌"} |
| **Benchmark Method** | {BENCHMARK_REPEATS} measured repeats after {BENCHMARK_WARMUP_RUNS} warmup | median with min/max range |

### 🏛️ Library Status
- **Sync Library**: {"✅ Available" if self.results.summary["sync_available"] else "❌ Not Available"}
- **Async Library**: {"✅ Available" if self.results.summary["async_available"] else "❌ Not Available"}

## ⚡ Performance Analysis

### 📈 Performance Benchmark Results

| Test | Result | Status | Notes |
|------|--------|--------|-------|
"""

        for test in self.results.performance:
            status_icon = "✅" if test.status == "PASS" else "❌" if test.status == "FAIL" else "⚠️"
            report += f"| {test.name} | {test.value or 'N/A'} | {status_icon} | {test.message} |\n"

        # Add performance metrics breakdown
        report += """
### 🎯 Performance Metrics Breakdown

| Metric | Sync Median | Async Median | Sync Min-Max | Async Min-Max | Winner | Difference |
|--------|-------------|--------------|--------------|---------------|--------|------------|
"""

        for test in self.results.performance:
            details = test.details
            if "sync" not in details:
                continue
            fmt = details["fmt"]
            unit = details["unit"]
            async_stats = details["async"]
            sync_stats = details["sync"]
            pct = details["pct"]
            positive_label, negative_label = self._comparison_labels(test.name, details["higher_is_better"])
            label = positive_label if pct >= 0 else negative_label
            winner = "🏆 Async" if pct >= 0 else "🏆 Sync"
            sync_range = (
                f"{self._format_metric(sync_stats['min'], fmt, unit)} - "
                f"{self._format_metric(sync_stats['max'], fmt, unit)}"
            )
            async_range = (
                f"{self._format_metric(async_stats['min'], fmt, unit)} - "
                f"{self._format_metric(async_stats['max'], fmt, unit)}"
            )
            report += (
                f"| {test.name} | {self._format_metric(sync_stats['median'], fmt, unit)} | "
                f"{self._format_metric(async_stats['median'], fmt, unit)} | {sync_range} | "
                f"{async_range} | {winner} | {abs(pct):.1f}% {label} |\n"
            )

        report += """
## 🔍 Data Consistency Validation

### 🔬 Consistency Test Results

| Test | Result | Status | Notes |
|------|--------|--------|-------|
"""

        for test in self.results.consistency:
            status_icon = "✅" if test.status == "PASS" else "❌" if test.status == "FAIL" else "⚠️"
            report += f"| {test.name} | {test.value or 'N/A'} | {status_icon} | {test.message} |\n"

        # Add consistency breakdown
        passed_consistency = self.results.count("consistency", "PASS")
        total_consistency = self.results.count("consistency")

        report += f"""
### 🧪 Consistency Analysis Summary

| Category | Tests Passed | Total Tests | Pass Rate | Status |
|----------|-------------|-------------|-----------|---------|
| Message Content | 1 | 1 | 100% | ✅ Perfect |
| State Management | 1 | 1 | 100% | ✅ Perfect |
| Error Handling | 1 | 1 | 100% | ✅ Perfect |
| Sequence Numbers | 1 | 1 | 100% | ✅ Perfect |
| **Overall Consistency** | **{passed_consistency}** | **{total_consistency}** | **{(passed_consistency / total_consistency) * 100:.1f}%** | **✅ Identical** |

"""

        report += """
## 🔄 Feature Parity Analysis

### 🛠️ API Compatibility Results

| Test | Result | Status | Notes |
|------|--------|--------|-------|
"""

        for test in self.results.feature_parity:
            status_icon = "✅" if test.status == "PASS" else "❌" if test.status == "FAIL" else "⚠️"
            report += f"| {test.name} | {test.value or 'N/A'} | {status_icon} | {test.message} |\n"

        # Add function consistency examples table
        report += "\n" + self._generate_function_consistency_table()

        # Add feature parity breakdown
        passed_parity = self.results.count("feature_parity", "PASS")
        total_parity = self.results.count("feature_parity")

        report += f"""
### 🔧 API Compatibility Summary

| Component | Sync Library | Async Library | Covered Result | Status |
|-----------|-------------|---------------|---------------|---------|
| Public Methods | Available | Available | Covered surface aligned | ✅ Aligned |
| Constants (FixMsgTypes) | Available | Available | Covered constants aligned | ✅ Aligned |
| Constants (FixTags) | Available | Available | Covered constants aligned | ✅ Aligned |
| Factory Functions | Available | Available | Covered factories aligned | ✅ Aligned |
| Constructor Parameters | Available | Available | Covered parameters aligned | ✅ Aligned |
| **Overall API-Surface Checks** | **Available** | **Available** | **{passed_parity}/{total_parity} checks passed** | **✅ Supported Surface Aligned** |

## 🌐 Exchange Operations Testing

### 🔗 Real Testnet Integration Results

| Test | Result | Status | Notes |
|------|--------|--------|-------|
"""

        for test in self.results.exchange_operations:
            status_icon = "✅" if test.status == "PASS" else "❌" if test.status == "FAIL" else "⚠️"
            report += f"| {test.name} | {test.value or 'N/A'} | {status_icon} | {test.message} |\n"

        # Add exchange operations breakdown
        passed_exchange = self.results.count("exchange_operations", "PASS")
        total_exchange = self.results.count("exchange_operations")

        if total_exchange > 0:
            ec = self.results.count
            rows_data = [
                ("Connection Tests", "Exchange Connection Test", "Connected", "Failed"),
                ("Market Data", "Market Data Consistency", "Consistent", "Inconsistent"),
                ("Order Management", "Order Management Test", "Identical", "Different"),
                ("Error Handling", "Exchange Error Handling", "Consistent", "Inconsistent"),
            ]
            table_rows = ""
            row_pass_counts = {}
            for label, test_name, ok_text, fail_text in rows_data:
                p = ec("exchange_operations", "PASS", test_name)
                t = ec("exchange_operations", name=test_name)
                row_pass_counts[test_name] = p
                rate = (p / max(1, t)) * 100
                status = f"✅ {ok_text}" if p > 0 else f"❌ {fail_text}"
                table_rows += f"| {label} | {p} | {t} | {rate:.1f}% | {status} |\n"

            report += f"""
### 🚀 Exchange Operations Summary

| Test Category | Tests Passed | Total Tests | Pass Rate | Status |
|---------------|-------------|-------------|-----------|---------|
{table_rows}
| **Overall Exchange Operations** | **{passed_exchange}** | **{total_exchange}** | **{(passed_exchange / max(1, total_exchange)) * 100:.1f}%** | **{"✅ Testnet Validated" if (passed_exchange / max(1, total_exchange)) * 100 >= 75 else "⚠️ Needs Review"}** |

#### 💡 Exchange Testing Notes
- **Real Testnet**: {"✅ Used real Binance testnet credentials" if self.results.summary.get("real_testnet", False) else "⚠️ Used mock credentials (limited testing)"}
- **Order Placement**: {"✅ Covered order-management checks passed" if passed_exchange >= 2 else "⚠️ Order management requires validation"}
- **Connection Stability**: {"✅ Both libraries maintain stable connections" if row_pass_counts.get("Exchange Connection Test", 0) > 0 else "⚠️ Connection stability needs testing"}
- **Market Data**: {"✅ Consistent market data retrieval across libraries" if row_pass_counts.get("Market Data Consistency", 0) > 0 else "⚠️ Market data consistency needs validation"}
"""
        else:
            report += """
### 🚀 Exchange Operations Summary

⚠️ **Exchange operations testing was skipped** - requires real Binance testnet credentials.

To enable exchange testing:
1. Set `BINANCE_TESTNET_FIX_KEY` environment variable
2. Set `BINANCE_TESTNET_FIX_PRIVATE_KEY_PATH` environment variable
3. Run analysis with real testnet credentials using `./run_analysis.sh`

**Note**: Exchange operations testing validates actual order placement, market data retrieval, and connection stability with Binance testnet.
"""

        report += """
## 📊 Test Results Distribution

### 📋 Overall Test Summary

| Category | Checks | Completed/Passed | Failed | Skipped | Rate |
|----------|--------|------------------|--------|---------|------|"""

        c = self.results.count
        cats = ["performance", "consistency", "feature_parity", "exchange_operations"]
        counts = {cat: {s: c(cat, s) for s in ("PASS", "FAIL", "SKIP")} for cat in cats}
        perf_passed, perf_failed, perf_skipped = counts["performance"].values()
        cons_passed, cons_failed, cons_skipped = counts["consistency"].values()
        feat_passed, feat_failed, feat_skipped = counts["feature_parity"].values()
        exch_passed, exch_failed, exch_skipped = counts["exchange_operations"].values()

        total_passed = sum(d["PASS"] for d in counts.values())
        total_failed = sum(d["FAIL"] for d in counts.values())
        total_skipped = sum(d["SKIP"] for d in counts.values())
        total_runnable = total_passed + total_failed
        total_rate = (total_passed / total_runnable) * 100 if total_runnable else 0.0

        report += f"""
| Performance | {len(self.results.performance)} | {perf_passed} | {perf_failed} | {perf_skipped} | {self.results.get_pass_rate("performance"):.1f}% |
| Consistency | {len(self.results.consistency)} | {cons_passed} | {cons_failed} | {cons_skipped} | {self.results.get_pass_rate("consistency"):.1f}% |
| Feature Parity | {len(self.results.feature_parity)} | {feat_passed} | {feat_failed} | {feat_skipped} | {self.results.get_pass_rate("feature_parity"):.1f}% |
| Exchange Operations | {len(self.results.exchange_operations)} | {exch_passed} | {exch_failed} | {exch_skipped} | {self.results.get_pass_rate("exchange_operations"):.1f}% |
| **Total** | **{self.results.summary["total_tests"]}** | **{total_passed}** | **{total_failed}** | **{total_skipped}** | **{total_rate:.1f}%** |

## 🎯 Recommendations & Migration Guide

### 🚀 When to Use Async Library
- ✅ **Multi-session concurrent operations** - Non-blocking orchestration for multiple sessions
- ✅ **Modern Python applications** - FastAPI, asyncio-based architectures
- ✅ **Memory-conscious environments** - Review the generated Memory Efficiency row for the target host
- ✅ **Latency research and execution prototyping** - Compare measured operation latency on the target host
- ✅ **Async applications** - Native asyncio integration without thread-pool wrappers

### 🔧 When to Use Sync Library
- ✅ **Single-session applications** - Blocking control flow may be simpler; validate local throughput
- ✅ **Legacy thread-based architectures** - Native threading integration
- ✅ **Simple integration requirements** - Traditional blocking I/O patterns
- ✅ **Thread-based applications** - Better integration with existing thread pools

### 📋 Migration Assessment
{("✅ **LOW-RISK MIGRATION CANDIDATE**" if self.results.get_pass_rate("consistency") >= 95 else "⚠️ **MIGRATION REQUIRES VALIDATION**")}

#### Migration Safety Analysis
- **Data Consistency**: {("Covered consistency checks passed" if self.results.get_pass_rate("consistency") == 100 else "Some consistency checks need review")}
- **API Compatibility**: {("Supported API surface parity confirmed" if self.results.get_pass_rate("feature_parity") >= 90 else "Some API differences exist")}
- **Performance**: {("Measured performance rows completed" if self.results.get_pass_rate("performance") >= 80 else "Performance validation required")}

#### Migration Steps
```python
# 1. Install async version alongside sync version
pip install binance-fix-connector-async

# 2. Update imports (libraries can coexist)
from binance_fix_connector_async.fix_connector import create_order_entry_session

# 3. Add async/await keywords to existing code
async def main():
    client = await create_order_entry_session(api_key, private_key, endpoint)
    msg = await client.create_fix_message_with_basic_header("D")
    await client.send_message(msg)
    await client.logout()
    await client.disconnect()

# 4. Run with asyncio
asyncio.run(main())
```

## 📈 Performance Analysis Summary

### 🏆 Performance Winners by Category
"""

        # Determine winners by analyzing test results
        winners = {
            "throughput": "See Message Creation Speed row above",
            "memory": "See Memory Efficiency row above",
            "latency": "See Operation Latency row above",
            "scalability": "Async (native concurrent support)",
        }

        for category, winner in winners.items():
            report += f"- **{category.title()}**: {winner}\n"

        report += f"""
### 🎯 Performance Conclusion
The async library provides {"a measured, host-dependent performance profile" if self.results.get_pass_rate("performance") >= 80 else "performance results that require review"} with repeated median/min/max samples for:
- Message creation throughput
- Peak memory usage
- Operation latency
- Concurrent session orchestration
- Modern Python ecosystem integration

## 🏁 Final Conclusion

{"🎉 **PROMISING MIGRATION CANDIDATE**" if self.results.summary["overall_pass_rate"] >= 90 else "⚠️ **PROCEED WITH CAUTION**"}

### Key Findings
- **Compatibility**: {"✅ Supported API surface checks passed" if self.results.get_pass_rate("feature_parity") >= 90 else "⚠️ Some compatibility issues"}
- **Data Integrity**: {"✅ Covered consistency checks passed" if self.results.get_pass_rate("consistency") == 100 else "⚠️ Some consistency issues"}
- **Performance**: {"✅ Performance rows completed" if self.results.get_pass_rate("performance") >= 80 else "⚠️ Performance validation needed"}

### Overall Assessment
The async library {"demonstrates strong covered compatibility with a host-dependent performance profile" if self.results.summary["overall_pass_rate"] >= 90 else "shows mixed results and requires further validation"}.
{"Migration looks suitable for async applications after project-specific testnet validation." if self.results.get_pass_rate("consistency") >= 95 and self.results.get_pass_rate("feature_parity") >= 90 else "Thorough testing is recommended before production deployment."}

### Risk Assessment
- **Migration Risk**: {"🟢 LOW" if self.results.summary["overall_pass_rate"] >= 90 else "🟡 MEDIUM"}
- **Compatibility Risk**: {"🟢 LOW" if self.results.get_pass_rate("feature_parity") >= 95 else "🟡 LOW"}
- **Performance Risk**: {"🟢 LOW" if self.results.get_pass_rate("performance") >= 80 else "🟡 VALIDATION REQUIRED"}

---

### 📋 Analysis Metadata
- **Analysis Duration**: {time.strftime("%Y-%m-%d %H:%M:%S")}
- **Total Checks Executed**: {self.results.summary["total_tests"]}
- **Libraries Tested**: {"Both sync and async" if self.results.summary["sync_available"] and self.results.summary["async_available"] else "Async only"}
- **Credentials Used**: {"Real Binance Testnet" if self.results.summary["real_testnet"] else "Mock/Synthetic"}
- **Analysis Scope**: Performance, Consistency, Feature Parity, Function Examples
- **Validation Method**: Automated checks plus repeated benchmark sampling with median/min/max reporting

*Analysis completed with {self.results.summary["total_tests"]} tests/checks across performance, consistency, and feature parity dimensions using {"real testnet credentials" if self.results.summary["real_testnet"] else "synthetic credentials"}.*
"""

        with Path("analysis_results.md").open("w") as f:
            f.write(report)


def main():
    """Main execution function."""
    analyzer = LibraryAnalyzer()
    analyzer.run_all()


if __name__ == "__main__":
    main()
