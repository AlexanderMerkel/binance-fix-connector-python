#!/usr/bin/env python3
"""
E2E Test Runner for Binance FIX Connector (Async)

This script provides a comprehensive test runner for the async Binance FIX Connector
E2E test suite, with support for different test categories and environments.
"""

import argparse
import logging
import os
import sys
import time
from pathlib import Path

import pytest

log = logging.getLogger(__name__)


class E2ETestRunner:
    """Manages execution of E2E tests with various configurations."""

    def __init__(self):
        self.test_categories = {
            "framework": ["test_e2e_framework.py"],
            "order_lifecycle": ["test_e2e_order_lifecycle.py"],
            "market_data": ["test_e2e_market_data.py"],
            "drop_copy": ["test_e2e_drop_copy.py"],
            "multi_session": ["test_e2e_multi_session.py"],
            "error_recovery": ["test_e2e_error_recovery.py"],
            "comprehensive": ["test_e2e_comprehensive.py"],
            "all": [
                "test_e2e_framework.py",
                "test_e2e_order_lifecycle.py",
                "test_e2e_market_data.py",
                "test_e2e_drop_copy.py",
                "test_e2e_multi_session.py",
                "test_e2e_error_recovery.py",
                "test_e2e_comprehensive.py",
            ],
        }

        self.test_markers = {
            "mocked": "not requires_testnet and not load_test and not error_scenario",
            "integration": "not requires_testnet",
            "testnet": "requires_testnet",
            "load": "load_test",
            "error": "error_scenario",
            "smoke": "not load_test and not error_scenario and not requires_testnet",
        }

    def get_pytest_args(
        self,
        category: str = "all",
        markers: list[str] | None = None,
        parallel: bool = True,
        verbose: bool = True,
        capture: str = "no",
        testnet: bool = False,
    ) -> list[str]:
        """Build pytest arguments based on configuration."""
        args = []

        if category in self.test_categories:
            test_files = self.test_categories[category]
            args.extend([f"tests/{file}" for file in test_files])
        else:
            log.warning("Unknown category '%s', running all tests", category)
            args.extend([f"tests/{file}" for file in self.test_categories["all"]])

        if markers:
            marker_expressions = []
            for marker in markers:
                if marker in self.test_markers:
                    marker_expressions.append(self.test_markers[marker])
                else:
                    marker_expressions.append(marker)

            if marker_expressions:
                args.extend(["-m", " and ".join(marker_expressions)])

        if parallel and not testnet:
            import multiprocessing

            worker_count = min(4, multiprocessing.cpu_count())
            args.extend(["-n", str(worker_count)])

        if verbose:
            args.append("-v")

        if capture:
            args.extend(["-s" if capture == "no" else f"--capture={capture}"])

        args.extend(["--tb=short", "--strict-markers"])

        if testnet:
            args.extend(["--timeout=120", "-x"])

        return args

    def run_tests(
        self,
        category: str = "all",
        markers: list[str] | None = None,
        parallel: bool = True,
        verbose: bool = True,
        testnet: bool = False,
        dry_run: bool = False,
    ) -> int:
        """Run E2E tests with specified configuration."""
        log.info("Running E2E tests - Category: %s", category)

        if markers:
            log.info("Markers: %s", ", ".join(markers))

        pytest_args = self.get_pytest_args(
            category=category,
            markers=markers,
            parallel=parallel,
            verbose=verbose,
            testnet=testnet,
        )

        if dry_run:
            log.info("Dry run - would execute:")
            log.info("pytest %s", " ".join(pytest_args))
            return 0

        if testnet:
            log.warning("Running testnet tests - requires valid credentials")
            if not self._check_testnet_credentials():
                log.error("Testnet credentials not found")
                return 1

        log.info("Executing: pytest %s", " ".join(pytest_args))

        start_time = time.time()
        result = pytest.main(pytest_args)
        duration = time.time() - start_time

        log.info("Tests completed in %.2f seconds", duration)

        if result == 0:
            log.info("All tests passed!")
        else:
            log.error("Tests failed with exit code %d", result)

        return result

    def _check_testnet_credentials(self) -> bool:
        """Check if testnet credentials are available."""
        if (os.getenv("BINANCE_TESTNET_FIX_KEY") or os.getenv("BINANCE_TESTNET_API_KEY")) and (
            os.getenv("BINANCE_TESTNET_FIX_PRIVATE_KEY_PATH") or os.getenv("BINANCE_TESTNET_PRIVATE_KEY_PATH")
        ):
            return True

        config_paths = [Path("config.json"), Path("config.ini")]

        for path in config_paths:
            if path.exists():
                try:
                    with path.open() as f:
                        content = f.read()
                        if ("API_KEY" in content or "BINANCE_FIX_KEY" in content) and len(content.strip()) > 50:
                            return True
                except Exception:
                    pass

        return False

    def list_tests(self, category: str = "all", markers: list[str] | None = None):
        """List available tests without running them."""
        pytest_args = self.get_pytest_args(
            category=category,
            markers=markers,
            parallel=False,
            verbose=False,
        )

        pytest_args.extend(["--collect-only", "-q"])

        log.info("Available tests - Category: %s", category)
        if markers:
            log.info("Markers: %s", ", ".join(markers))

        pytest.main(pytest_args)

    def run_smoke_tests(self) -> int:
        log.info("Running smoke tests...")
        return self.run_tests(category="all", markers=["smoke"], parallel=True, verbose=False)

    def run_load_tests(self) -> int:
        log.info("Running load tests...")
        return self.run_tests(markers=["load"], parallel=False, verbose=True)

    def run_error_tests(self) -> int:
        log.info("Running error scenario tests...")
        return self.run_tests(markers=["error"], parallel=True, verbose=True)

    def run_testnet_tests(self) -> int:
        log.info("Running testnet tests...")
        return self.run_tests(markers=["testnet"], parallel=False, verbose=True, testnet=True)

    def dry_run_testnet_tests(self) -> int:
        log.info("Dry-running testnet tests...")
        return self.run_tests(markers=["testnet"], parallel=False, verbose=True, testnet=True, dry_run=True)


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="E2E Test Runner for Binance FIX Connector (Async)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                                    # Run all tests
  %(prog)s --smoke                            # Run smoke tests only
  %(prog)s --category order_lifecycle         # Run order lifecycle tests
  %(prog)s --markers mocked                   # Run mocked E2E tests
  %(prog)s --markers "load and not testnet"   # Run load tests (no testnet)
  %(prog)s --testnet                          # Run testnet tests
  %(prog)s --list                             # List available tests
  %(prog)s --dry-run --category all           # Show what would be run
        """,
    )

    parser.add_argument(
        "--category",
        choices=[
            "framework",
            "order_lifecycle",
            "market_data",
            "drop_copy",
            "multi_session",
            "error_recovery",
            "comprehensive",
            "all",
        ],
        default="all",
        help="Test category to run (default: all)",
    )

    parser.add_argument(
        "--markers", nargs="+", help="Test markers to filter by (mocked, integration, testnet, load, error, smoke)"
    )

    parser.add_argument("--no-parallel", action="store_true", help="Disable parallel test execution")
    parser.add_argument("--quiet", action="store_true", help="Reduce output verbosity")
    parser.add_argument("--testnet", action="store_true", help="Run tests against real testnet (requires credentials)")
    parser.add_argument("--list", action="store_true", help="List available tests without running them")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be executed without running tests")
    parser.add_argument("--smoke", action="store_true", help="Run quick smoke tests")
    parser.add_argument("--load", action="store_true", help="Run performance/load tests")
    parser.add_argument("--error", action="store_true", help="Run error scenario tests")

    return parser


def main():
    logging.basicConfig(format="%(message)s", level=logging.INFO)

    parser = create_parser()
    args = parser.parse_args()

    runner = E2ETestRunner()

    if args.list:
        runner.list_tests(category=args.category, markers=args.markers)
        return 0

    if args.smoke:
        return runner.run_smoke_tests()

    if args.load:
        return runner.run_load_tests()

    if args.error:
        return runner.run_error_tests()

    if args.testnet or (args.markers and "testnet" in args.markers):
        if args.dry_run:
            return runner.dry_run_testnet_tests()
        return runner.run_testnet_tests()

    return runner.run_tests(
        category=args.category,
        markers=args.markers,
        parallel=not args.no_parallel,
        verbose=not args.quiet,
        testnet=args.testnet,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    sys.exit(main())
