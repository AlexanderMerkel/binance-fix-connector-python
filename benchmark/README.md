# Benchmark Suite

Performance and compatibility analysis for the async FIX connector.

`comprehensive_analysis.py` benchmarks message creation speed, memory efficiency, operation latency, data consistency, and API feature parity against the sync library.

## Usage

```bash
# Interactive (recommended)
./run_analysis.sh

# With environment variables
export BINANCE_TESTNET_FIX_KEY="your_api_key"
export BINANCE_TESTNET_FIX_PRIVATE_KEY_PATH="/path/to/private_key.pem"
python comprehensive_analysis.py

# Mock credentials (development)
python comprehensive_analysis.py
```

## Output

- `analysis_results.md` — generated report with performance tables and migration recommendations
- `performance_dashboard.png` — 4-panel performance charts
- `validation_results.png` — pass/fail summary
