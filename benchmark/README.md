# Benchmark Suite

Local research and compatibility analysis for the async FIX connector.

`comprehensive_analysis.py` benchmarks message creation speed, peak memory usage, mean operation latency, covered consistency checks, and supported API-surface parity against the sync library. Performance rows use one warmup run plus seven measured repeats and report median/min-max values.

Use these results for quick Python testing, sync-vs-async comparisons, and regression checks on the host where the benchmark runs. Do not treat the generated values as universal Binance latency claims.

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

- `analysis_results.md` — generated report with performance tables, median/min-max ranges, and migration recommendations
