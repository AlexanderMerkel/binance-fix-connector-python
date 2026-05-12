#!/bin/bash

# Binance FIX Connector Analysis Runner
# This script loads testnet credentials and runs the comprehensive analysis

set -e

echo "🔧 Binance FIX Connector - Analysis Setup"
echo "========================================="

# Prompt for credentials
echo "📝 Enter your Binance Testnet FIX API Key:"
read -r BINANCE_TESTNET_FIX_KEY

echo "📝 Enter path to your Ed25519 private key PEM file:"
read -r BINANCE_TESTNET_FIX_PRIVATE_KEY_PATH

# Set hardcoded testnet configuration
export BINANCE_TESTNET_FIX_KEY
export BINANCE_TESTNET_FIX_PRIVATE_KEY_PATH
export BINANCE_TESTNET_ENDPOINT="tcp+tls://fix-oe.testnet.binance.vision:9000"
export BINANCE_TESTNET_SENDER_COMP_ID="TESTCLI"
export BINANCE_TESTNET_TARGET_COMP_ID="SPOT"

# Validate credentials
if [ -z "$BINANCE_TESTNET_FIX_KEY" ]; then
    echo "❌ API Key is required"
    exit 1
fi

if [ -z "$BINANCE_TESTNET_FIX_PRIVATE_KEY_PATH" ]; then
    echo "❌ Private key path is required"
    exit 1
fi

if [ ! -f "$BINANCE_TESTNET_FIX_PRIVATE_KEY_PATH" ]; then
    echo "❌ Private key file not found: $BINANCE_TESTNET_FIX_PRIVATE_KEY_PATH"
    exit 1
fi

# Display configuration
echo ""
echo "🔐 Configuration:"
echo "  API Key: ${BINANCE_TESTNET_FIX_KEY:0:8}..."
echo "  Private Key: $BINANCE_TESTNET_FIX_PRIVATE_KEY_PATH"
echo "  Endpoint: $BINANCE_TESTNET_ENDPOINT"
echo ""

# Change to benchmark directory
cd "$(dirname "$0")"

# Run the analysis
echo "🚀 Starting comprehensive analysis with real testnet credentials..."
python comprehensive_analysis.py

echo ""
echo "✅ Analysis complete! Check analysis_results.md for detailed results."
