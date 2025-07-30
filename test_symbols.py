#!/usr/bin/env python3

import ccxt
import asyncio

def test_exchange_symbols():
    """Test different symbol formats for each exchange"""
    
    exchanges = {
        'bitget': ccxt.bitget({'enableRateLimit': True}),
        'huobi': ccxt.huobi({'enableRateLimit': True}),
        'kucoin': ccxt.kucoin({'enableRateLimit': True}),
        'bybit': ccxt.bybit({'enableRateLimit': True}),
        'bingx': ccxt.bingx({'enableRateLimit': True}),
        'gateio': ccxt.gateio({'enableRateLimit': True}),
        'okx': ccxt.okx({'enableRateLimit': True}),
        'mexc': ccxt.mexc({'enableRateLimit': True})
    }
    
    # Different symbol formats to try
    symbol_formats = [
        'BTC/USDT:USDT',
        'BTCUSDT',
        'BTC-USDT',
        'BTC_USDT',
        'BTC-USDT-SWAP',
        'BTCUSDTM',
        'BTC/USDT',
        'BTC:USDT'
    ]
    
    for exchange_name, exchange in exchanges.items():
        print(f"\n=== Testing {exchange_name.upper()} ===")
        
        try:
            # Load markets first
            exchange.load_markets()
            
            # Check if exchange supports funding rates
            if not hasattr(exchange, 'fetch_funding_rate'):
                print(f"❌ {exchange_name} does not support fetch_funding_rate")
                continue
                
            print(f"✅ {exchange_name} supports fetch_funding_rate")
            
            # Test different symbol formats
            for symbol in symbol_formats:
                try:
                    if symbol in exchange.markets:
                        funding_rate = exchange.fetch_funding_rate(symbol)
                        print(f"✅ {symbol}: {funding_rate.get('fundingRate', 'N/A')}")
                        break  # Found working symbol, move to next exchange
                    else:
                        print(f"❌ {symbol}: Not in markets")
                except Exception as e:
                    print(f"❌ {symbol}: {str(e)}")
                    
        except Exception as e:
            print(f"❌ Failed to load markets for {exchange_name}: {str(e)}")

if __name__ == "__main__":
    test_exchange_symbols()