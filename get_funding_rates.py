import ccxt
import asyncio
import aiohttp
import json
from typing import Dict, List, Optional, Any
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FundingRateCollector:
    """Collect funding rates from multiple cryptocurrency exchanges"""
    
    def __init__(self):
        self.exchanges = {
            'bitget': ccxt.bitget({'enableRateLimit': True}),
            'huobi': ccxt.huobi({'enableRateLimit': True}),
            'kucoin': ccxt.kucoin({'enableRateLimit': True}),
            'bybit': ccxt.bybit({'enableRateLimit': True}),
            'bingx': ccxt.bingx({'enableRateLimit': True}),
            'gateio': ccxt.gateio({'enableRateLimit': True}),
            'okx': ccxt.okx({'enableRateLimit': True}),
            'mexc': ccxt.mexc({'enableRateLimit': True})
        }
        
        # Configure exchanges for sandbox/testnet if needed
        for exchange_name, exchange in self.exchanges.items():
            exchange.set_sandbox_mode(False)  # Set to True for testnet
    
    def get_perpetual_symbol(self, exchange_name: str, base_symbol: str) -> str:
        """Convert spot symbol to perpetual symbol for each exchange"""
        symbol_mapping = {
            'bitget': f"{base_symbol}USDT",   # Bitget uses BTCUSDT for perpetuals
            'huobi': f"{base_symbol}-USDT",   # Huobi uses BTC-USDT for perpetuals
            'kucoin': f"{base_symbol}USDTM",  # KuCoin uses BTCUSDTM for perpetuals
            'bybit': f"{base_symbol}USDT",    # Bybit uses BTCUSDT for perpetuals
            'bingx': f"{base_symbol}-USDT",   # BingX uses BTC-USDT for perpetuals
            'gateio': f"{base_symbol}_USDT",  # Gate.io uses BTC_USDT for perpetuals
            'okx': f"{base_symbol}-USDT-SWAP", # OKX uses BTC-USDT-SWAP for perpetuals
            'mexc': f"{base_symbol}_USDT"     # MEXC uses BTC_USDT for perpetuals
        }
        return symbol_mapping.get(exchange_name, base_symbol)

    async def get_funding_rate_single_exchange(self, exchange_name: str, symbol: str = 'XCN/USDT') -> Dict[str, Any]:
        """Get funding rate from a single exchange"""
        try:
            exchange = self.exchanges[exchange_name]
            
            # Load markets if not already loaded
            if not exchange.markets:
                # Try async first, then sync
                try:
                    if hasattr(exchange, 'load_markets') and asyncio.iscoroutinefunction(exchange.load_markets):
                        await exchange.load_markets()
                    else:
                        exchange.load_markets()
                except:
                    exchange.load_markets()
            
            # Convert to perpetual symbol for the specific exchange
            base_symbol = symbol.replace('/USDT', '').replace('/USD', '')
            perp_symbol = self.get_perpetual_symbol(exchange_name, base_symbol)
            
            # Get funding rate - try sync method since CCXT funding rates are typically sync
            try:
                funding_rate_info = exchange.fetch_funding_rate(perp_symbol)
            except Exception as sync_error:
                # If the perpetual symbol fails, try the original symbol
                try:
                    funding_rate_info = exchange.fetch_funding_rate(symbol)
                except Exception as original_error:
                    # Try some alternative symbol formats
                    alternative_symbols = [
                        f"{base_symbol}/USDT:USDT",  # Some exchanges use this format
                        f"{base_symbol}/USD:USD",
                        f"{base_symbol}USDT",
                        f"{base_symbol}/USDT"
                    ]
                    
                    for alt_symbol in alternative_symbols:
                        try:
                            funding_rate_info = exchange.fetch_funding_rate(alt_symbol)
                            perp_symbol = alt_symbol  # Update the symbol that worked
                            break
                        except:
                            continue
                    else:
                        # If all attempts failed, raise the original error
                        raise sync_error
            
            return {
                'exchange': exchange_name,
                'symbol': symbol,
                'perpetual_symbol': perp_symbol,
                'funding_rate': funding_rate_info.get('fundingRate'),
                'funding_time': funding_rate_info.get('fundingDatetime'),
                'next_funding_time': funding_rate_info.get('nextFundingDatetime'),
                'timestamp': funding_rate_info.get('timestamp'),
                'success': True,
                'error': None
            }
            
        except Exception as e:
            logger.error(f"Error getting funding rate from {exchange_name}: {str(e)}")
            base_symbol = symbol.replace('/USDT', '').replace('/USD', '')
            perp_symbol = self.get_perpetual_symbol(exchange_name, base_symbol)
            return {
                'exchange': exchange_name,
                'symbol': symbol,
                'perpetual_symbol': perp_symbol,
                'funding_rate': None,
                'funding_time': None,
                'next_funding_time': None,
                'timestamp': None,
                'success': False,
                'error': str(e)
            }
    
    async def get_funding_rates_all_exchanges(self, symbol: str = 'BTC/USDT') -> List[Dict[str, Any]]:
        """Get funding rates from all configured exchanges"""
        tasks = []
        
        for exchange_name in self.exchanges.keys():
            task = self.get_funding_rate_single_exchange(exchange_name, symbol)
            tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Handle any exceptions that occurred
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                exchange_name = list(self.exchanges.keys())[i]
                base_symbol = symbol.replace('/USDT', '').replace('/USD', '')
                perp_symbol = self.get_perpetual_symbol(exchange_name, base_symbol)
                processed_results.append({
                    'exchange': exchange_name,
                    'symbol': symbol,
                    'perpetual_symbol': perp_symbol,
                    'funding_rate': None,
                    'funding_time': None,
                    'next_funding_time': None,
                    'timestamp': None,
                    'success': False,
                    'error': str(result)
                })
            else:
                processed_results.append(result)
        
        return processed_results
    
    async def get_multiple_symbols_funding_rates(self, symbols: List[str]) -> Dict[str, List[Dict[str, Any]]]:
        """Get funding rates for multiple symbols from all exchanges"""
        results = {}
        
        for symbol in symbols:
            logger.info(f"Fetching funding rates for {symbol}")
            results[symbol] = await self.get_funding_rates_all_exchanges(symbol)
        
        return results
    
    def get_successful_rates(self, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Filter only successful funding rate results"""
        return [result for result in results if result['success']]
    
    def get_failed_exchanges(self, results: List[Dict[str, Any]]) -> List[str]:
        """Get list of exchanges that failed to return funding rates"""
        return [result['exchange'] for result in results if not result['success']]
    
    async def close_connections(self):
        """Close all exchange connections"""
        for exchange in self.exchanges.values():
            if hasattr(exchange, 'close'):
                await exchange.close()

# Synchronous wrapper functions for easier use
def get_funding_rates_sync(symbol: str = 'BTC/USDT') -> List[Dict[str, Any]]:
    """Synchronous wrapper to get funding rates from all exchanges"""
    collector = FundingRateCollector()
    
    async def _get_rates():
        try:
            results = await collector.get_funding_rates_all_exchanges(symbol)
            await collector.close_connections()
            return results
        except Exception as e:
            logger.error(f"Error in sync wrapper: {str(e)}")
            await collector.close_connections()
            raise
    
    return asyncio.run(_get_rates())

def get_multiple_symbols_sync(symbols: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    """Synchronous wrapper to get funding rates for multiple symbols"""
    collector = FundingRateCollector()
    
    async def _get_multiple_rates():
        try:
            results = await collector.get_multiple_symbols_funding_rates(symbols)
            await collector.close_connections()
            return results
        except Exception as e:
            logger.error(f"Error in sync wrapper: {str(e)}")
            await collector.close_connections()
            raise
    
    return asyncio.run(_get_multiple_rates())

# Example usage functions
def print_funding_rates(symbol: str = 'BTC/USDT'):
    """Print funding rates in a formatted way"""
    print(f"\n=== Funding Rates for {symbol} ===")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 80)
    
    results = get_funding_rates_sync(symbol)
    
    successful_results = [r for r in results if r['success']]
    failed_results = [r for r in results if not r['success']]
    
    # Print successful results
    if successful_results:
        print(f"{'Exchange':<12} {'Funding Rate':<15} {'Next Funding Time':<20}")
        print("-" * 80)
        
        for result in successful_results:
            funding_rate = result['funding_rate']
            if funding_rate is not None:
                funding_rate_pct = f"{funding_rate * 100:.4f}%" if funding_rate else "N/A"
            else:
                funding_rate_pct = "N/A"
            
            next_funding = result['next_funding_time']
            if next_funding:
                next_funding_str = datetime.fromisoformat(next_funding.replace('Z', '+00:00')).strftime('%H:%M:%S')
            else:
                next_funding_str = "N/A"
            
            print(f"{result['exchange']:<12} {funding_rate_pct:<15} {next_funding_str:<20}")
    
    # Print failed exchanges
    if failed_results:
        print(f"\nFailed to get data from: {', '.join([r['exchange'] for r in failed_results])}")
        for result in failed_results:
            print(f"  {result['exchange']}: {result['error']}")

def save_funding_rates_to_json(symbol: str = 'BTC/USDT', filename: str = None):
    """Save funding rates to JSON file"""
    if filename is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"funding_rates_{symbol.replace('/', '_')}_{timestamp}.json"
    
    results = get_funding_rates_sync(symbol)
    
    with open(filename, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"Funding rates saved to {filename}")
    return filename

# Main execution
if __name__ == "__main__":
    # Example 1: Get funding rates for BTC/USDT
    print_funding_rates('BTC/USDT')
    
    # Example 2: Get funding rates for multiple symbols
    symbols = ['BTC/USDT', 'ETH/USDT', 'SOL/USDT']
    multiple_results = get_multiple_symbols_sync(symbols)
    
    print(f"\n=== Multiple Symbols Results ===")
    for symbol, results in multiple_results.items():
        successful_count = len([r for r in results if r['success']])
        print(f"{symbol}: {successful_count}/{len(results)} exchanges successful")
    
    # Example 3: Save to JSON
    save_funding_rates_to_json('BTC/USDT')