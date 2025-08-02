import ccxt.async_support as ccxt
import asyncio
import aiohttp
import json
from typing import Dict, List, Optional, Any
from datetime import datetime
import logging
import time

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
        """Get perpetual symbol format for each exchange (hard-coded)"""
        symbol_mapping = {
            'bitget': f"{base_symbol}/USDT:USDT",      # Bitget uses BTC/USDT:USDT for swap contracts
            'huobi': f"{base_symbol}-USDT",            # Huobi uses BTC-USDT
            'kucoin': f"{base_symbol}USDTM",           # KuCoin uses BTCUSDTM
            'bybit': f"{base_symbol}USDT",             # Bybit uses BTCUSDT
            'bingx': f"{base_symbol}-USDT",            # BingX uses BTC-USDT
            'gateio': f"{base_symbol}/USDT:USDT",      # Gate.io uses BTC/USDT:USDT for swap contracts
            'okx': f"{base_symbol}-USDT-SWAP",         # OKX uses BTC-USDT-SWAP
            'mexc': f"{base_symbol}_USDT",             # MEXC uses BTC_USDT
            'binance': f"{base_symbol}USDT"            # Binance uses BTCUSDT
        }
        return symbol_mapping.get(exchange_name, f"{base_symbol}/USDT")

    async def get_kucoin_funding_rate(self, symbol: str) -> Dict[str, Any]:
        """Get funding rate from KuCoin using direct API call"""
        try:
            base_symbol = symbol.replace('/USDT', '').replace('/USD', '')
            perp_symbol = f"{base_symbol}USDTM"  # KuCoin futures symbol format
            
            url = f"https://api-futures.kucoin.com/api/v1/funding-rate/{perp_symbol}/current"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get('code') == '200000' and data.get('data'):
                            funding_data = data['data']
                            funding_rate = float(funding_data.get('value', 0))
                            
                            return {
                                'exchange': 'kucoin',
                                'symbol': symbol,
                                'perpetual_symbol': perp_symbol,
                                'funding_rate': funding_rate,
                                'funding_time': None, # KuCoin API doesn't provide this in current endpoint
                                'next_funding_time': None,  # KuCoin API doesn't provide this in current endpoint
                                'timestamp': funding_data.get('timePoint'),
                                'success': True,
                                'error': None
                            }
                        else:
                            return {
                                'exchange': 'kucoin',
                                'symbol': symbol,
                                'perpetual_symbol': perp_symbol,
                                'funding_rate': None,
                                'funding_time': None,
                                'next_funding_time': None,
                                'timestamp': None,
                                'success': False,
                                'error': f"KuCoin API error: {data.get('msg', 'Unknown error')}"
                            }
                    else:
                        return {
                            'exchange': 'kucoin',
                            'symbol': symbol,
                            'perpetual_symbol': perp_symbol,
                            'funding_rate': None,
                            'funding_time': None,
                            'next_funding_time': None,
                            'timestamp': None,
                            'success': False,
                            'error': f"HTTP {response.status}: {await response.text()}"
                        }
        except Exception as e:
            return {
                'exchange': 'kucoin',
                'symbol': symbol,
                'perpetual_symbol': f"{symbol.replace('/USDT', '').replace('/USD', '')}USDTM",
                'funding_rate': None,
                'funding_time': None,
                'next_funding_time': None,
                'timestamp': None,
                'success': False,
                'error': str(e)
            }

    async def get_funding_rate_single_exchange(self, exchange_name: str, symbol: str = 'XCN/USDT') -> Dict[str, Any]:
        """Get funding rate from a single exchange"""
        try:
            # Handle KuCoin with direct API call
            if exchange_name == 'kucoin':
                return await self.get_kucoin_funding_rate(symbol)
            
            exchange = self.exchanges[exchange_name]
            
            if not exchange.markets:
                try:
                    await exchange.load_markets()
                except:
                    pass
            
            # Convert to perpetual symbol for the specific exchange
            base_symbol = symbol.replace('/USDT', '').replace('/USD', '')
            perp_symbol = self.get_perpetual_symbol(exchange_name, base_symbol)
            
            # Get funding rate using the hard-coded symbol format
            funding_rate_info = await exchange.fetch_funding_rate(perp_symbol)
            
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
        start = time.time()
        print('start: ',start)
        for exchange_name in self.exchanges.keys():
            task = self.get_funding_rate_single_exchange(exchange_name, symbol)
            tasks.append(task)

        results = await asyncio.gather(*tasks, return_exceptions=True)
        end = time.time()
        print('time is: ', end - start)

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
        """Get funding rates for multiple symbols from all exchanges concurrently"""
        results = {}

        async def fetch_symbol(symbol):
            logger.info(f"Fetching funding rates for {symbol}")
            return symbol, await self.get_funding_rates_all_exchanges(symbol)

        # Create tasks for all symbols
        tasks = [fetch_symbol(symbol) for symbol in symbols]

        # Run all tasks concurrently
        task_results = await asyncio.gather(*tasks, return_exceptions=True)

        # Collect results
        for result in task_results:
            if isinstance(result, Exception):
                logger.error(f"Error fetching symbol: {str(result)}")
            else:
                symbol, data = result
                results[symbol] = data

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

# Token loading function
def load_tokens_from_json(filename: str = 'merged_tokens_20250730_161741.json') -> List[str]:
    """Load tokens from JSON file"""
    try:
        with open(filename, 'r') as f:
            tokens = json.load(f)
        logger.info(f"Loaded {len(tokens)} tokens from {filename}")
        return tokens
    except Exception as e:
        logger.error(f"Error loading tokens from {filename}: {str(e)}")
        return []

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

def get_funding_rates_for_all_tokens(token_file: str = 'merged_tokens_20250730_161741.json', max_tokens: int = None) -> Dict[str, List[Dict[str, Any]]]:
    """Get funding rates for all tokens from JSON file"""
    tokens = load_tokens_from_json(token_file)
    
    if not tokens:
        logger.error("No tokens loaded from file")
        return {}
    
    # Limit the number of tokens if specified
    if max_tokens and len(tokens) > max_tokens:
        tokens = tokens[:max_tokens]
        logger.info(f"Limited to first {max_tokens} tokens")
    
    # Convert tokens to symbol format
    symbols = [f"{token}/USDT" for token in tokens]
    
    logger.info(f"Getting funding rates for {len(symbols)} symbols from all exchanges...")
    return get_multiple_symbols_sync(symbols)

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

def save_all_tokens_funding_rates_to_json(token_file: str = 'merged_tokens_20250730_161741.json', filename: str = None):
    """Save funding rates for all tokens to JSON file"""
    if filename is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"all_funding_rates_{timestamp}.json"
    
    print(f"🚀 Getting funding rates for all tokens from {token_file}...")
    results = get_funding_rates_for_all_tokens(token_file)
    
    # Save results
    with open(filename, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    # Print summary
    total_symbols = len(results)
    successful_symbols = 0
    total_successful_exchanges = 0
    
    print(f"\n📊 Summary:")
    print(f"Total symbols processed: {total_symbols}")
    
    for symbol, exchange_results in results.items():
        successful_exchanges = len([r for r in exchange_results if r['success']])
        total_successful_exchanges += successful_exchanges
        if successful_exchanges > 0:
            successful_symbols += 1
    
    print(f"Symbols with successful funding rates: {successful_symbols}/{total_symbols}")
    print(f"Total successful exchange responses: {total_successful_exchanges}")
    print(f"✅ All funding rates saved to {filename}")
    
    return filename, results

# Main execution
if __name__ == "__main__":
    filename, results = save_all_tokens_funding_rates_to_json('merged_tokens_20250730_161741.json')
    
    count = 0
    for symbol, exchange_results in results.items():
        if count >= 20:
            break
        
        successful = [r for r in exchange_results if r['success']]
        failed = [r for r in exchange_results if not r['success']]
        
        successful_exchanges = [r['exchange'] for r in successful]
        
        print(f"{symbol:<15} {len(successful):<12} {len(failed):<8} {', '.join(successful_exchanges)}")
        count += 1
    
    if len(results) > 20:
        print(f"... and {len(results) - 20} more tokens")
    
    print(f"Complete results saved to: {filename}")
    print("Funding rate collection completed!")