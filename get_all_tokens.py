#!/usr/bin/env python3

import ccxt
import json
import asyncio
from typing import Dict, List, Set
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TokenCollector:
    """Collect all available tokens from multiple cryptocurrency exchanges"""
    
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
        
        # Configure exchanges
        for exchange_name, exchange in self.exchanges.items():
            exchange.set_sandbox_mode(False)
    
    def get_perpetual_symbols_from_exchange(self, exchange_name: str) -> List[str]:
        """Get all perpetual/swap symbols from a single exchange"""
        try:
            exchange = self.exchanges[exchange_name]
            
            # Load markets
            exchange.load_markets()
            
            perpetual_symbols = []
            
            for symbol, market in exchange.markets.items():
                # Check if it's a perpetual/swap contract
                if market.get('type') == 'swap' or market.get('contract') == True:
                    # Extract base symbol (e.g., BTC from BTC/USDT:USDT)
                    base_symbol = market.get('base', '')
                    if base_symbol and base_symbol not in ['USDT', 'USD', 'BUSD']:
                        perpetual_symbols.append({
                            'symbol': symbol,
                            'base': base_symbol,
                            'quote': market.get('quote', ''),
                            'exchange': exchange_name,
                            'type': market.get('type', ''),
                            'active': market.get('active', False)
                        })
            
            logger.info(f"Found {len(perpetual_symbols)} perpetual symbols on {exchange_name}")
            return perpetual_symbols
            
        except Exception as e:
            logger.error(f"Error getting symbols from {exchange_name}: {str(e)}")
            return []
    
    async def get_all_tokens_async(self) -> Dict[str, List[Dict]]:
        """Get all tokens from all exchanges asynchronously"""
        results = {}
        
        # Create tasks for each exchange
        tasks = []
        for exchange_name in self.exchanges.keys():
            task = asyncio.create_task(
                asyncio.to_thread(self.get_perpetual_symbols_from_exchange, exchange_name)
            )
            tasks.append((exchange_name, task))
        
        # Wait for all tasks to complete
        for exchange_name, task in tasks:
            try:
                symbols = await task
                results[exchange_name] = symbols
            except Exception as e:
                logger.error(f"Failed to get symbols from {exchange_name}: {str(e)}")
                results[exchange_name] = []
        
        return results
    
    def get_all_tokens_sync(self) -> Dict[str, List[Dict]]:
        """Get all tokens from all exchanges synchronously"""
        results = {}
        
        for exchange_name in self.exchanges.keys():
            logger.info(f"Getting tokens from {exchange_name}...")
            results[exchange_name] = self.get_perpetual_symbols_from_exchange(exchange_name)
        
        return results
    
    def merge_tokens(self, exchange_tokens: Dict[str, List[Dict]]) -> List[Dict]:
        """Merge tokens from all exchanges and remove duplicates"""
        merged_tokens = []
        seen_bases = set()
        
        # Collect all unique base symbols
        for exchange_name, tokens in exchange_tokens.items():
            for token in tokens:
                base = token['base']
                if base not in seen_bases:
                    seen_bases.add(base)
                    
                    # Find all exchanges that have this token
                    exchanges_with_token = []
                    for ex_name, ex_tokens in exchange_tokens.items():
                        for ex_token in ex_tokens:
                            if ex_token['base'] == base:
                                exchanges_with_token.append({
                                    'exchange': ex_name,
                                    'symbol': ex_token['symbol'],
                                    'active': ex_token['active']
                                })
                    
                    merged_tokens.append({
                        'base_symbol': base,
                        'exchanges': exchanges_with_token,
                        'exchange_count': len(exchanges_with_token)
                    })
        
        # Sort by exchange count (most popular tokens first)
        merged_tokens.sort(key=lambda x: x['exchange_count'], reverse=True)
        
        return merged_tokens
    
    def get_base_symbols_only(self, exchange_tokens: Dict[str, List[Dict]]) -> List[str]:
        """Get only unique base symbols (like BTC, ETH, etc.)"""
        base_symbols = set()
        
        for exchange_name, tokens in exchange_tokens.items():
            for token in tokens:
                base_symbols.add(token['base'])
        
        return sorted(list(base_symbols))
    
    def save_to_json(self, data, filename: str):
        """Save data to JSON file"""
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        logger.info(f"Data saved to {filename}")
    
    async def close_connections(self):
        """Close all exchange connections"""
        for exchange in self.exchanges.values():
            if hasattr(exchange, 'close'):
                await exchange.close()

def main():
    """Main function to collect and export all tokens"""
    collector = TokenCollector()
    
    print("🚀 Starting token collection from all exchanges...")
    
    # Get all tokens from all exchanges
    exchange_tokens = collector.get_all_tokens_sync()
    
    # Print summary
    print("\n📊 Summary:")
    total_tokens = 0
    for exchange_name, tokens in exchange_tokens.items():
        count = len(tokens)
        total_tokens += count
        print(f"  {exchange_name}: {count} tokens")
    print(f"  Total: {total_tokens} tokens")
    
    # Merge tokens
    merged_tokens = collector.merge_tokens(exchange_tokens)
    print(f"  Unique base symbols: {len(merged_tokens)}")
    
    # Get base symbols only
    base_symbols = collector.get_base_symbols_only(exchange_tokens)
    
    # Generate timestamp for filenames
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Save all data
    collector.save_to_json(exchange_tokens, f'exchange_tokens_{timestamp}.json')
    collector.save_to_json(merged_tokens, f'merged_tokens_{timestamp}.json')
    collector.save_to_json(base_symbols, f'base_symbols_{timestamp}.json')
    
    print(f"\n✅ Files saved:")
    print(f"  📄 exchange_tokens_{timestamp}.json - All tokens by exchange")
    print(f"  📄 merged_tokens_{timestamp}.json - Merged tokens with exchange info")
    print(f"  📄 base_symbols_{timestamp}.json - Simple list of base symbols")
    
    # Show top 20 most popular tokens
    print(f"\n🔥 Top 20 most popular tokens (by exchange count):")
    for i, token in enumerate(merged_tokens[:20], 1):
        print(f"  {i:2d}. {token['base_symbol']:<8} ({token['exchange_count']}/8 exchanges)")
    
    return exchange_tokens, merged_tokens, base_symbols

if __name__ == "__main__":
    main()