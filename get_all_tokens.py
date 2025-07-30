#!/usr/bin/env python3

import ccxt
import json
from typing import List, Set
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
    
    def get_tokens_from_exchange(self, exchange_name: str) -> Set[str]:
        """Get all base tokens from a single exchange"""
        try:
            exchange = self.exchanges[exchange_name]
            
            # Load markets
            exchange.load_markets()
            
            tokens = set()
            
            for symbol, market in exchange.markets.items():
                # Check if it's a perpetual/swap contract
                if market.get('type') == 'swap' or market.get('contract') == True:
                    # Extract base symbol (e.g., BTC from BTC/USDT:USDT)
                    base_symbol = market.get('base', '')
                    if base_symbol and base_symbol not in ['USDT', 'USD', 'BUSD', 'USDC']:
                        tokens.add(base_symbol)
            
            logger.info(f"Found {len(tokens)} unique tokens on {exchange_name}")
            return tokens
            
        except Exception as e:
            logger.error(f"Error getting tokens from {exchange_name}: {str(e)}")
            return set()
    
    def get_all_merged_tokens(self) -> List[str]:
        """Get all unique tokens merged from all exchanges"""
        all_tokens = set()
        
        for exchange_name in self.exchanges.keys():
            logger.info(f"Getting tokens from {exchange_name}...")
            exchange_tokens = self.get_tokens_from_exchange(exchange_name)
            all_tokens.update(exchange_tokens)
        
        # Convert to sorted list
        merged_tokens = sorted(list(all_tokens))
        return merged_tokens
    
    def save_to_json(self, tokens: List[str], filename: str = None):
        """Save tokens list to JSON file"""
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'merged_tokens_{timestamp}.json'
        
        with open(filename, 'w') as f:
            json.dump(tokens, f, indent=2)
        
        logger.info(f"Merged tokens saved to {filename}")
        return filename

def main():
    """Main function to collect and export merged tokens"""
    collector = TokenCollector()
        
    # Get all merged tokens
    merged_tokens = collector.get_all_merged_tokens()
    
    # Save to JSON
    filename = collector.save_to_json(merged_tokens)
    
    print(f" Merged tokens exported to: {filename}")
    
    for i, token in enumerate(merged_tokens[:50], 1):
        print(f"  {i:2d}. {token}")
    
    if len(merged_tokens) > 50:
        print(f"  ... and {len(merged_tokens) - 50} more tokens")
    
    return merged_tokens

if __name__ == "__main__":
    main()