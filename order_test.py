import json
import rel
import logging
from binance.lib.utils import config_logging
import time
import socket
from urllib.parse import urlencode, urlparse
import base64
from typing import Callable, Optional, Union
import hmac
import hashlib
from ccxt.pro import binance
import asyncio

from binance.um_futures import UMFutures
from binance.um_futures.data_stream import *
from binance.websocket.um_futures.websocket_client import UMFuturesWebsocketClient as client
from binance.websocket.websocket_client import BinanceWebsocketClient
from websocket import WebSocketApp, ABNF

api_key = ""
secret_key = ""

id_map = {
}
config_logging(logging, logging.INFO)

symbol = "BNBUSDT"    

def test_order():
    cli = UMFutures(key=api_key, secret=secret_key)
    resp = cli.new_order(
        symbol="BTCUSDT",
        side="SELL",
        type="LIMIT",
        quantity=0.001,
        timeInForce="FOK",
        price=59808.02,
    )
    cli.cancel_order(symbol=symbol, orderId=resp['data']['orderId'])

async def main():
    b = binance({
        "apiKey": api_key,
        "secret": secret_key,
    })
    resetb = binance({
        "apiKey": api_key,
        "secret": secret_key,
    })
    
    ws_cost = 0
    rest_cost = 0
    for i in range(0, 100):
        old1 = time.time()
        resp = await b.create_order_ws(
            symbol=symbol,
            side="buy",
            type="limit",
            amount=0.1,
            price=155,
        )
        print(resp)
        old2 = time.time()
        ws_cost += old2 - old1
        resp = await resetb.create_order(
            symbol=symbol,
            side="buy",
            type="limit",
            amount=0.1,
            price=155,
        )
        print(resp)
        now = time.time()
        rest_cost += now - old2
        print("ws order pass", old2 - old1)
        print("rest order pass", now - old2)
        await resetb.cancel_all_orders(symbol=symbol)
        time.sleep(1)
    await b.close()
    await resetb.close()
        
    print(f"100 times, ws avg cost {ws_cost/100}s, reset avg cost {rest_cost/100}s")
    
if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())