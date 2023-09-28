import json
import rel
import time
import socket
import signal
import threading
from typing import Callable, Optional, Union
from functools import partial

from binance.um_futures import UMFutures
from binance.um_futures.data_stream import *
from binance.websocket.um_futures.websocket_client import UMFuturesWebsocketClient as client
from binance.websocket.websocket_client import BinanceWebsocketClient
from websocket import WebSocketApp, ABNF

api_key = ""
secret_key = ""

id_map = {
}

class UserDataStream(BinanceWebsocketClient):
    def __init__(self, stream_url, on_message=None, on_open=None, on_close=None, on_error=None, on_ping=None, on_pong=None, logger=None, proxies: dict | None = None):
        super().__init__(stream_url, self.on_user_date_message, on_open, on_close, on_error, on_ping, on_pong, logger, proxies)
        self.listen_key = None

    def on_user_date_message(self, ws, message):
        msg = json.loads(message)
        id = msg["id"]
        if id in id_map:
            fn = id_map[id]
            fn(ws, message)
        else:
            self.start_user_stream(ws)
            
    def start_user_stream(self, ws):
        param = {
            "id": "d3df8a61-98ea-4fe0-8f4e-0fcea5d418b0",
            "method": "userDataStream.start",
            "params": {
                "apiKey": "vmPUZE6mv9SD5VNHk4HlWFsOr6aKE2zvsw0MuIgwCIPy6utIco14y7Ju91duEh8A"
            }
        }
        id_map[param['id']] = self.on_user_stream
        ws.send(param)
        
    def send_user_stream_ping(self, ws):
        param = {
            "id": "815d5fce-0880-4287-a567-80badf004c74",
            "method": "userDataStream.ping",
            "params": {
                "listenKey": "xs0mRXdAKlIPDRFrlPcw0qI41Eh3ixNntmymGyhrhgqo7L6FuLaWArTD7RLP",
                "apiKey": "vmPUZE6mv9SD5VNHk4HlWFsOr6aKE2zvsw0MuIgwCIPy6utIco14y7Ju91duEh8A"
            }
        }
        id_map[param['id']] = self.on_user_data_ping
        ws.send(param)

    def on_user_data_ping(self, _, message):
        print(message)

    def on_user_stream(self, _, message):
        '''
        response
        {
            "id": "d3df8a61-98ea-4fe0-8f4e-0fcea5d418b0",
            "status": 200,
            "result": {
                "listenKey": "xs0mRXdAKlIPDRFrlPcw0qI41Eh3ixNntmymGyhrhgqo7L6FuLaWArTD7RLP"
            },
            "rateLimits": [
                {
                "rateLimitType": "REQUEST_WEIGHT",
                "interval": "MINUTE",
                "intervalNum": 1,
                "limit": 6000,
                "count": 2
                }
            ]
        }
        '''
        msg = json.loads(message)
        listen_key = msg['result']['listenKey']
        self.listen_key = listen_key
        return listen_key
    
class QuoteTime(object):
    def __init__(self, update_id, exchange_time):
        self.recv_time = time.time() * 1000
        self.update_id = update_id
        self.exchange_time = exchange_time
        
    def __str__(self):
        return f'{self.update_id}:{self.recv_time}:{self.exchange_time}\t'

depth_map = {}

last_recv_time = {}
count = 0
total = 0
def on_quote_message(tag, d, message):
    global last_recv_time, count, total
    msg = json.loads(message)
    now = time.time() * 1000
    interval = now - (last_recv_time[tag] if tag in last_recv_time else 0)
    # print(f"{tag} interval", interval)
    last_recv_time[tag] = now
    # print(f"{tag}", now, msg['data'])
    if 'depth' in msg['stream']:
        depth_time = on_depth(msg)
        if depth_time.update_id in depth_map:
            # print("found", depth_time.update_id, depth_map[depth_time.update_id], depth_time)
            count += 1
            if tag == 'auth':
                total += depth_time.recv_time - depth_map[depth_time.update_id].recv_time
            else:
                total += depth_map[depth_time.update_id].recv_time - depth_time.recv_time 
        else:
            depth_map[depth_time.update_id] = depth_time
    elif 'bookTicker' in msg['stream']:
        bookticker_time = on_bookticker(msg)
        if bookticker_time.update_id in depth_map:
            # print("found", bookticker_time.update_id, depth_map[bookticker_time.update_id], bookticker_time)
            count += 1
            if tag == 'auth':
                total += bookticker_time.recv_time - depth_map[bookticker_time.update_id].recv_time
            else:
                total += depth_map[bookticker_time.update_id].recv_time - bookticker_time.recv_time 
        else:
            depth_map[bookticker_time.update_id] = bookticker_time
           
def on_bookticker(msg):
    data = msg
    if 'data' in msg:
        data = msg['data']
    return QuoteTime(data['u'], data['T'])
    
def on_depth(msg):
    data = msg
    if 'data' in msg:
        data = msg['data']
    return QuoteTime(data['u'], data['E'])

def create_user_stream_websocket():
    ws_client = UserDataStream(stream_url="wss://stream.binance.com:443/ws")
    return ws_client
    
class WrapperWebSocketClient(WebSocketApp, threading.Thread):
    def __init__(self, url: str, name=None, listen_key = None, header: Union[list, dict, Callable] = None,
                 on_open: Callable = None, on_message: Callable = None, on_error: Callable = None,
                 on_close: Callable = None, on_ping: Callable = None, on_pong: Callable = None,
                 on_cont_message: Callable = None,
                 keep_running: bool = True, get_mask_key: Callable = None, cookie: str = None,
                 subprotocols: list = None,
                 on_data: Callable = None,
                 socket: socket.socket = None) -> None:
        super().__init__(url, header, on_open, on_message, on_error, 
                         on_close, on_ping, on_pong, on_cont_message, 
                         keep_running, get_mask_key, cookie, subprotocols, 
                         on_data, socket)
        threading.Thread.__init__(self)
        self.listen_key = listen_key
        self.name = name
        
    def subscribe(self, params=None):
        self.send(json.dumps(params))
        
    def on_ping(self, _, message):
        self.send("", ABNF.OPCODE_PONG)
        
    def run(self):
        self.run_forever(reconnect=5, ping_interval=30)
        rel.dispatch()

symbol = "btcusdt"    

def create_firse_quote_ws():
    ws_client = WrapperWebSocketClient(url="wss://fstream.binance.com", on_message=on_quote_message)
    ws_client.run_forever(dispatcher=rel, reconnect=5, ping_interval=30)
    rel.signal(2, rel.abort)
    rel.dispatch()
    
def combina_url(listen_key, symbol, quote):
    url = "wss://fstream-auth.binance.com/stream?streams="
    if listen_key is None:
        url = "wss://fstream.binance.com/stream?streams="
        
    url = f'{url}{symbol}@{quote}'
    if listen_key is not None:
        url = f'{url}&listenKey={listen_key}'
    return url
    
def create_quote_wss(listen_key):
    quote_fn = partial(on_quote_message, "auth")
    url = combina_url(listen_key, symbol=symbol, quote="bookTicker")
    ws_client1 = WrapperWebSocketClient(url=url, listen_key=listen_key, on_message=quote_fn)
    ws_client1.start()
    time.sleep(10)
    quote_fn = partial(on_quote_message, "unauth")
    url = combina_url(listen_key=None, symbol=symbol, quote="bookTicker")
    ws_client = WrapperWebSocketClient(url=url,on_message=quote_fn)
    ws_client.start()
    return (ws_client1, ws_client)
    
def main():
    # user_date_stream = create_user_stream_websocket()
    cli = UMFutures(key=api_key, secret=secret_key)
    # while True:
    #     if user_date_stream.listen_key is not None:
    #         break
    resp = cli.new_listen_key()
        
    listen_key = resp['listenKey']
    auth_ws = create_quote_wss(listen_key)
    sig_fn = partial(tear_down, auth_ws[0], auth_ws[1])
    signal.signal(signal.SIGINT, sig_fn)
    

def tear_down(ws1, ws2, signalnum, frame):
    print(f"total delay {total}, count {count}, avg {total/count}ms")
    ws1.stop()
    ws2.stop()
    
if __name__ == '__main__':
    main()
