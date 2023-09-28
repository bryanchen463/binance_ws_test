#!/usr/bin/env python3

import hashlib
import hmac
import logging
import os
import time
import base64
import json
from websocket import create_connection
import requests
from requests import HTTPError, RequestException


def setup_log(loglevel):
    logging.basicConfig(level=getattr(logging, loglevel.upper()))


def do_requests(session, method, url, headers=None):
    try:
        response = session.request(method=method, url=url, headers=headers, timeout=10)
        response.raise_for_status()
        response_json = response.json()
    except (RequestException, HTTPError) as conn_err:
        logging.error("Failed to connect: %s", conn_err)
        logging.debug(conn_err, exc_info=True)
        raise RuntimeError(conn_err) from conn_err

    return response_json, response.elapsed.total_seconds()

def signature(params, api_key, secret_key):

    # Timestamp the request
    timestamp = int(time.time() * 1000) # UNIX timestamp in milliseconds
    params['timestamp'] = timestamp

    # Sign the request
    payload = '&'.join([f'{param}={value}' for param, value in sorted(params.items())])
    signature = hmac.new(key=secret_key.encode(), msg=payload.encode(), digestmod=hashlib.sha256).hexdigest()
    params['signature'] = signature

def create_order(session, api_key, secret_key):
    # Send the request
    params = {
        'apiKey':        api_key,
        'symbol':       'BNBUSDT',
        'side':         'BUY',
        'type':         'LIMIT',
        'timeInForce':  'GTC',
        'quantity':     '0.1',
        'price':        '155',
    }
    signature(params, api_key, secret_key)
    req = {
        'id': 'my_new_order',
        'method': 'order.place',
        'params': params
    }
    n = time.time()
    session.send(json.dumps(req))
    result =  session.recv()
    elapsed_seconds = time.time() - n
    response_json = json.loads(result)
    order_id = response_json.get("orderId")
    return order_id, elapsed_seconds


def cancel_order(session, order_id, api_key, secret_key):
    params = {
        "symbol": 'BNBUSDT',
        "apiKey": api_key,
    }
    signature(params, api_key, secret_key)
    req = {
        "id": "5633b6a2-90a9-4192-83e7-925c90b6a2fd",
        "method": "openOrders.cancelAll",
        "params": params,
    }

    n = time.time()
    session.send(json.dumps(req))
    result =  session.recv()
    elapsed_seconds = time.time() - n
    response_json = json.loads(result)
    return response_json, elapsed_seconds


def order_list(session, api_key, secret_key):
    params = {
        "symbol": 'BNBUSDT',
        "apiKey": api_key,
    }
    signature(params, api_key, secret_key)
    req = {
        "id": "55f07876-4f6f-4c47-87dc-43e5fff3f2e7",
        "method": "openOrders.status",
        "params": params,
    }

    n = time.time()
    session.send(json.dumps(req))
    result =  session.recv()
    elapsed_seconds = time.time() - n
    response_json = json.loads(result)
    return response_json, elapsed_seconds


def do_order(url, order_conf, count=1, api_key=None, secret_key=None, proxy_conf=None):
    session = requests.Session()
    if proxy_conf is not None:
        logging.info(f"####### Proxy requests: {proxy_conf}\n")
        session.proxies.update(proxy_conf)
    else:
        logging.info("####### DIRECT requests\n")
    create_total_seconds = []
    cancel_total_seconds = []
    ws = create_ws_connection(api_key, secret_key)
    for round in range(count):
        order_id, create_elapsed_seconds = create_order(session=ws, api_key=api_key, secret_key=secret_key)
        logging.info(f"\tCreate: round: {round}: {create_elapsed_seconds}")
        create_total_seconds.append(create_elapsed_seconds)
        (_, cancel_elapsed_seconds) = cancel_order(ws, order_id, api_key, secret_key)
        cancel_total_seconds.append(cancel_elapsed_seconds)
        logging.info(f"\tCancel: round: {round}: {cancel_elapsed_seconds}")
        (_, order_list_elapsed_seconds) = order_list(ws, api_key, secret_key)
        cancel_total_seconds.append(order_list_elapsed_seconds)
        logging.info(f"\Order list: round: {round}: {order_list_elapsed_seconds}")
        time.sleep(1)

    logging.info("Calculating latency for order Creation")
    calc_latency(create_total_seconds)
    logging.info("Calculating latency for order Cancelation")
    calc_latency(cancel_total_seconds)
    logging.info("\n")


def get_binance_server_time(session):
    url = "https://api1.binance.com/api/v3/time"
    (response, _) = do_requests(session=session, method="get", url=url)
    server_time = response.get("serverTime")
    if server_time is None:
        logging.error("Failed to get server times")
        raise RuntimeError()

    return server_time


def calc_latency(req_latencies):
    req_average = sum(req_latencies) / len(req_latencies)
    req_mode = max(set(req_latencies), key=req_latencies.count)

    logging.info(
        f"""
        \tExecutions: {req_latencies}
        \tAverage: {req_average}
        \tMode: {req_mode}
    """
    )

def create_ws_connection(api_key, secret_key):
    ws = create_connection("wss://ws-api.binance.com:443/ws-api/v3")
    return ws

def main():
    setup_log(loglevel="INFO")
    api_key = os.environ.get("BINANCE_API_KEY")
    secret_key = os.environ.get("BINANCE_SECRET_KEY")
    url = "https://api1.binance.com/api/v3/order"
    proxy_conf = {"https": "http://XXXX", "http": "http://XXXX"}
    order_conf = {
        "symbol": "ETHUSDT",  # Trading pair symbol (e.g., ETHUSDT)
        "side": "BUY",  # Order side (BUY or SELL)
        "type": "LIMIT",  # Order type (LIMIT, MARKET, etc.)
        "time_in_force": "GTC",  # Time in force (GTC, IOC, etc.)
        "quantity": 0.0121,  # Quantity of the asset to buy/sell
        "price": 1625.0,  # Price at which to buy/sell (only for LIMIT orders)
        "secret_key": secret_key,
        "headers": {"X-MBX-APIKEY": api_key},
    }
    do_order(url=url, order_conf=order_conf, count=10, api_key=api_key, secret_key=secret_key)


if __name__ == "__main__":
    main()