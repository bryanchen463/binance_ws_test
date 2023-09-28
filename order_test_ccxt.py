#!/usr/bin/env python3

import os
import logging

import ccxt
import time


def setup_log(loglevel):
    logging.basicConfig(level=getattr(logging, loglevel.upper()))


def dict_replace_value(d: dict, old: str, new: str) -> dict:
    x = {}
    for k, v in d.items():
        if isinstance(v, dict):
            v = dict_replace_value(v, old, new)
        elif isinstance(v, list):
            v = list_replace_value(v, old, new)
        elif isinstance(v, str):
            v = v.replace(old, new)
        x[k] = v
    return x


def list_replace_value(array: list, old: str, new: str) -> list:
    x = []
    for e in array:
        if isinstance(e, list):
            e = list_replace_value(e, old, new)
        elif isinstance(e, dict):
            e = dict_replace_value(e, old, new)
        elif isinstance(e, str):
            e = e.replace(old, new)
        x.append(e)
    return x


def do_order(endpoint, order_conf, keys, count=1, proxy=None):
    logging.info(f"Testing: {endpoint} {count} times (Proxy: {proxy})")
    exchange_params = {
        "apiKey": keys["apiKey"],
        "secret": keys["secret"],
        "enableRateLimit": True,
        "options": {
            "defaultType": "spot",
        },
    }
    if proxy:
        exchange_params["proxies"] = {
            "http": proxy,
            "https": proxy,
        }
    exchange = ccxt.binance(exchange_params)
    exchange.urls = dict_replace_value(
        exchange.urls, 
        old="https://api.binance.com", 
        new=endpoint
    )

    if proxy is not None:
        logging.info(f"####### Proxy requests: {proxy}\n")
    else:
        logging.info("####### DIRECT requests\n")
    create_total_seconds = []
    cancel_total_seconds = []
    for round in range(count):
        # Create
        t1 = time.time()
        order = exchange.create_order(**order_conf)
        t2 = time.time()
        create_elapsed_seconds = t2 - t1
        logging.info(f"\tCreate: round: {round}: {create_elapsed_seconds}")
        create_total_seconds.append(create_elapsed_seconds)

        # Cancel
        time.sleep(1)
        t1 = time.time()
        exchange.cancel_all_orders(symbol=order_conf["symbol"])
        t2 = time.time()
        cancel_elapsed_seconds = t2 - t1
        cancel_total_seconds.append(cancel_elapsed_seconds)
        logging.info(f"\tCancel: round: {round}: {cancel_elapsed_seconds}")
        time.sleep(0.2)

    logging.info("Calculating latency for order Creation")
    calc_latency(create_total_seconds)
    logging.info("Calculating latency for order Cancelation")
    calc_latency(cancel_total_seconds)
    logging.info("\n")


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


def main():
    setup_log(loglevel="INFO")

    endpoint = "https://api1.binance.com"
    keys = {
        "apiKey": os.environ.get("BINANCE_API_KEY"),
        "secret": os.environ.get("BINANCE_SECRET_KEY"),
    }
    print(os.environ.get("BINANCE_API_KEY"))
    print(os.environ.get("BINANCE_SECRET_KEY"))
    order_conf = {
        "symbol": "BNBUSDT",  # Trading pair symbol (e.g., ETHUSDT)
        "side": "buy",  # Order side (BUY or SELL)
        "type": "limit",  # Order type (LIMIT, MARKET, etc.)
        "amount": 0.1,  # Quantity of the asset to buy/sell
        "price": 205,  # Price at which to buy/sell (only for LIMIT orders)
    }
    do_order(endpoint, order_conf, keys, count=10)


if __name__ == "__main__":
    main()
