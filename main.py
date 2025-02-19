#!/usr/bin/env python
#-*- coding:utf-8 -*-


import sys
import time
import json
import torch
import logging
import argparse

from datetime import datetime as dt
from datetime import timedelta as td
from websocket import WebSocketConnectionClosedException

from binance.cm_futures import CMFutures
from binance.um_futures import UMFutures
from binance.websocket.um_futures.websocket_client import UMFuturesWebsocketClient
from binance.websocket.cm_futures.websocket_client import CMFuturesWebsocketClient

from traderx.strategy.abtri import Abtri


API_KEY =  "sVxH1Gao4OpkPfrLKBCPvd8CoZnwGJsEVGAlSTJxXrVW9Pp4TcgJgeuii6H4CbBo"
API_SECRET = "R90YMbrL0X5iUU9Gg5ZR1GonZ4668gWgVEfRwSswrSisXRaoteD3awK46gNWTTLH"


class TraderManager:

    def __init__(self, type_, symbols, key=None, secret=None):
        self.key = key
        self.type = type_
        self.secret = secret
        self.listenkey = None
        self.connected = False
        self.last_check_time = 0
        self.symbols = symbols.split('|')

    def on_open(self, _):
        self.connected = True
        logging.info(f"web socket opened!")

     
    def on_error(self, _, e):
        if isinstance(e, WebSocketConnectionClosedException):
            self.connected = False
            logging.error(f"found that websocket loss it's connection!")

    def on_message(self, _, message):
        try:
            doc = json.loads(message)
            if 'E' in doc:
                if doc['E'] - self.last_check_time > 60000: # 1min
                    logging.info(f'HeartBeat!')
                    self.last_check_time = doc['E']
        except Exception as e:
            logging.error(f"write message failed with error:{e}!")

    def trade(self, message):
        s = message['s']
        i = sym2idx[s]

    def sub_stream(self):
        if self.type == 'um':
            self.wss = UMFuturesWebsocketClient(
                on_open=self.on_open,
                on_error=self.on_error,
                on_message=self.on_message)
        elif self.type == 'cm':
            self.wss = CMFuturesWebsocketClient(
                on_open=self.on_open,
                on_error=self.on_error,
                on_message=self.on_message)
        else:
            raise RuntimeError(f"unsupported type got:{self.type}, only `cm`, `um` were allowed!")

        # Subscribe to a single symbol stream
        # client.agg_trade(symbol="ETHUSDT")
        for symbol in self.symbols:
            # self.wss.agg_trade(symbol=symbol)
            self.wss.book_ticker(symbol=symbol)
            # self.wss.partial_book_depth(symbol=symbol, level=20, speed=100)

    def create_cli(self):
        assert self.key and self.secret
        if self.type == 'um':
            self.cli = UMFutures(key=self.key, secret=self.secret)
        elif tp == 'cm':
            self.cli = CMFutures(key=self.key, secret=self.secret)
        else:
            raise RuntimeError(f"unsupported type got:{self.type}, only `cm`, `um` were allowed!")

    def new_listen_key(self):
        if self.listenkey:
            self.close_listen_key()
        rsp = self.cli.new_listen_key()
        self.listenkey = rsp['listenKey']
        self.listen_key_time = time.time()

    def renew_listen_key(self):
        if time.time() - self.listen_key_time > 1800: # 30min
            self.cli.renew_listen_key(self.listenkey)
            self.listen_key_time = time.time()
    
    def close_listen_key(self):
        rsp = self.cli.close_listen_key(self.listenkey)
        self.listenkey = None


if __name__  == "__main__":
    # args
    parser = argparse.ArgumentParser()
    parser.add_argument('--stgname', type=str, required=True)
    parser.add_argument('--symbols', '-s', type=str, required=True)
    parser.add_argument('--type', type=str, default='um')
    args = parser.parse_args()
    # logging
    logging.basicConfig(
        filename=f'{dt.now().date()}.{args.type}.{args.stgname}.log',  # 日志文件名
        filemode='a',            # 文件模式，'a'表示追加模式
        level=logging.INFO,      # 日志级别
        format='%(asctime)s - %(levelname)s - %(message)s'  # 日志格式
    )
    # init TraderManager
    trader = TraderManager(
        args.type, args.symbols, key=API_KEY, secret=API_SECRET)
    # main loop
    while True:
        trader.create_cli()
        trader.new_listen_key()
        trader.sub_stream()
        while True:
            time.sleep(1)
            if trader.connected == False:
                trader.close_listen_key()
                trader.wss.stop()
                logging.info("websocket thread stopped succeeded!")
                break
