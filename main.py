#!/usr/bin/env python
#-*- coding:utf-8 -*-


import os
import sys
import time
import json
import torch
import logging
import argparse
import numpy as np

from datetime import datetime as dt
from datetime import timedelta as td
from websocket import WebSocketConnectionClosedException

from binance.websocket.um_futures.websocket_client import UMFuturesWebsocketClient
from binance.websocket.cm_futures.websocket_client import CMFuturesWebsocketClient

from traderx.strategy.abtri import Abtri
from traderx.manager.zero import Platform
from traderx.utils.const import API_KEY, API_SECRET
from traderx.utils.common import create_rest_api


class TraderManager:

    def __init__(self, type_, symbols, stgname, key=None, secret=None):
        self.key = key
        self.type = type_
        self.secret = secret
        self.stgname = stgname
        self.namelen = len(stgname)
        self.cmpname = stgname + '_'
        self.listenkey = None
        self.connected = False
        self.last_check_time = 0
        self.symbols = symbols.split(',')
        # set the position store path
        self.position_path = f'{self.stgname}.pos.npy'
        self.order_id_path = f'{self.stgname}.oid.txt'
        # init trader platform
        self.platform = Platform()
        self.platform.reset(self.symbols)
        # init the strategy
        self.strategy = Abtri(
            open_threshold=1.0, #5.0,
            force_open_threshold=2.0, # 9.0,
            close_threshold=0.5, # 4.0,
            force_close_threshold= 1.0, # 7.0,
            rolling_cache_len=120,
            # trading parameters
            max_volume_per_tick=0.008,
            max_volume_per_size=0.008,
            max_volume_legdiff=0.008,
            order_timeout_ms=5000,
            action_timeout_ms=1000
        )
        # risk control
        self.add_action_count = 0
        self.add_action_limit = 10
        self.cancel_action_count = 0
        self.cancel_action_limit = 20

    def on_open(self, _):
        self.connected = True
        logging.info(f"web socket opened!")

    def on_error(self, _, e):
        if isinstance(e, WebSocketConnectionClosedException):
            self.connected = False
            logging.error(f"found that websocket loss it's connection!")

    def new_user_order_id(self, g_order_id):
        return f"{self.stgname}_{g_order_id}"

    def is_this_strategy(self, user_order_id):
        return user_order_id.startswith(self.cmpname)

    def recover(self):
        # postions
        if os.path.exists(self.position_path):
            self.platform.positions = np.load(self.position_path)
        # g_order_id
        if os.path.exists(self.order_id_path):
            with open(self.order_id_path) as fp:
                self.platform.g_order_id = int(fp.read().strip())
        # orders
        for symbol in self.symbols:
            orders = self.cli.get_open_orders(symbol)
            for order in orders:
                user_oid = order['clientOrderId']
                if not self.is_this_strategy(user_oid):
                    continue # not belongs to this strategy
                order_id = int(user_oid[self.namelen + 1:])
                self.platform.recover(ii, order_id, order) # recover one order

    def save_positions(self):
        np.save(self.position_path, self.platform.positions)

    def save_order_id(self):
        with open(self.order_id_path, 'w') as fp:
            fp.write(str(self.platform.g_order_id))

    def on_message(self, _, message):
        try:
            doc = json.loads(message)
            if 'E' in doc:
                if doc['E'] - self.last_check_time > 60000: # 1min
                    logging.info(f'HeartBeat!')
                    self.last_check_time = doc['E']
            etype = doc.get('e', '')
            if etype == 'bookTicker':
                if doc['s'] not in self.symbols:
                    return # this message not belongs to this strategy
                self.platform.step(doc)
                actions = self.strategy.forward(
                    self.platform.timestamp,
                    torch.from_numpy(self.platform.mds).transpose(0, 1),
                    torch.from_numpy(self.platform.positions).transpose(0, 1),
                    torch.from_numpy(self.platform.ask_orders).transpose(0, 1),
                    torch.from_numpy(self.platform.bid_orders).transpose(0, 1)
                ).transpose(0, 1).numpy()
                for action in actions:
                    if action[self.strategy.action] == 0: # add
                        order_id = self.platform.add(action)
                        order = self.platform.action2order(action)
                        order['newClientOrderId'] = self.new_user_order_id(order_id)
                        self.add_action_count += 1
                        if self.add_action_count <= self.add_action_limit:
                            self.cli.new_order(**order)
                            logging.info(f'ORDER|{order}')
                        else:
                            logging.warning(f"{self.add_action_count=} > {self.add_action_limit=}")
                    else: # cancel
                        ii = int(action[self.strategy.instrument_id] + 0.5)
                        order_id = int(action[self.strategy.order_id] + 0.5)
                        symbol = self.symbols[ii]
                        self.cancel_action_count += 1
                        if self.cancel_action_count <= self.cancel_action_limit:
                            self.cli.cancel_order(
                                symbol=symbol,
                                origClientOrderId=self.new_user_order_id(order_id))
                        else:
                            logging.warning(f"{self.cancel_action_count=} > {self.cancel_action_limit=}")
                        logging.info(f'CANCEL|{symbol=},{action=}')
                if actions.shape[0]:
                    self.save_positions()
            elif etype == 'ORDER_TRADE_UPDATE':
                if not self.is_this_strategy(doc['o']['c']):
                    return # message not belongs to this strategy
                if doc['x'] == 'NEW':
                    pass
                elif doc['x'] == 'PARTIALLY_FILLED' or doc['x'] == 'FILLED':
                    self.platform.match(doc)
                    self.save_positions()
                elif doc['x'] == 'CANCELED' or doc['x'] == 'EXPIRED':
                    self.platform.cancel(int(doc['o']['c'][self.namelen + 1:]))
                    self.save_positions()
                else:
                    logging.warning(f"MSSAGE|{doc}")
        except Exception as e:
            logging.exception(f"processing message failed with error:{e}!")

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

        # Subscribe the user data stream
        self.wss.user_data(self.listenkey)

    def create_cli(self, is_online):
        assert self.key and self.secret
        self.cli = create_rest_api(
            self.key, self.secret, self.type, is_online=is_online)

    def new_listen_key(self, retry_cnt=3):
        if self.listenkey:
            self.close_listen_key()
        for i in range(retry_cnt): # retry count = 3
            try:
                self.listenkey = self.cli.new_listen_key()['listenKey']
                self.listen_key_time = time.time()
                logging.info(f'new listen key: {self.listenkey} succeeded! retry count={i + 1}/{retry_cnt}')
                break
            except Exception as e:
                logging.exception(f'new listen key: {self.listenkey} failed with exception:{e}, retry count={i + 1}/{retry_cnt}')

    def renew_listen_key(self, interval=1800, retry_cnt=3):
        if time.time() - self.listen_key_time > interval: # sec
            for i in range(retry_cnt):
                try:
                    self.cli.renew_listen_key(self.listenkey)
                    self.listen_key_time = time.time()
                    logging.info(f'renew listen key: {self.listenkey} succeeded! retry count={i + 1}/{retry_cnt}')
                    break
                except Exception as e:
                    logging.exception(f'renew listen key: {self.listenkey} failed with exception:{e}, retry count={i + 1}/{retry_cnt}')
    
    def close_listen_key(self, retry_cnt=3):
        for i in range(retry_cnt): # retry count = 3
            try:
                self.cli.close_listen_key(self.listenkey)
                logging.info(f'close listen key: {self.listenkey} succeeded! retry count={i + 1}/{retry_cnt}')
                self.listenkey = None
                break
            except Exception as e:
                logging.exception(f'close listen key: {self.listenkey} failed with exception:{e}, retry count={i + 1}/{retry_cnt}')


if __name__  == "__main__":
    # args
    parser = argparse.ArgumentParser()
    parser.add_argument('--stgname', type=str, required=True)
    parser.add_argument('--symbols', '-s', type=str, required=True)
    parser.add_argument('--type', type=str, default='um')
    parser.add_argument('--online', action='store_true')
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
        args.type, args.symbols, args.stgname, key=API_KEY, secret=API_SECRET)
    # main loop
    # test_var = 0
    try:
        while True:
            trader.create_cli(args.online)
            trader.new_listen_key()
            trader.recover()
            trader.sub_stream()
            # oid = '12345'
            while True:
                time.sleep(1)
                # test_var += 1
                # if test_var == 5:
                #     trader.cli.new_order(
                #         symbol="ETHUSDT",
                #         side="BUY",
                #         positionSide="LONG",
                #         type="LIMIT",
                #         quantity=1,
                #         price=2500,
                #         newClientOrderId=oid,
                #         timeInForce='GTC',
                #         timestamp=int(time.time() * 1000)
                #     )
                # if test_var == 10:
                #     trader.cli.cancel_order(
                #         symbol="ETHUSDT", origClientOrderId=oid)
                if trader.connected == False:
                    trader.close_listen_key()
                    trader.wss.stop()
                    logging.info("websocket thread stopped succeeded!")
                    break
                trader.renew_listen_key(interval=1800)
    except KeyboardInterrupt:
        trader.connected = False
        trader.close_listen_key()
        trader.wss.stop()
        logging.info("user kerboard interrupt: websocket thread stopped succeeded!")
