#!/usr/bin/env python
#-*- coding:utf-8 -*-


import os
import numpy as np

from typing import List
from zero.trans.strategy.common import StructBase


# order status definations
INIT = 0
ADDED = 1
PARTFILL = 2
ALLFILLED = 3
CANCELLED = 4


class Platform(StructBase):

    def __init__(self, source = "PK"):
        super().__init__()
        self.timestamp = 0
        self.g_order_id = 0

    def reset(self, tickers: List[str]):
        self.tickers = tickers
        self.n_tickers = len(tickers)
        self.ticker2ii = {
            self.tickers[i]:i
            for i in range(self.n_tickers)}
        # init the data buffers
        n_tickers = self.n_tickers
        self.orderbook_size = n_tickers * 1000
        self.mds = np.zeros((n_tickers, self.md_size), dtype=np.float32)
        self.positions = np.zeros((n_tickers, self.position_size), dtype=np.float32)
        self.ask_orders_ = [set() for _ in range(n_tickers)]
        self.ask_order_data = np.zeros((n_tickers * 20, self.order_size), dtype=np.float32)
        self.bid_orders_ = [set() for _ in range(n_tickers)]
        self.bid_order_data = np.zeros((n_tickers * 20, self.order_size), dtype=np.float32)
        # init the order books
        self.orderbook = {}

    def step(self, message):
        ss = message['s']
        ii = self.ticker2ii[ss]
        self.timestamp = message['E']
        # update md
        md = self.mds[ii]
        md[self.bidprice] = message['b']
        md[self.askprice] = message['a']
        md[self.bidvolume] = message['B']
        md[self.askvolume] = message['A']

    @property
    def bid_orders(self):
        count = 0
        for s in self.bid_orders_:
            for i in s:
                self.bid_order_data[count] = self.orderbook[i]
                count += 1
        return self.bid_order_data[:count]

    @property
    def ask_orders(self):
        count = 0
        for s in self.ask_orders_:
            for i in s:
                self.ask_order_data[count] = self.orderbook[i]
                count += 1
        return self.ask_order_data[:count]

    def action2order(self, action):
        ii = int(action[self.instrument_id] + 0.5)
        return dict(
            type = 'LIMIT',
            timeInForce = 'GTC',
            symbol = self.tickers[ii],
            price = action[self.price],
            quantity = action[self.total_volume],
            side = 'SELL' if action[self.direction] > 0.5 else 'BUY',
            positionSide = 'SHORT' if action[self.side] > 0.5 else 'LONG'
        )

    def recover(self, ii, order_id, order):
        action = np.array([
            ii, order_id, 0, float(order['price']), 
            0 if order['positionSide'] == 'LONG' else 1,
            0 if order['side'] == 'BUY' else 1,
            float(order['origQty']), float(order['executedQty']),
            order['time'] % self.time_scale_,
            order['time'] // self.time_scale_,
            0], dtype=np.float32)
        self.orderbook[order_id] = action
        if action[self.direction] < 0.5: # buy
            self.bid_orders_[ii].add(order_id)
        else:
            self.ask_orders_[ii].add(order_id)

    def add(self, action: np.array):
        self.g_order_id += 1
        ii = int(action[self.instrument_id] + 0.5)
        # action[self.order_status] = INIT # init 
        action[self.insert_time] = self.timestamp % self.time_scale_
        action[self.insert_base] = self.timestamp // self.time_scale_
        action[self.order_id] = self.g_order_id
        volume = action[self.total_volume]
        self.orderbook[self.g_order_id] = action.copy()
        if action[self.direction] == 0: # buy
            self.bid_orders_[ii].add(self.g_order_id)
            if action[self.side] == 0: # long-buy --> long-open
                self.positions[ii][self.long_unfilled_buy] += volume
            else: # short-buy --> short-close
                self.positions[ii][self.short_unfilled_buy] += volume
        else: # sell
            self.ask_orders_[ii].add(self.g_order_id)
            if action[self.side] == 0: # long-sell --> long-close
                self.positions[ii, self.long_unfilled_sell] += volume
            else: # short-sell --> short-open
                self.positions[ii, self.short_unfilled_sell] += volume
        return self.g_order_id

    def cancel(self, oi: int):
        order = self.orderbook.pop(oi)
        ii = int(order[self.instrument_id] + 0.5)
        volume = order[self.total_volume] - order[self.trade_volume]
        if order[self.direction] == 0: # buy
            if order[self.side] == 0: # long-buy --> long-open
                self.positions[ii, self.long_unfilled_buy] -= volume
            else: # short-buy --> short-close
                self.positions[ii, self.short_unfilled_buy] -= volume
            self.bid_orders_[ii].remove(oi)
        else:
            if order[self.side] == 0: # long-sell --> long-close
                self.positions[ii][self.long_unfilled_sell] -= volume
            else: # short-sell --> short-open
                self.positions[ii][self.short_unfilled_sell] -= volume
            self.ask_orders_[ii].remove(oi)

    def match(self, tick):
        order = tick['o']
        ii = self.ticker2ii[order['s']]
        user_oid = order['c']
        order_id = int(user_oid.split('_')[-1])
        # total_volume = float(order['q'])
        # trade_volume = float(order['z'])
        volume = float(order['l'])
        if order_id in self.orderbook:
            self.orderbook[order_id][self.trade_volume] = float(order['z'])
        else:
            logging.error(f'recived an unknown TRADE message:{tick}')
            return
        # buy side
        if order['S'] == 'BUY': 
            if order[self.side] == 0: # long-buy --> long-open
                self.positions[ii, self.long_unfilled_buy] -= volume
                self.positions[ii, self.long_buy] += volume
            else: # short-buy --> short-close
                self.positions[ii, self.short_unfilled_buy] -= volume
                self.positions[ii, self.short_buy] += volume
            if order['X'] == 'FILLED':
                self.bid_orders_[ii].remove(order_id)
        # sell side
        if order['S'] == 'SELL': 
            if order[self.side] == 0: # long-sell --> long-close
                self.positions[ii, self.long_unfilled_sell] -= volume
                self.positions[ii, self.long_sell] += volume
            else: # short-sell --> short-open
                self.positions[ii, self.short_unfilled_sell] -= volume
                self.positions[ii, self.short_sell] += volume
            if order['X'] == 'FILLED':
                self.ask_orders_[ii].remove(order_id)
