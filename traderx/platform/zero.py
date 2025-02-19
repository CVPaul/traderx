#!/usr/bin/env python
#-*- coding:utf-8 -*-


import os
import numba as nb
import numpy as np
import pandas as pd
import traderx.data.convert as conv

from typing import List
from types import SimpleNamespace
from zero.trans.strategy.common import StructBase


# order status definations
INIT = 0
ADDED = 1
PARTFILL = 2
ALLFILLED = 3
CANCELLED = 4


class Platform(StructBase):

    def __init__(self, tick: int = 0, n_threads: int = 0, source = "PK"):
        super().__init__()
        self.tick = tick
        self.done = False
        self.n_threads = n_threads
        self.mds = np.array([])
        self.mask = np.array([])
        self.positions = np.array([])
        self.ask_orders_ = np.array([])
        self.bid_orders_ = np.array([])
        self.ask_order_loc = np.array([])
        self.bid_order_loc = np.array([])
        self.filled_orders = np.array([])
        self.filled_orders_cnt = 0
        self.max_hold_orders = 20
        self.g_order_id = 0
        self.timestamp = 0
        self.source_ = source
        self.last_ticks = []
        if source == 'PK':
            self.msg2md = conv.tick2md

    def reset(self, tickers: List[str]):
        self.done = False
        self.repeat = repeat
        self.tickers = tickers
        self.n_tickers = len(tickers)
        self.ticker2ii = {
            self.tickers[i]:i
            for i in range(self.n_tickers)}
        self.last_ticks = [None] * self.n_tickers
        # init the data buffers
        n_tickers = self.n_tickers
        self.orderbook_size = n_tickers * 1000
        self.mds = np.zeros((n_tickers, self.md_size), dtype=np.float32)
        self.mask = np.zeros(n_tickers, dtype=np.bool_)
        self.positions = np.zeros((n_tickers, self.position_size), dtype=np.float32)
        self.ask_orders_ = [set() for _ in range(n_tickers)]
        self.bid_orders_ = [set() for _ in range(n_tickers)]
        self.ask_order_loc = np.zeros(n_tickers, np.int32)
        self.bid_order_loc = np.zeros(n_tickers, np.int32)
        # init the order books
        self.orderbook = {}

    def step(self, message=None):
        tick = json.loads(message)
        ss = tick['s']
        ii = self.ticker2ii[ss]
        self.timestamp = self.msg2md(tick, self.mds[ii])
        self.match(tick)
        return tick

    @property
    def bid_orders(self):
        return self.bid_orders_[self.bid_orders_[:, self.order_id] > 0]

    @property
    def ask_orders(self):
        return self.ask_orders_[self.ask_orders_[:, self.order_id] > 0]

    def take(self, actions: np.array):
        for action in actions:
            if action[self.action] == 0:
                self.add(action)
            else:
                self.cancel(action)

    def add(self, action: np.array):
        self.g_order_id += 1
        ii = int(action[self.instrument_id] + 0.5)
        action[self.order_status] = INIT # init 
        # action[self.insert_time] = self.timestamp % self.time_scale_
        # action[self.insert_base] = self.timestamp // self.time_scale_
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

    def cancel(self, oi: int):
        if oi in self.orderbook:
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

    def match(self, message):
        ii = self.ticker2ii[tick.symbol]
        st = ii * self.max_hold_orders
        self.last_ticks[ii] = tick
        # buy side
        rs = []
        bi = self.bid_order_loc[ii]
        price = tick.ask_price_1
        for i in range(st, st + bi):
            # float由于误差会出现0.03 != 0.02999的现象，所以容许1e-7的偏差
            # 原本这里应该是ask_price <= order.price
            if 1.0 - price / self.bid_orders_[i][self.price] >= -1e-7:
                order = self.bid_orders_[i]
                self.filled_orders[self.filled_orders_cnt] = order
                self.filled_orders_cnt += 1
                volume = order[self.total_volume] - order[self.trade_volume]
                if order[self.side] == 0: # long-buy --> long-open
                    self.positions[ii][self.long_unfilled_buy] -= volume
                    self.positions[ii][self.long_buy] += volume
                else: # short-buy --> short-close
                    self.positions[ii][self.short_unfilled_buy] -= volume
                    self.positions[ii][self.short_buy] += volume
                self.bid_orders_[i, self.order_id] = 0
            else:
                rs.append(i)
        self.bid_order_loc[ii] = len(rs)
        if bi != len(rs):
            if self.bid_order_loc[ii] > 0:
                self.bid_orders_[st:st + len(rs)] = self.bid_orders_[rs]
        # sell side
        rs = []
        ai = self.ask_order_loc[ii]
        price = tick.bid_price_1
        for i in range(st, st + ai):
            # float由于误差会出现0.03 != 0.02999的现象，所以容许1e-7的偏差
            # 原本这里应该是bid_price >= order.price
            if price / self.ask_orders_[i][self.price] - 1.0 >= -1e-7:
                order = self.ask_orders_[i]
                self.filled_orders[self.filled_orders_cnt] = order
                self.filled_orders_cnt += 1
                volume = order[self.total_volume] - order[self.trade_volume]
                if order[self.side] == 0: # long-sell --> long-close
                    self.positions[ii][self.long_unfilled_sell] -= volume
                    self.positions[ii][self.long_sell] += volume
                else: # short-sell --> short-open
                    self.positions[ii][self.short_unfilled_sell] -= volume
                    self.positions[ii][self.short_sell] += volume
                self.ask_orders_[i, self.order_id] = 0
            else:
                rs.append(i)
        self.ask_order_loc[ii] = len(rs)
        if ai != len(rs):
            if self.ask_order_loc[ii] > 0:
                self.ask_orders_[st:st + len(rs)] = self.ask_orders_[rs]

    def on_tick(self, tick):
        ii = self.ticker2ii[tick.symbol]
        md = self.mds[ii]
        if self.source_ == "PK": # 盘口
            md[7] = float(tick.bid_price_1)
            md[17] = float(tick.ask_price_1)
            md[27] = float(tick.bid_volume_1)
            md[37] = float(tick.ask_volume_1)
        else:
            if self.source_ == "MD": # not depth --> md snapshot
                md[0] = tick.pre_close
                md[1] = tick.open_price
                md[2] = tick.high_price
                md[3] = tick.low_price
                md[4] = tick.last_price
                md[5] = tick.last_price
                md[6] = tick.volume
            md[7] = tick.bid_price_1
            md[8] = tick.bid_price_2
            md[9] = tick.bid_price_3
            md[10] = tick.bid_price_4
            md[11] = tick.bid_price_5
            md[12] = tick.bid_price_6
            md[13] = tick.bid_price_7
            md[14] = tick.bid_price_8
            md[15] = tick.bid_price_9
            md[16] = tick.bid_price_10
    
            md[17] = tick.ask_price_1
            md[18] = tick.ask_price_2
            md[19] = tick.ask_price_3
            md[20] = tick.ask_price_4
            md[21] = tick.ask_price_5
            md[22] = tick.ask_price_6
            md[23] = tick.ask_price_7
            md[24] = tick.ask_price_8
            md[25] = tick.ask_price_9
            md[26] = tick.ask_price_10
    
            md[27] = tick.bid_volume_1
            md[28] = tick.bid_volume_2
            md[29] = tick.bid_volume_3
            md[30] = tick.bid_volume_4
            md[31] = tick.bid_volume_5
            md[32] = tick.bid_volume_6
            md[33] = tick.bid_volume_7
            md[34] = tick.bid_volume_8
            md[35] = tick.bid_volume_9
            md[36] = tick.bid_volume_10
    
            md[37] = tick.ask_volume_1
            md[38] = tick.ask_volume_2
            md[39] = tick.ask_volume_3
            md[40] = tick.ask_volume_4
            md[41] = tick.ask_volume_5
            md[42] = tick.ask_volume_6
            md[43] = tick.ask_volume_7
            md[44] = tick.ask_volume_8
            md[45] = tick.ask_volume_9
            md[46] = tick.ask_volume_10
        md[47] = 1 # lot_size





