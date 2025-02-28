#!/usr/bin/env python
#-*- coding:utf-8 -*-


import torch
import logging
import numpy as np

from zero.trans.strategy.strategy import Strategy

# 这一版没问题了，发布上线
class Abtri(Strategy):

    def __init__(
        self,
        # algo parameters
        open_threshold=5.0, force_open_threshold=9.0,
        close_threshold=-4.0, force_close_threshold=-7.0,
        rolling_cache_len = 600,

        # trading parameters
        max_volume_per_tick=2, max_volume_per_size=6, max_volume_legdiff=3,

        order_timeout_ms=5000, action_timeout_ms=1000,
    ):
        super().__init__()
        self.open_threshold = open_threshold
        self.force_open_threshold = force_open_threshold

        self.close_threshold = close_threshold
        self.force_close_threshold = force_close_threshold

        # 0 means empty, 1 means long0 short1, -1 means short0 long1
        self.current_dir = 0

        self.rolling_window_basis_sec = torch.zeros(rolling_cache_len, 4)
        self.rolling_window_is_full = False
        self.rolling_window_idx = 0
        self.current_timestamp = 0
        self.last_action_timestamp = 0
        self.current_basisbp = None

        # trading parameters
        self.max_volume_per_tick = max_volume_per_tick
        self.max_volume_per_side = max_volume_per_size

        self.max_volume_legdiff = max_volume_legdiff
        self.order_timeout_ms = order_timeout_ms
        self.action_timeout_ms = action_timeout_ms

        self.consider_maker = [True, False]
        self.fake_position = torch.zeros([self.position_size, 2])
        self.fake_trade = [False, True]

    # 以当前盘口本方一档为基准，向对方移动多少价格，正代表靠近对方，负代表远离对方，如果price是None，下taker
    def _long_open_maker(self, instrument_id, mds, volume, price=0):
        if price is None or not self.consider_maker[instrument_id]:
            return [instrument_id, 0, 0, mds[self.askprice, instrument_id], 0, 0, volume, 0, 0, 0, 0, 1.0]
        elif price == 0:
            return [instrument_id, 0, 0, mds[self.bidprice, instrument_id], 0, 0, volume, 0, 0, 0, 0, 1.0]
        else:
            p = min(mds[self.askprice, instrument_id] - self.price_tick, mds[self.bidprice, instrument_id] + price)
            return [instrument_id, 0, 0, p, 0, 0, volume, 0, 0, 0, 0, 1.0]

    def _long_close_maker(self, instrument_id, mds, volume, price=0):
        if price is None or not self.consider_maker[instrument_id]:
            return [instrument_id, 0, 0, mds[self.bidprice, instrument_id], 0, 1, volume, 0, 0, 0, 0, 1.0]
        elif price == 0:
            return [instrument_id, 0, 0, mds[self.askprice, instrument_id], 0, 1, volume, 0, 0, 0, 0, 1.0]
        else:
            p = max(mds[self.bidprice, instrument_id] + self.price_tick, mds[self.askprice, instrument_id] - price)
            return [instrument_id, 0, 0, p, 0, 1, volume, 0, 0, 0, 0, 1.0]

    def _short_open_maker(self, instrument_id, mds, volume, price=0):
        if price is None or not self.consider_maker[instrument_id]:
            return [instrument_id, 0, 0, mds[self.bidprice, instrument_id], 1, 1, volume, 0, 0, 0, 0, 1.0]
        elif price == 0:
            return [instrument_id, 0, 0, mds[self.askprice, instrument_id], 1, 1, volume, 0, 0, 0, 0, 1.0]
        else:
            p = max(mds[self.bidprice, instrument_id] + self.price_tick, mds[self.askprice, instrument_id] - price)
            return [instrument_id, 0, 0, p, 1, 1, volume, 0, 0, 0, 0, 1.0]

    def _short_close_maker(self, instrument_id, mds, volume, price=0):
        if price is None or not self.consider_maker[instrument_id]:
            return [instrument_id, 0, 0, mds[self.askprice, instrument_id], 1, 0, volume, 0, 0, 0, 0, 1.0]
        elif price == 0:
            return [instrument_id, 0, 0, mds[self.bidprice, instrument_id], 1, 0, volume, 0, 0, 0, 0, 1.0]
        else:
            p = min(mds[self.askprice, instrument_id] - self.price_tick, mds[self.bidprice, instrument_id] + price)
            return [instrument_id, 0, 0, p, 1, 0, volume, 0, 0, 0, 0, 1.0]

    # 过滤会造成仓位错误的订单，比如有多头时开空头，没有多头时平多头这样的错单
    def _filter_invalid_orders(self, positions, new_orders):
        pos_long = positions[self.long_init_pos] + positions[self.long_buy] - positions[self.long_sell]
        pos_short = positions[self.short_init_pos] + positions[self.short_sell] - positions[self.short_buy]
        inst_id = new_orders[self.instrument_id].to(torch.long)
        valid_orders = \
            ((new_orders[self.side] == 0) & (new_orders[self.direction] == 0) & (pos_short[inst_id] == 0)) | \
            ((new_orders[self.side] == 1) & (new_orders[self.direction] == 1) & (pos_long[inst_id] == 0)) | \
            ((new_orders[self.side] == 0) & (new_orders[self.direction] == 1) & (pos_long[inst_id] != 0)) | \
            ((new_orders[self.side] == 1) & (new_orders[self.direction] == 0) & (pos_short[inst_id] != 0))
        new_orders = new_orders[:, valid_orders]
        return new_orders

    # alpha部分，计算基差，给出套利决策
    def eval_basis(self):
        basisbp = self.current_basisbp
        rolling_mean_basisbp = self.rolling_window_basis_sec.mean(dim=0)

        force_long_open = basisbp[2] > rolling_mean_basisbp[2] + self.force_open_threshold
        force_long_close = basisbp[1] < rolling_mean_basisbp[1] + self.force_close_threshold
        force_short_open = basisbp[1] <  rolling_mean_basisbp[1] - self.force_open_threshold
        force_short_close = basisbp[2] > rolling_mean_basisbp[2] - self.force_close_threshold

        if self.consider_maker[0] and self.consider_maker[1]:
            try_long_open_idx = 1
            try_short_open_idx = 2
        elif self.consider_maker[0] and not self.consider_maker[1]:
            try_long_open_idx = 3
            try_short_open_idx = 0
        elif self.consider_maker[1] and not self.consider_maker[0]:
            try_long_open_idx = 0
            try_short_open_idx = 3
        else:
            try_long_open_idx = 2
            try_short_open_idx = 1
        try_long_close_idx, try_short_close_idx = try_short_open_idx, try_long_open_idx

        try_long_open = basisbp[try_long_open_idx] > rolling_mean_basisbp[try_long_open_idx] + self.open_threshold
        try_long_close = basisbp[try_long_close_idx] < rolling_mean_basisbp[try_long_close_idx] + self.close_threshold
        try_short_open = basisbp[try_short_open_idx] < rolling_mean_basisbp[try_short_open_idx] - self.open_threshold
        try_short_close = basisbp[try_short_close_idx] > rolling_mean_basisbp[try_short_close_idx] - self.open_threshold

        conflict_long_open = force_long_close or try_long_close or force_short_open or try_short_open
        conflict_short_open = force_short_close or try_short_close or force_long_open or try_long_open
        conflict_long_close = force_long_open or try_long_open
        conflict_short_close = force_short_open or try_short_open

        new_force_long_open = force_long_open and not conflict_long_open
        new_force_short_open = force_short_open and not conflict_short_open
        new_force_long_close = force_long_close and not conflict_long_close
        new_force_short_close = force_short_close and not conflict_short_close

        new_try_long_open = try_long_open and not conflict_long_open
        new_try_short_open = try_short_open and not conflict_short_open
        new_try_long_close = try_long_close and not conflict_long_close
        new_try_short_close = try_short_close and not conflict_short_close

        return \
            new_force_long_open, new_force_short_open, new_force_long_close, new_force_short_close, \
            new_try_long_open, new_try_short_open, new_try_long_close, new_try_short_close

    def status_empty(self, mds):
        assert self.current_dir == 0
        if self.current_timestamp < self.last_action_timestamp + self.action_timeout_ms:
            return torch.zeros(self.order_size, 0)
        force_long_open, force_short_open, force_long_close, force_short_close, try_long_open, try_short_open, try_long_close, try_short_close = self.eval_basis()

        if force_long_open:
            # taker
            order = [
                self._long_open_maker(0, mds, volume=self.max_volume_per_tick, price=None),
                self._short_open_maker(1, mds, volume=self.max_volume_per_tick, price=None),
            ]
        elif force_short_open:
            # taker
            order = [
                self._short_open_maker(0, mds, volume=self.max_volume_per_tick, price=None),
                self._long_open_maker(1, mds, volume=self.max_volume_per_tick, price=None),
            ]
        elif try_long_open:
            # maker
            order = [
                self._long_open_maker(0, mds, volume=self.max_volume_per_tick, price=0),
                self._short_open_maker(1, mds, volume=self.max_volume_per_tick, price=0),
            ]
        elif try_short_open:
            # maker
            order = [
                self._short_open_maker(0, mds, volume=self.max_volume_per_tick, price=0),
                self._long_open_maker(1, mds, volume=self.max_volume_per_tick, price=0),
            ]
        else:
            return torch.zeros(self.order_size, 0)
        self.last_action_timestamp = self.current_timestamp
        order = torch.tensor(order).transpose(1, 0)
        return order

    def _get_probable_fill_volume(self, ask_orders, bid_orders):
        ask_order_insert_time = ask_orders[self.insert_base].to(torch.float64) * 1e6 + ask_orders[self.insert_time]
        ask_order_no_timeout = ask_order_insert_time + self.order_timeout_ms > self.current_timestamp
        probable_long_close_mask = (ask_orders[self.side] == 0) & ask_order_no_timeout
        probable_short_open_mask = (ask_orders[self.side] == 1) & ask_order_no_timeout
        unfilled_long_close = ask_orders[:, probable_long_close_mask]
        unfilled_short_open = ask_orders[:, probable_short_open_mask]

        bid_order_insert_time = bid_orders[self.insert_base].to(torch.float64) * 1e6 + bid_orders[self.insert_time]
        bid_order_no_timeout = bid_order_insert_time + self.order_timeout_ms > self.current_timestamp
        probable_long_open_mask = (bid_orders[self.side] == 0) & bid_order_no_timeout
        probable_short_close_mask = (bid_orders[self.side] == 1) & bid_order_no_timeout
        unfilled_long_open = bid_orders[:, probable_long_open_mask]
        unfilled_short_close = bid_orders[:, probable_short_close_mask]

        unfilled_long_open = (unfilled_long_open[self.total_volume] - unfilled_long_open[self.trade_volume]).sum()
        unfilled_long_close = (unfilled_long_close[self.total_volume] - unfilled_long_close[self.trade_volume]).sum()
        unfilled_short_open = (unfilled_short_open[self.total_volume] - unfilled_short_open[self.trade_volume]).sum()
        unfilled_short_close = (unfilled_short_close[self.total_volume] - unfilled_short_close[self.trade_volume]).sum()

        return (
            probable_long_open_mask, probable_long_close_mask,
            probable_short_open_mask, probable_short_close_mask,
            unfilled_long_open, unfilled_long_close,
            unfilled_short_open, unfilled_short_close
        )

    def status_trading(self, mds, positions, ask_orders, bid_orders):
        pos_long = positions[self.long_init_pos] + positions[self.long_buy] - positions[self.long_sell]
        pos_short = positions[self.short_init_pos] + positions[self.short_sell] - positions[self.short_buy]
        probable_long_open_mask, probable_long_close_mask, probable_short_open_mask, probable_short_close_mask, \
            unfilled_long_open, unfilled_long_close, unfilled_short_open, unfilled_short_close = self._get_probable_fill_volume(ask_orders, bid_orders)

        # 先平掉所有过期的订单，确认所有外挂单都合法之后再进入后续交易
        cancel_orders = torch.concat([
            ask_orders[:, ~(probable_long_close_mask | probable_short_open_mask)],
            bid_orders[:, ~(probable_long_open_mask | probable_short_close_mask)]
        ], dim=1)
        if cancel_orders.shape[1] != 0:
            cancel_orders[self.action] = 1
            return cancel_orders

        if self.current_timestamp < self.last_action_timestamp + self.action_timeout_ms:
            return torch.zeros(self.order_size, 0)

        # 进入策略部分
        long_id = 0 if self.current_dir == 1 else 1
        short_id = 1 if self.current_dir == 1 else 0

        # 检查仓位合法性，理论上不会发生
        assert pos_long[short_id] == pos_short[long_id] == 0
        short_open_0 = (ask_orders[self.instrument_id] == long_id) & (ask_orders[self.side] == 1)
        long_open_1 = (bid_orders[self.instrument_id] == short_id) & (bid_orders[self.side] == 0)
        assert short_open_0.sum() == long_open_1.sum() == 0

        # 计算当前仓位
        _pos_long = pos_long[long_id]
        _pos_short = pos_short[short_id]
        unfilled_long_open_orders = bid_orders[:, probable_long_open_mask & (bid_orders[self.instrument_id] == long_id)]
        unfilled_short_open_orders = ask_orders[:, probable_short_open_mask & (ask_orders[self.instrument_id] == short_id)]
        unfilled_long_close_orders = ask_orders[:, probable_long_close_mask & (ask_orders[self.instrument_id] == long_id)]
        unfilled_short_close_orders = bid_orders[:, probable_short_close_mask & (bid_orders[self.instrument_id] == short_id)]
        unfilled_pos_long_open = (unfilled_long_open_orders[self.total_volume] - unfilled_long_open_orders[self.trade_volume]).sum()
        unfilled_pos_short_open = (unfilled_short_open_orders[self.total_volume] - unfilled_short_open_orders[self.trade_volume]).sum()
        unfilled_pos_long_close = (unfilled_long_close_orders[self.total_volume] - unfilled_long_close_orders[self.trade_volume]).sum()
        unfilled_pos_short_close = (unfilled_short_close_orders[self.total_volume] - unfilled_short_close_orders[self.trade_volume]).sum()

        # 检查是否仓位balance，并taker平掉单腿
        balance = (_pos_long + unfilled_pos_long_open - unfilled_pos_long_close) - (_pos_short + unfilled_pos_short_open - unfilled_pos_short_close)
        if balance != 0:
            if balance > 0:
                order = [self._long_close_maker(long_id, mds, volume=balance, price=None)]
            else:
                order = [self._short_close_maker(short_id, mds, volume=-balance, price=None)]
            self.last_action_timestamp = self.current_timestamp
            return torch.tensor(order).transpose(1, 0)

        # 如果考虑短期可能成交的订单，此时应该多空balance，进入开平仓逻辑
        force_long_open, force_short_open, force_long_close, force_short_close, try_long_open , try_short_open, try_long_close, try_short_close = self.eval_basis()
        try_to_open = (self.current_dir == 1 and try_long_open) or (self.current_dir == -1 and try_short_open)
        force_open = (self.current_dir == 1 and force_long_open) or (self.current_dir == -1 and force_short_open)
        try_to_close = (self.current_dir == 1 and try_long_close) or (self.current_dir == -1 and try_short_close)
        force_close = (self.current_dir == 1 and force_long_close) or (self.current_dir == -1 and force_short_close)
        if (try_to_open or force_open):
            otherside_order = torch.concat([
                ask_orders[:, ask_orders[self.instrument_id] == long_id],
                bid_orders[:, bid_orders[self.instrument_id] == short_id],
            ], dim=1) # short open的情况在上文的仓位合法性检查时已考虑，只撤掉平仓的订单即可
            if otherside_order.shape[1] != 0:
                otherside_order[self.action] = 1
                return otherside_order

            assert unfilled_pos_long_close == unfilled_pos_short_close == 0
            unfilled_pos_long = unfilled_pos_long_open
            unfilled_pos_short = unfilled_pos_short_open
            assert _pos_long + unfilled_pos_long == _pos_short + unfilled_pos_short
            target_pos = min(_pos_long + unfilled_pos_long + self.max_volume_per_tick, self.max_volume_per_side) # 仓位上限
            target_pos = min(target_pos, min(_pos_long, _pos_short) + self.max_volume_legdiff) # 单腿上限，风控限制
            extra_pos = target_pos - (_pos_long + unfilled_pos_long)
            if extra_pos > 0:
                order = [
                    self._long_open_maker(long_id, mds, volume=extra_pos, price=None if force_open else 0),
                    self._short_open_maker(short_id, mds, volume=extra_pos, price=None if force_open else 0),
                ]
            else:
                return torch.zeros(self.order_size, 0)
        elif (try_to_close or force_close):
            otherside_order = torch.concat([
                ask_orders[:, ask_orders[self.instrument_id] == short_id],
                bid_orders[:, bid_orders[self.instrument_id] == long_id],
            ], dim=1)
            if otherside_order.shape[1] != 0:
                otherside_order[self.action] = 1
                return otherside_order

            assert unfilled_pos_long_open == unfilled_pos_short_open == 0
            unfilled_pos_long = unfilled_pos_long_close
            unfilled_pos_short = unfilled_pos_short_close
            assert _pos_long - unfilled_pos_long == _pos_short - unfilled_pos_short
            target_pos = max(0, _pos_long - unfilled_pos_long - self.max_volume_per_tick)
            target_pos = max(target_pos, max(_pos_long, _pos_short) - self.max_volume_legdiff)
            extra_pos = _pos_long - unfilled_pos_long - target_pos
            if extra_pos > 0:
                order = [
                    self._long_close_maker(long_id, mds, volume=extra_pos, price=None if force_close else 0),
                    self._short_close_maker(short_id, mds, volume=extra_pos, price=None if force_close else 0),
                ]
            else:
                return torch.zeros(self.order_size, 0)
        else:
            return torch.zeros(self.order_size, 0)
        self.last_action_timestamp = self.current_timestamp
        order = torch.tensor(order).transpose(1, 0)
        return order

    def filter_fake_trade(self, orders):
        fake_orders = \
            (self.fake_trade[0] & (orders[self.instrument_id] == 0)) | \
            (self.fake_trade[1] & (orders[self.instrument_id] == 1))
        assert (orders[self.action, fake_orders] == 0).all()
        if fake_orders.sum() == 0:
            return orders
        valid_orders = orders[:, ~fake_orders]
        fake_orders = orders[:, fake_orders]

        long_buy_orders = (fake_orders[self.side] == 0) & (fake_orders[self.direction] == 0)
        long_sell_orders = (fake_orders[self.side] == 0) & (fake_orders[self.direction] == 1)
        short_sell_orders = (fake_orders[self.side] == 1) & (fake_orders[self.direction] == 1)
        short_buy_orders = (fake_orders[self.side] == 1) & (fake_orders[self.direction] == 0)

        for inst_id in range(self.fake_position.shape[1]):
            self.fake_position[self.long_buy, inst_id] += fake_orders[self.total_volume, long_buy_orders & (fake_orders[self.instrument_id]==inst_id)].sum()
            self.fake_position[self.long_sell, inst_id] += fake_orders[self.total_volume, long_sell_orders & (fake_orders[self.instrument_id]==inst_id)].sum()
            self.fake_position[self.short_sell, inst_id] += fake_orders[self.total_volume, short_sell_orders & (fake_orders[self.instrument_id]==inst_id)].sum()
            self.fake_position[self.short_buy, inst_id] += fake_orders[self.total_volume, short_buy_orders & (fake_orders[self.instrument_id]==inst_id)].sum()
        return valid_orders

    def forward(self, timestamp, mds, positions, ask_orders, bid_orders):
        if (mds[self.askprice] == 0).any() or (mds[self.bidprice] == 0).any():
            return torch.zeros(self.order_size, 0)

        price_1 = mds[[self.askprice, self.bidprice], 1]
        price_0 = mds[[self.askprice, self.bidprice], 0]
        basis = price_1.unsqueeze(dim=1) - price_0.unsqueeze(dim=0)
        avgprice = (price_1.unsqueeze(dim=1) + price_0.unsqueeze(dim=0)) / 2
        basisbp = ((basis / avgprice) * 1e4).reshape(-1) # a1-a0, a1-b0, b1-a0, b1-b0
        logging.info(f'{basisbp}')
        if self.current_timestamp // 1e3 != timestamp // 1e3:
            self.rolling_window_basis_sec[self.rolling_window_idx] = basisbp
            self.rolling_window_idx += 1
            if self.rolling_window_idx == len(self.rolling_window_basis_sec):
                self.rolling_window_is_full = True
                self.rolling_window_idx = 0
        self.current_timestamp = timestamp
        self.current_basisbp = basisbp
        if not self.rolling_window_is_full:
            return torch.zeros(self.order_size, 0)

        positions = positions + self.fake_position
        pos_long = positions[self.long_init_pos] + positions[self.long_buy] - positions[self.long_sell]
        pos_short = positions[self.short_init_pos] + positions[self.short_sell] - positions[self.short_buy]

        short_open_orders = ask_orders[:, ask_orders[self.side] == 1] # 开空
        long_open_orders = bid_orders[:, bid_orders[self.side] == 0] # 开多

        if pos_long[0] != 0 or (long_open_orders[self.instrument_id] == 0).sum() != 0 or \
            pos_short[1] != 0 or (short_open_orders[self.instrument_id] == 1).sum() != 0:
            self.current_dir = 1
            orders = self.status_trading(mds, positions, ask_orders, bid_orders)
        elif pos_long[1] != 0 or (long_open_orders[self.instrument_id] == 1).sum() != 0 or \
            pos_short[0] != 0 or (short_open_orders[self.instrument_id] == 0).sum() != 0:
            self.current_dir = -1
            orders = self.status_trading(mds, positions, ask_orders, bid_orders)
        else:
            self.current_dir = 0
            orders = self.status_empty(mds)
        if orders.shape[1] != 0:
            orders = self._filter_invalid_orders(positions, orders)
        if orders.shape[1] != 0:
            orders = self.filter_fake_trade(orders)
        return orders
