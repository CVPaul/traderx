#!/usr/bin/env python
#-*- coding:utf-8 -*-


import torch


def tick2md(stg, tick, md): 
    md[stg.bid_price] = tick['b']
    md[stg.ask_price] = tick['a']
    md[stg.bid_volume] = tick['B']
    md[std.ask_volume] = tick['A']
    return tick['E']
