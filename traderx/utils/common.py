#!/usr/bin/env python
#-*- coding:utf-8 -*-


import random
import string

from binance.cm_futures import CMFutures
from binance.um_futures import UMFutures


class FakeClient:

    def __init__(self, key=None, sceret=None, **kw):
        pass

    def new_order(self, **kw):
        pass # do nothing

    def cancel_order(self, **kw):
        pass # do nothing

    def new_listen_key(self):
        return {'listenKey': 'in-test-mode'}

    def renew_listen_key(self, listenKey):
        pass

    def close_listen_key(self, listenKey):
        pass

    def get_open_orders(self, symbol):
        return []


def uuid(length=4):
    return ''.join(random.choices(
        string.ascii_lowercase + string.digits, k=length))


def create_rest_api(key=None, secret=None, type_='um', is_online=False):
    if not is_online:
        return FakeClient()
    if type_ == 'um':
        cli = UMFutures(key=key, secret=secret)
    elif type_ == 'cm':
        cli = CMFutures(key=key, secret=secret)
    else:
        raise RuntimeError(f"unsupported type got:{type_}, only `cm`, `um` were allowed!")
    return cli


if __name__ == "__main__":
    print(uuid())
