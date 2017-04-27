# -*- coding: utf-8 -*-
#
# Copyright 2017 Ricequant, Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from collections import defaultdict

from rqalpha.utils.i18n import gettext as _
from rqalpha.const import ORDER_TYPE, SIDE, BAR_STATUS
from rqalpha.model.trade import Trade
from rqalpha.environment import Environment
from rqalpha.events import EVENT

# 撮合机制类型
class Matcher(object):
    def __init__(self,
                 deal_price_decider,
                 bar_limit=True,
                 volume_percent=0.25):
        self._board = None  # 所有标的的一个bar的数据, bar_dict
        self._turnover = defaultdict(int)  # 今天本策略已经买的股数记录
        self._calendar_dt = None
        self._trading_dt = None
        self._deal_price_decider = deal_price_decider # 交易价格 获取方法
        self._volume_percent = volume_percent # 成交量占当前bar总量的上限, 默认25%, 即要操作的量超过该bar总量的25%, 不撮合
        self._bar_limit = bar_limit # 有没有日内显示(是否是T+1), 股票/期货有区别

    def update(self, calendar_dt, trading_dt, bar_dict):
        self._board = bar_dict
        self._turnover.clear()
        self._calendar_dt = calendar_dt
        self._trading_dt = trading_dt

    def match(self, open_orders):  # 撮合订单
        for account, order in open_orders:
            slippage_decider = account.slippage_decider  # 获取账户的滑点
            commission_decider = account.commission_decider  # 获取账户的佣金
            tax_decider = account.tax_decider  # 获取账户的印花税

            bar = self._board[order.order_book_id]  # 此股今日的BAR
            bar_status = bar._bar_status

            if bar_status == BAR_STATUS.ERROR:
                listed_date = bar.instrument.listed_date.date()
                if listed_date == self._trading_dt.date():
                    reason = _("Order Cancelled: current security [{order_book_id}] can not be traded in listed date [{listed_date}]").format(
                        order_book_id=order.order_book_id,
                        listed_date=listed_date,
                    )
                else:
                    reason = _("Order Cancelled: current bar [{order_book_id}] miss market data.").format(
                        order_book_id=order.order_book_id)
                order._mark_rejected(reason)
                continue

            deal_price = self._deal_price_decider(bar)  # 撮合的价格
            if order.type == ORDER_TYPE.LIMIT:
                if order.price > bar.limit_up:
                    reason = _(
                        "Order Rejected: limit order price {limit_price} is higher than limit up {limit_up}."
                    ).format(
                        limit_price=order.price,
                        limit_up=bar.limit_up
                    )
                    order._mark_rejected(reason)
                    continue

                if order.price < bar.limit_down:
                    reason = _(
                        "Order Rejected: limit order price {limit_price} is lower than limit down {limit_down}."
                    ).format(
                        limit_price=order.price,
                        limit_down=bar.limit_down
                    )
                    order._mark_rejected(reason)
                    continue

                if order.side == SIDE.BUY and order.price < deal_price:
                    continue
                if order.side == SIDE.SELL and order.price > deal_price:
                    continue
            else:  # 市价单
                if self._bar_limit and order.side == SIDE.BUY and bar_status == BAR_STATUS.LIMIT_UP:  # 涨停, 拒买单
                    reason = _(
                        "Order Cancelled: current bar [{order_book_id}] reach the limit_up price."
                    ).format(order_book_id=order.order_book_id)
                    order._mark_rejected(reason)  # 拒单
                    continue
                elif self._bar_limit and order.side == SIDE.SELL and bar_status == BAR_STATUS.LIMIT_DOWN:  # 跌停, 拒卖单
                    reason = _(
                        "Order Cancelled: current bar [{order_book_id}] reach the limit_down price."
                    ).format(order_book_id=order.order_book_id)
                    order._mark_rejected(reason)  # 拒单
                    continue

            if self._bar_limit:
                if order.side == SIDE.BUY and bar_status == BAR_STATUS.LIMIT_UP:  # 涨停, 拒买单
                    continue
                if order.side == SIDE.SELL and bar_status == BAR_STATUS.LIMIT_DOWN:  # 跌停, 拒卖单
                    continue

            volume_limit = round(bar.volume * self._volume_percent) - self._turnover[order.order_book_id]  # 可操作的股数的上限
            round_lot = bar.instrument.round_lot  # 操作单位股数
            volume_limit = (volume_limit // round_lot) * round_lot  # 规整后的可操作的股数的上限
            if volume_limit <= 0:  # 标的成交量不符合该订单的需求量
                if order.type == ORDER_TYPE.MARKET:
                    reason = _('Order Cancelled: market order {order_book_id} volume {order_volume}'
                               ' due to volume limit').format(
                        order_book_id=order.order_book_id,
                        order_volume=order.quantity
                    )
                    order._mark_cancelled(reason)
                continue
            # 到此处, 订单撮合成功, 可以生成成交记录
            unfilled = order.unfilled_quantity  # 订单未成交股数
            fill = min(unfilled, volume_limit)  # 限制成交的股数, 一般不会触发
            ct_amount = account.portfolio.positions[order.order_book_id]._cal_close_today_amount(fill, order.side)
            price = slippage_decider.get_trade_price(order, deal_price)  # 加上滑点, 计算最终的订单价格
            trade = Trade.__from_create__(order=order, calendar_dt=self._calendar_dt, trading_dt=self._trading_dt,
                                          price=price, amount=fill, close_today_amount=ct_amount)  # 生成成交记录
            trade._commission = commission_decider.get_commission(trade)  # 成交记录佣金更新
            trade._tax = tax_decider.get_tax(trade)  # # 成交记录印花税更新
            order._fill(trade)  # 根据成交填补订单, 股数成交完毕则修改订单状态为成交完毕
            self._turnover[order.order_book_id] += fill  # 更新该标的今天成交的股数

            Environment.get_instance().event_bus.publish_event(EVENT.TRADE, account, trade)  # 触发成交后事件, 主要是更新账户的Portfolio, Position等信息

            if order.type == ORDER_TYPE.MARKET and order.unfilled_quantity != 0:
                reason = _(
                    "Order Cancelled: market order {order_book_id} volume {order_volume} is"
                    " larger than 25 percent of current bar volume, fill {filled_volume} actually"
                ).format(
                    order_book_id=order.order_book_id,
                    order_volume=order.quantity,
                    filled_volume=order.filled_quantity
                )
                order._mark_cancelled(reason)
