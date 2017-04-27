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

import six
import pandas as pd

from .base_account import BaseAccount
from ..dividend import Dividend
from ...const import SIDE, ACCOUNT_TYPE
from ...utils.i18n import gettext as _
from ...utils.logger import user_system_log, system_log
from ...execution_context import ExecutionContext


class StockAccount(BaseAccount):
    def __init__(self, env, init_cash, start_date):
        super(StockAccount, self).__init__(env, init_cash, start_date, ACCOUNT_TYPE.STOCK)

    def before_trading(self):  # 开盘前股票账户处理, 将0股数的股票移除仓位, 并处理现金分红数据, 将分红资金并入现金
        super(StockAccount, self).before_trading()  # 调用上级before_trading
        positions = self.portfolio.positions
        removing_ids = []
        for order_book_id in positions.keys():
            position = positions[order_book_id]
            if position._quantity == 0:
                removing_ids.append(order_book_id)  # 将股数为0的标的移出positions
        for order_book_id in removing_ids:
            positions.pop(order_book_id, None)
        trading_date = ExecutionContext.get_current_trading_dt().date()
        self._handle_dividend_payable(trading_date)  # 处理现金分红入现金
        if self.config.base.handle_split:
            self._handle_split(trading_date)

    def after_trading(self):
        trading_date = ExecutionContext.get_current_trading_dt().date()
        portfolio = self.portfolio
        # de_listed may occur
        portfolio._portfolio_value = None

        positions = portfolio.positions

        de_listed_id_list = []
        # update buy_today_holding_quantity to zero for T+1
        for order_book_id in positions:
            position = positions[order_book_id]
            position._buy_today_holding_quantity = 0  # 清除今天买入的股数

            # 检查股票今天是否退市，如果退市，则按照close_price卖出，并warning
            if position._de_listed_date is not None and trading_date >= position._de_listed_date.date():
                de_listed_id_list.append(order_book_id)

        for de_listed_id in de_listed_id_list:
            position = positions[de_listed_id]
            if self.config.validator.cash_return_by_stock_delisted:
                portfolio._cash += position.market_value
            if position._quantity != 0:
                user_system_log.warn(
                    _("{order_book_id} is expired, close all positions by system").format(order_book_id=de_listed_id))
            del positions[de_listed_id]

    def settlement(self):  # 股票账户结算
        portfolio = self.portfolio
        trading_date = ExecutionContext.get_current_trading_dt().date()
        self.portfolio_persist()  # StockAccount中存储今天的PORTFOLIO
        portfolio._yesterday_portfolio_value = portfolio.portfolio_value  # 用今天的组合资金更新昨天的组合资金
        self._handle_dividend_ex_dividend(trading_date)  # 处理今天仓位的分红

    def bar(self, bar_dict):  # 更新仓位每支股票的仓位情况
        portfolio = self.portfolio  # 股票账户组合信息
        # invalidate cache
        portfolio._portfolio_value = None
        positions = portfolio.positions

        for order_book_id, position in six.iteritems(positions):  # 更新仓位中每支股票的信息
            bar = bar_dict[order_book_id]
            if not bar.isnan:
                position._market_value = position._quantity * bar.close  # 更新市值信息
                position._last_price = bar.close  # 更新最新价格

    def tick(self, tick):
        portfolio = self.portfolio
        # invalidate cache
        portfolio._portfolio_value = None
        position = portfolio.positions[tick.order_book_id]

        position._market_value = position._quantity * tick.last_price
        position._last_price = tick.last_price

    def order_pending_new(self, account, order):  # 模拟下单
        if self != account:
            return
        if order._is_final():  # 检查订单状态是否结束
            return
        order_book_id = order.order_book_id
        position = self.portfolio.positions[order_book_id]
        position._total_orders += 1  # 该股票仓位委托单计数递增
        create_quantity = order.quantity  # 订单股数
        create_value = order._frozen_price * create_quantity  # 订单市值

        self._update_order_data(order, create_quantity, create_value)  # 更新股票对应仓位的字段
        self._update_frozen_cash(order, create_value)  # 冻结买入股票的资金

    def order_creation_pass(self, account, order):
        pass

    def order_creation_reject(self, account, order):
        if self != account:
            return
        order_book_id = order.order_book_id
        position = self.portfolio.positions[order_book_id]
        position._total_orders -= 1
        cancel_quantity = order.unfilled_quantity
        cancel_value = order._frozen_price * cancel_quantity
        self._update_order_data(order, cancel_quantity, cancel_value)
        self._update_frozen_cash(order, -cancel_value)

    def order_pending_cancel(self, account, order):
        pass

    def order_cancellation_pass(self, account, order):
        self._cancel_order_cal(account, order)

    def order_cancellation_reject(self, account, order):
        pass

    def order_unsolicited_update(self, account, order):
        self._cancel_order_cal(account, order)

    def _cancel_order_cal(self, account, order):
        if self != account:
            return
        rejected_quantity = order.unfilled_quantity
        rejected_value = order._frozen_price * rejected_quantity
        self._update_order_data(order, -rejected_quantity, -rejected_value)
        self._update_frozen_cash(order, -rejected_value)

    def trade(self, account, trade):  # 根据成交单更新仓位, 账户等信息
        if self != account:
            return
        portfolio = self.portfolio
        portfolio._portfolio_value = None
        order = trade.order
        bar_dict = ExecutionContext.get_current_bar_dict()
        order_book_id = order.order_book_id
        position = portfolio.positions[order.order_book_id]
        position._is_traded = True
        trade_quantity = trade.last_quantity  # 成交股数
        minus_value_by_trade = order._frozen_price * trade_quantity  # 成交金额
        trade_value = trade.last_price * trade_quantity  # 计算滑点的成交价格, 真实成交价格

        if order.side == SIDE.BUY:
            position._avg_price = (position._avg_price * position._quantity +
                                   trade_quantity * trade.last_price) / (position._quantity + trade_quantity)  # 更新仓位的平均价格

        self._update_order_data(order, -trade_quantity, -minus_value_by_trade)  # 更新股票Position中的买入数据, 平掉
        self._update_trade_data(order, trade, trade_quantity, trade_value)  # 更新Position中对应成交记录的变量
        self._update_frozen_cash(order, -minus_value_by_trade)  # 解冻资金更新
        price = bar_dict[order_book_id].close
        if order.side == SIDE.BUY and order.order_book_id not in \
                {'510900.XSHG', '513030.XSHG', '513100.XSHG', '513500.XSHG'}:
            position._buy_today_holding_quantity += trade_quantity  # 仓位今日买入股数
        position._market_value = (position._buy_trade_quantity - position._sell_trade_quantity) * price  # 更新仓位市值信息
        position._last_price = price  # 更新仓位最后操作的价格
        position._total_trades += 1  # 仓位成交次数递增

        portfolio._total_tax += trade.tax  # Portfolio总印花税增加
        portfolio._total_commission += trade.commission  # Portfolio总佣金增加
        portfolio._cash = portfolio._cash - trade.tax - trade.commission  # Portfolio现金去除费用
        if order.side == SIDE.BUY:
            portfolio._cash -= trade_value  # 去掉买入成交用掉的现金
        else:
            portfolio._cash += trade_value

        self._last_trade_id = trade.exec_id  # 记录账户最后一条成交记录ID

    def _update_order_data(self, order, inc_order_quantity, inc_order_value):  # 买入\卖出 股票时, 对应仓位的[买入\卖出]股数,金额, 其实是中间变量
        position = self.portfolio.positions[order.order_book_id]
        if order.side == SIDE.BUY:
            position._buy_order_quantity += inc_order_quantity  # 需要买入的股数, 创建订单时累加, 撮合完成后减掉
            position._buy_order_value += inc_order_value  # 需要买入的金额, 创建订单时累加, 撮合完成后减掉
        else:
            position._sell_order_quantity += inc_order_quantity
            position._sell_order_value += inc_order_value

    def _update_trade_data(self, order, trade, trade_quantity, trade_value):  # 更新Position中对应成交记录的变量
        position = self.portfolio.positions[order.order_book_id]
        position._transaction_cost = position._transaction_cost + trade.commission + trade.tax  # 更新仓位的总费用
        if order.side == SIDE.BUY:
            position._buy_trade_quantity += trade_quantity  # 成交撮合总买入股数
            position._buy_trade_value += trade_value  # 成交撮合总买入金额
        else:
            position._sell_trade_quantity += trade_quantity
            position._sell_trade_value += trade_value

    def _update_frozen_cash(self, order, inc_order_value):  # [冻结\解冻]买入股票的资金, 创建订单时冻结资金, 成交撮合后解冻资金, 其实是中间变量
        portfolio = self.portfolio
        if order.side == SIDE.BUY:
            portfolio._frozen_cash += inc_order_value  # 创建订单时冻结资金, 成交撮合后解冻资金
            portfolio._cash -= inc_order_value  # 创建订单时扣除资金

    def _handle_split(self, trading_date):
        import rqdatac
        for order_book_id, position in six.iteritems(self.portfolio.positions):
            split_df = rqdatac.get_split(order_book_id, start_date="2005-01-01", end_date="2099-01-01")
            if split_df is None:
                system_log.warn(_("no split data {}").foramt(order_book_id))
                continue
            try:
                series = split_df.loc[trading_date]
            except KeyError:
                continue

            # 处理拆股

            user_system_log.info(_("split {order_book_id}, {position}").format(
                order_book_id=order_book_id,
                position=position,
            ))

            ratio = series.split_coefficient_to / series.split_coefficient_from
            for key in ["_buy_order_quantity", "_sell_order_quantity", "_buy_trade_quantity", "_sell_trade_quantity"]:
                setattr(position, key, getattr(position, key) * ratio)

            user_system_log.info(_("split {order_book_id}, {position}").format(
                order_book_id=order_book_id,
                position=position,
            ))

            user_system_log.info(_("split {order_book_id}, {series}").format(
                order_book_id=order_book_id,
                series=series,
            ))

    def _handle_dividend_payable(self, trading_date):  # 每日before_trading时, 处理现金分红入现金
        """handle dividend payable before trading
        """
        to_delete_dividend = []
        for order_book_id, dividend_info in six.iteritems(self.portfolio._dividend_info):
            dividend_series_dict = dividend_info.dividend_series_dict

            if pd.Timestamp(trading_date) == pd.Timestamp(dividend_series_dict['payable_date']):
                dividend_per_share = dividend_series_dict["dividend_cash_before_tax"] / dividend_series_dict["round_lot"]
                if dividend_per_share > 0 and dividend_info.quantity > 0:
                    dividend_cash = dividend_per_share * dividend_info.quantity  # 分红金额
                    self.portfolio._dividend_receivable -= dividend_cash  # 分红金额从待收金额划出
                    self.portfolio._cash += dividend_cash  # 分红金额并入现金
                    to_delete_dividend.append(order_book_id)

        for order_book_id in to_delete_dividend:
            self.portfolio._dividend_info.pop(order_book_id, None)  # 将已计算分红的标的, 从_dividend_info中删除

    def _handle_dividend_ex_dividend(self, trading_date):  # 每日after_trading, 处理今天仓位的分红, 将分红信息压入变量存储
        data_proxy = ExecutionContext.get_data_proxy()
        for order_book_id, position in six.iteritems(self.portfolio.positions):
            dividend_series = data_proxy.get_dividend_by_book_date(order_book_id, trading_date)
            if dividend_series is None:
                continue

            dividend_series_dict = {
                'book_closure_date': dividend_series['book_closure_date'].to_pydatetime(),
                'ex_dividend_date': dividend_series['ex_dividend_date'].to_pydatetime(),
                'payable_date': dividend_series['payable_date'].to_pydatetime(),
                'dividend_cash_before_tax': float(dividend_series['dividend_cash_before_tax']),
                'round_lot': int(dividend_series['round_lot'])
            }

            dividend_per_share = dividend_series_dict["dividend_cash_before_tax"] / dividend_series_dict["round_lot"]  # 每股分红x元

            self.portfolio._dividend_info[order_book_id] = Dividend(order_book_id, position._quantity, dividend_series_dict)  # 将本次分红信息存储记录
            self.portfolio._dividend_receivable += dividend_per_share * position._quantity  # 本次总计分红金额
