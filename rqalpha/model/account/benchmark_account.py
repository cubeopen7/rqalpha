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
import numpy as np

from .base_account import BaseAccount
from ..dividend import Dividend
from ...execution_context import ExecutionContext
from ...const import ACCOUNT_TYPE


class BenchmarkAccount(BaseAccount):
    def __init__(self, env, init_cash, start_date):
        super(BenchmarkAccount, self).__init__(env, init_cash, start_date, ACCOUNT_TYPE.BENCHMARK)
        self.benchmark = env.config.base.benchmark

    def before_trading(self):  # benchmark的portfolio资金更新, 分红资金并入现金
        portfolio = self.portfolio  # 获取benchmark的portfolio
        portfolio._yesterday_portfolio_value = portfolio.portfolio_value  # 昨日资产更替
        trading_date = ExecutionContext.get_current_trading_dt().date()
        self._handle_dividend_payable(trading_date)  # 执行分红更正

    def bar(self, bar_dict):  # 更新基准仓位信息
        price = bar_dict[self.config.base.benchmark].close  # 基准最新收盘价
        if np.isnan(price):
            return
        portfolio = self.portfolio  # 基准组合信息
        portfolio._portfolio_value = None
        position = portfolio.positions[self.benchmark]  # 基准仓位信息

        if portfolio.market_value == 0:
            trade_quantity = int(portfolio.cash / price)  # 可买入股数
            delta_value = trade_quantity * price  # 买入上面的股数需要的资金
            commission = 0.0008 * trade_quantity * price  # 交易产生的佣金
            position._total_commission = commission  # 记录佣金
            position._buy_trade_quantity = trade_quantity  # 记录买入股数
            position._buy_trade_value = delta_value  # 记录买入资金
            position._market_value = delta_value  # 记录基准市值
            portfolio._cash = portfolio._cash - delta_value - commission  # 更新组合现金
        else:
            position._market_value = position._buy_trade_quantity * price

    def after_trading(self):
        trading_date = ExecutionContext.get_current_trading_dt().date()
        self.portfolio_persist()  # 将今天的portfolio组合信息保存
        self._handle_dividend_ex_dividend(trading_date)  # 对今日仓内股票进行分红检查

    def _handle_dividend_payable(self, trading_date):  # 每日before_trading时, 处理benchmark分红数据, 将分红资金并入现金
        """handle dividend payable before trading
        """
        to_delete_dividend = []
        for order_book_id, dividend_info in six.iteritems(self.portfolio._dividend_info):
            dividend_series_dict = dividend_info.dividend_series_dict  # 分红数据字典

            if pd.Timestamp(trading_date) == pd.Timestamp(dividend_series_dict['payable_date']):
                dividend_per_share = dividend_series_dict["dividend_cash_before_tax"] / dividend_series_dict["round_lot"]  # 每股分红金额
                if dividend_per_share > 0 and dividend_info.quantity > 0:
                    dividend_cash = dividend_per_share * dividend_info.quantity  # 本次分红总金额
                    self.portfolio._dividend_receivable -= dividend_cash  # 分红未处理金额划去此标的分红金额
                    self.portfolio._cash += dividend_cash  # benchmark现金加入此次现金分红金额
                    # user_log.info(_("get dividend {dividend} for {order_book_id}").format(
                    #     dividend=dividend_cash,
                    #     order_book_id=order_book_id,
                    # ))
                    to_delete_dividend.append(order_book_id)  # 处理过的分红数据从_dividend_info中删除

        for order_book_id in to_delete_dividend:
            self.portfolio._dividend_info.pop(order_book_id, None)  # _dividend_info中删除处理过的分红数据

    def _handle_dividend_ex_dividend(self, trading_date):  # 对今日仓内股票进行分红检查
        data_proxy = ExecutionContext.get_data_proxy()
        for order_book_id, position in six.iteritems(self.portfolio.positions):
            dividend_series = data_proxy.get_dividend_by_book_date(order_book_id, trading_date)
            if dividend_series is None:
                continue

            dividend_series_dict = {
                'book_closure_date': dividend_series['book_closure_date'],
                'ex_dividend_date': dividend_series['ex_dividend_date'],
                'payable_date': dividend_series['payable_date'],
                'dividend_cash_before_tax': dividend_series['dividend_cash_before_tax'],
                'round_lot': dividend_series['round_lot']
            }

            dividend_per_share = dividend_series_dict["dividend_cash_before_tax"] / dividend_series_dict["round_lot"]
            self.portfolio._dividend_info[order_book_id] = Dividend(order_book_id, position._quantity, dividend_series_dict)
            self.portfolio._dividend_receivable += dividend_per_share * position._quantity
