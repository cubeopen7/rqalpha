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

import pickle

from ..model.instrument import Instrument


class InstrumentStore(object):
    def __init__(self, f):
        with open(f, 'rb') as store:
            d = pickle.load(store)
            '''
            读取到的为标的列表, 元素为dict类型, 包含每个标的的信息
            dict的key值有:
            ---以下为股票|指数共有字段---
            'order_book_id': '000001.XSHE', 标的代码
            'type': 'CS'(股票) | 'INDX'(指数), 标的类型
            'symbol': '中兴通讯', 标的名称
            'listed_date': '1997-11-18', 上市时间
            'de_listed_date': '0000-00-00', 退市时间
            'abbrev_symbol': 'ZXTX', 拼音缩写
            'round_lot': 1.0(指数)/100.0(股票), 最小买卖股数?
            'exchange': 'XSHG'|'XSHE', 交易所
            ---以下为股票特有字段---
            'status': 'Active', 股票状态
            'special_type': 'Normal', 特别类型?
            'board_type': 'MainBoard', 主板|中小板|创业板
            'concept_names': '分拆上市|4G概念|云计算|...', 概念
            'industry_name': '计算机,通信和其他电子设备制造业', 所属行业
            'industry_code': 'C39', 行业代码
            'sector_code': 'InformationTechnology', 行业种类代码(大行业划分)
            'sector_code_name': '信息技术', 行业种类名称(大行业划分)
           '''
        self._instruments = [Instrument(i) for i in d]

    def get_all_instruments(self):
        return self._instruments
