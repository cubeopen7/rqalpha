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

import sys
import inspect
import datetime
import six
import pandas as pd
from functools import wraps

from dateutil.parser import parse as parse_date

from .exception import RQInvalidArgument, RQTypeError
from ..execution_context import ExecutionContext
from ..model.instrument import Instrument
from ..environment import Environment
from ..const import INSTRUMENT_TYPE, RUN_TYPE
from ..utils import unwrapper, INST_TYPE_IN_STOCK_ACCOUNT
from ..utils.i18n import gettext as _
from ..utils.logger import user_system_log


main_contract_warning_flag = True
index_contract_warning_flag = True


class ArgumentChecker(object):
    def __init__(self, arg_name):
        self._arg_name = arg_name
        self._rules = []

    def is_instance_of(self, types):
        def check_is_instance_of(func_name, value):
            if not isinstance(value, types):
                raise RQInvalidArgument(
                    _('function {}: invalid {} argument, expect a value of type {}, got {} (type: {})').format(
                        func_name, self._arg_name, types, value, type(value)
                    ))

        self._rules.append(check_is_instance_of)
        return self

    def raise_not_valid_instrument_error(self, func_name, arg_name, value):
        raise RQInvalidArgument(
            _('function {}: invalid {} argument, expect a valid instrument/order_book_id/symbol, '
              'got {} (type: {})').format(
                func_name, self._arg_name, value, type(value)
            ))

    def raise_not_valid_stock_error(self, func_name, arg_name, value):
        raise RQInvalidArgument(
            _('function {}: invalid {} argument, expect a valid stock instrument/order_book_id/symbol, '
              'got {} (type: {})').format(
                func_name, self._arg_name, value, type(value)
            ))

    def raise_not_valid_future_error(self, func_name, arg_name, value):
        raise RQInvalidArgument(
            _('function {}: invalid {} argument, expect a valid future instrument/order_book_id/symbol, '
              'got {} (type: {})').format(
                func_name, self._arg_name, value, type(value)
            ))

    def _is_valid_instrument(self, func_name, value):
        config = Environment.get_instance().config
        global index_contract_warning_flag
        if isinstance(value, six.string_types):
            if config.base.run_type == RUN_TYPE.PAPER_TRADING:
                if "88" in value:
                    raise RQInvalidArgument(_("Main Future contracts[88] are not supported in paper trading."))
                if "99" in value:
                    raise RQInvalidArgument(_("Index Future contracts[99] are not supported in paper trading."))
            else:
                if "88" in value:
                    global main_contract_warning_flag
                    if main_contract_warning_flag:
                        main_contract_warning_flag = False
                        user_system_log.warn(_("Main Future contracts[88] are not supported in paper trading."))
                if "99" in value:
                    global index_contract_warning_flag
                    if index_contract_warning_flag:
                        index_contract_warning_flag = False
                        user_system_log.warn(_("Index Future contracts[99] are not supported in paper trading."))
            instrument = ExecutionContext.get_data_proxy().instruments(value)
            if instrument is None:
                self.raise_not_valid_instrument_error(func_name, self._arg_name, value)
            return

        if isinstance(value, Instrument):
            return

        self.raise_not_valid_instrument_error(func_name, self._arg_name, value)

    def is_valid_instrument(self):
        self._rules.append(self._is_valid_instrument)
        return self

    def _is_valid_stock(self, func_name, value):
        if isinstance(value, six.string_types):
            instrument = ExecutionContext.get_data_proxy().instruments(value)
            if instrument is None:
                self.raise_not_valid_instrument_error(func_name, self._arg_name, value)
            if instrument.enum_type not in INST_TYPE_IN_STOCK_ACCOUNT:
                self.raise_not_valid_stock_error(func_name, self._arg_name, value)
            return

        if isinstance(value, Instrument):
            if value.enum_type not in INST_TYPE_IN_STOCK_ACCOUNT:
                self.raise_not_valid_stock_error(func_name, self._arg_name, value)
            else:
                return

        self.raise_not_valid_instrument_error(func_name, self._arg_name, value)

    def is_valid_stock(self):
        self._rules.append(self._is_valid_stock)
        return self

    def _is_valid_future(self, func_name, value):
        if isinstance(value, six.string_types):
            instrument = ExecutionContext.get_data_proxy().instruments(value)
            if instrument is None:
                self.raise_not_valid_instrument_error(func_name, self._arg_name, value)
            if instrument.enum_type != INSTRUMENT_TYPE.FUTURE:
                self.raise_not_valid_future_error(func_name, self._arg_name, value)
            return
        if isinstance(value, Instrument):
            if value.enum_type != INSTRUMENT_TYPE.FUTURE:
                self.raise_not_valid_future_error(func_name, self._arg_name, value)
            else:
                return
        self.raise_not_valid_instrument_error(func_name, self._arg_name, value)

    def is_valid_future(self):
        self._rules.append(self._is_valid_future)
        return self

    def _is_number(self, func_name, value):
        try:
            v = float(value)
        except ValueError:
            raise RQInvalidArgument(
                _('function {}: invalid {} argument, expect a number, got {} (type: {})').format(
                    func_name, self._arg_name, value, type(value))
            )

    def is_number(self):
        self._rules.append(self._is_number)
        return self

    def is_in(self, valid_values, ignore_none=True):
        def check_is_in(func_name, value):
            if ignore_none and value is None:
                return

            if value not in valid_values:
                raise RQInvalidArgument(
                    _('function {}: invalid {} argument, valid: {}, got {} (type: {})').format(
                        func_name, self._arg_name, repr(valid_values), value, type(value))
                )
            return

        self._rules.append(check_is_in)
        return self

    def are_valid_fields(self, valid_fields, ignore_none=True):
        valid_fields = set(valid_fields)

        def check_are_valid_fields(func_name, fields):
            if isinstance(fields, six.string_types):
                if fields not in valid_fields:
                    raise RQInvalidArgument(
                        _('function {}: invalid {} argument, valid fields are {}, got {} (type: {})').format(
                            func_name, self._arg_name, repr(valid_fields), fields, type(fields)
                        ))
                return

            if fields is None and ignore_none:
                return

            if isinstance(fields, list):
                invalid_fields = [field for field in fields if field not in valid_fields]
                if invalid_fields:
                    raise RQInvalidArgument(
                        _('function {}: invalid field {}, valid fields are {}, got {} (type: {})').format(
                            func_name, invalid_fields, repr(valid_fields), fields, type(fields)
                        ))

            raise RQInvalidArgument(
                _('function {}: invalid {} argument, expect a string or a list of string, got {} (type: {})').format(
                    func_name, self._arg_name, repr(fields), type(fields)
                ))
        self._rules.append(check_are_valid_fields)
        return self

    def _are_valid_instruments(self, func_name, values):
        if isinstance(values, (six.string_types, Instrument)):
            self._is_valid_instrument(func_name, values)
            return

        if isinstance(values, list):
            for v in values:
                self._is_valid_instrument(func_name, v)
            return

        raise RQInvalidArgument(
            _('function {}: invalid {} argument, expect a string or a list of string, got {} (type: {})').format(
                func_name, self._arg_name, repr(values), type(values)
            ))

    def are_valid_instruments(self):
        self._rules.append(self._are_valid_instruments)
        return self

    def is_valid_date(self, ignore_none=True):
        def check_is_valid_date(func_name, value):
            if ignore_none and value is None:
                return None
            if isinstance(value, (datetime.date, pd.Timestamp)):
                return
            if isinstance(value, six.string_types):
                try:
                    v = parse_date(value)
                except ValueError:
                    raise RQInvalidArgument(
                        _('function {}: invalid {} argument, expect a valid date, got {} (type: {})').format(
                            func_name, self._arg_name, value, type(value)
                        ))

            raise RQInvalidArgument(
                _('function {}: invalid {} argument, expect a valid date, got {} (type: {})').format(
                    func_name, self._arg_name, value, type(value)
                ))

        self._rules.append(check_is_valid_date)
        return self

    def is_greater_than(self, low):
        def check_greater_than(func_name, value):
            if value <= low:
                raise RQInvalidArgument(
                    _('function {}: invalid {} argument, expect a value > {}, got {} (type: {})').format(
                        func_name, self._arg_name, low, value, type(value)
                    ))
        self._rules.append(check_greater_than)
        return self

    def is_less_than(self, high):
        def check_less_than(func_name, value):
            if value >= high:
                raise RQInvalidArgument(
                    _('function {}: invalid {} argument, expect a value < {}, got {} (type: {})').format(
                        func_name, self._arg_name, high, value, type(value)
                    ))

        self._rules.append(check_less_than)
        return self

    def _is_valid_interval(self, func_name, value):
        valid = isinstance(value, six.string_types) and value[-1] in {'d', 'm', 'q', 'y'}
        if valid:
            try:
                valid = int(value[:-1]) > 0
            except ValueError:
                valid = False

        if not valid:
            raise RQInvalidArgument(
                _("function {}: invalid {} argument, interval should be in form of '1d', '3m', '4q', '2y', "
                  "got {} (type: {})").format(
                    func_name, self.arg_name, value, type(value)
                ))

    def is_valid_interval(self):
        self._rules.append(self._is_valid_interval)
        return self

    def _are_valid_query_entities(self, func_name, entities):
        from sqlalchemy.orm.attributes import InstrumentedAttribute
        for e in entities:
            if not isinstance(e, InstrumentedAttribute):
                raise RQInvalidArgument(
                    _("function {}: invalid {} argument, should be entity like "
                      "Fundamentals.balance_sheet.total_equity, got {} (type: {})").format(
                        func_name, self.arg_name, e, type(e)
                    ))

    def are_valid_query_entities(self):
        self._rules.append(self._are_valid_query_entities)
        return self

    def _is_valid_frequency(self, func_name, value):
        valid = isinstance(value, six.string_types) and value[-1] in ('d', 'm')
        if valid:
            try:
                valid = int(value[:-1]) > 0
            except ValueError:
                valid = False

        if not valid:
            raise RQInvalidArgument(
                _("function {}: invalid {} argument, frequency should be in form of "
                  "'1m', '5m', '1d', got {} (type: {})").format(
                    func_name, self.arg_name, value, type(value)
                ))

    def is_valid_frequency(self):
        self._rules.append(self._is_valid_frequency)
        return self

    def verify(self, func_name, value):
        for r in self._rules:
            r(func_name, value)

    @property
    def arg_name(self):
        return self._arg_name


def verify_that(arg_name):
    return ArgumentChecker(arg_name)


def apply_rules(*rules):  # 检查函数输入规则
    def decorator(func):
        @wraps(func)
        def api_rule_check_wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except RQInvalidArgument:
                raise
            except Exception:
                exc_info = sys.exc_info()
                t, v, tb = exc_info

                try:
                    call_args = inspect.getcallargs(unwrapper(func), *args, **kwargs)
                except TypeError as e:
                    raise RQTypeError(*e.args).with_traceback(tb)

                try:
                    for r in rules:
                        r.verify(func.__name__, call_args[r.arg_name])
                except RQInvalidArgument as e:
                    raise e.with_traceback(tb)

                raise

        api_rule_check_wrapper._rq_exception_checked = True
        return api_rule_check_wrapper

    return decorator
