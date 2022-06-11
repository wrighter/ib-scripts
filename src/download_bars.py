#!/usr/bin/env python

import os
import sys
import argparse
import logging
from datetime import datetime, timedelta
from threading import Thread
from queue import Queue

from typing import List, Optional
from collections import defaultdict
from dateutil.parser import parse

import numpy as np
import pandas as pd

import ibapi
from ibapi.common import TickerId, BarData
from ibapi.client import EClient
from ibapi.contract import Contract
from ibapi.utils import iswrapper

ContractList = List[Contract]
BarDataList = List[BarData]
OptionalDate = Optional[datetime]


def make_download_path(args: argparse.Namespace, contract: Contract) -> str:
    """Make path for saving csv files.
    Files to be stored in base_directory/<security_type>/<size>/<symbol>/
    """
    path = os.path.sep.join(
        [
            args.base_directory,
            args.security_type,
            args.size.replace(" ", "_"),
            contract.symbol,
        ]
    )
    return path


class DownloadApp(EClient, ibapi.wrapper.EWrapper):
    def __init__(self, contracts: ContractList, args: argparse.Namespace):
        EClient.__init__(self, wrapper=self)
        ibapi.wrapper.EWrapper.__init__(self)
        self.request_id = 0
        self.started = False
        self.next_valid_order_id = None
        self.contracts = contracts
        self.requests = {}
        self.bar_data = defaultdict(list)
        self.pending_ends = set()
        self.args = args
        self.current = self.args.end_date
        self.duration = self.args.duration
        self.useRTH = args.useRTH
        self.queue = Queue()

    def send_done(self, code):
        logging.info("Sending code %s", code)
        self.queue.put(code)

    def wait_done(self):
        logging.info("Waiting for thread to finish ...")
        code = self.queue.get()
        logging.info("Received code %s", code)
        self.queue.task_done()
        return code

    def next_request_id(self, contract: Contract) -> int:
        self.request_id += 1
        self.requests[self.request_id] = contract
        return self.request_id

    def next_start_time(self) -> datetime:
        return (self.current - timedelta(days=1)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )

    def historicalDataRequest(self, contract: Contract) -> None:
        cid = self.next_request_id(contract)
        self.pending_ends.add(cid)

        self.reqHistoricalData(
            cid,                    # tickerId, used to identify incoming data
            contract,
            self.current.strftime("%Y%m%d %H:%M:%S"),
            self.duration,          # amount of time to go back
            self.args.size,         # bar size
            self.args.data_type,    # historical data type
            self.useRTH,            # useRTH (regular trading hours)
            1,                      # format the date in yyyyMMdd HH:mm:ss
            False,                  # keep up to date after snapshot
            [],                     # chart options
        )

    def save_data(self, contract: Contract, bars: BarDataList) -> None:
        if ibapi.__version__ > "9":
            data = [
                [b.date, b.open, b.high, b.low, b.close, b.volume, b.barCount, b.average]
                for b in bars
            ]
            df = pd.DataFrame(
                data,
                columns=[
                    "date",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "barCount",
                    "average",
                ],
            )
        else:
            data = [
                [b.date, b.open, b.high, b.low, b.close, b.volume, b.barCount, b.wap]
                for b in bars
            ]
            df = pd.DataFrame(
                data,
                columns=[
                    "date",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "barCount",
                    "wap",
                ],
            )
        if self.daily_files():
            path = "%s.csv" % make_download_path(self.args, contract)
        else:
            # since we fetched data until midnight, store data in
            # date file to which it belongs
            last = (self.current - timedelta(days=1)).strftime("%Y%m%d")
            path = os.path.sep.join(
                [make_download_path(self.args, contract), "%s.csv" % last,]
            )
        df.to_csv(path, index=False)

    def daily_files(self):
        return SIZES.index(self.args.size.split()[1]) >= 5

    @iswrapper
    def headTimestamp(self, reqId: int, headTimestamp: str) -> None:
        contract = self.requests.get(reqId)
        ts = datetime.strptime(headTimestamp, "%Y%m%d  %H:%M:%S")
        logging.info("Head Timestamp for %s is %s", contract, ts)
        if ts > self.args.start_date or self.args.max_days:
            logging.warning("Overriding start date, setting to %s", ts)
            self.args.start_date = ts  # TODO make this per contract
        if ts > self.args.end_date:
            logging.warning("Data for %s is not available before %s", contract, ts)
            self.send_done(-1)
            return
        # if we are getting daily data or longer, we'll grab the entire amount at once
        if self.daily_files():
            days = (self.args.end_date - self.args.start_date).days
            if days < 365:
                self.duration = "%d D" % days
            else:
                self.duration = "%d Y" % np.ceil(days / 365)
            # when getting daily data, look at regular trading hours only
            # to get accurate daily closing prices
            self.useRTH = True
            # round up current time to midnight for even days
            self.current = self.current.replace(
                hour=0, minute=0, second=0, microsecond=0
            )

        self.historicalDataRequest(contract)

    @iswrapper
    def historicalData(self, reqId: int, bar) -> None:
        self.bar_data[reqId].append(bar)

    @iswrapper
    def historicalDataEnd(self, reqId: int, start: str, end: str) -> None:
        super().historicalDataEnd(reqId, start, end)
        logging.info("historicalDataEnd: %s %s %s", reqId, start, end)
        self.handle_end(reqId, datetime.strptime(start, "%Y%m%d  %H:%M:%S"))
    
    def handle_end(self, reqId: int, start: str) -> None:
        self.pending_ends.remove(reqId)
        if len(self.pending_ends) == 0:
            logging.info("All requests for %s complete.", self.current)
            for rid, bars in self.bar_data.items():
                self.save_data(self.requests[rid], bars)
            self.current = start            
            if self.current <= self.args.start_date:
                self.send_done(0)
            else:
                for contract in self.contracts:
                    self.historicalDataRequest(contract)

    @iswrapper
    def connectAck(self):
        logging.info("Connected")

    @iswrapper
    def nextValidId(self, order_id: int):
        super().nextValidId(order_id)

        self.next_valid_order_id = order_id
        logging.info(f"nextValidId: {order_id}")
        # we can start now
        self.start()

    def start(self):
        if self.started:
            return

        self.started = True
        for contract in self.contracts:
            self.reqHeadTimeStamp(
                self.next_request_id(contract), contract, self.args.data_type, 0, 1
            )

    @iswrapper
    def error(self, req_id: TickerId, error_code: int, error: str, advancedOrderRejectJson: str=""):
        super().error(req_id, error_code, error)
        if req_id < 0:
            # we get error logs that really are just info
            logging.debug("Error. Id: %s Code %s Msg: %s", req_id, error_code, error)
        else:
            logging.error("Error. Id: %s Code %s Msg: %s", req_id, error_code, error)
            if error_code == 162: # no data returned, keep going
                self.handle_end(req_id, self.next_start_time())
            else:
                # we will always exit on error since data will need to be validated
                self.done = True


def make_contract(symbol: str, sec_type: str, currency: str, exchange: str, localsymbol: str) -> Contract:
    contract = Contract()
    contract.symbol = symbol
    contract.secType = sec_type
    contract.currency = currency
    contract.exchange = exchange
    if localsymbol:
        contract.localSymbol = localsymbol
    return contract


class ValidationException(Exception):
    pass


def _validate_in(value: str, name: str, valid: List[str]) -> None:
    if value not in valid:
        raise ValidationException(f"{value} not a valid {name} unit: {','.join(valid)}")


def _validate(value: str, name: str, valid: List[str]) -> None:
    tokens = value.split()
    if len(tokens) != 2:
        raise ValidationException("{name} should be in the form <digit> <{name}>")
    _validate_in(tokens[1], name, valid)
    try:
        int(tokens[0])
    except ValueError as ve:
        raise ValidationException(f"{name} dimenion not a valid number: {ve}")


SIZES = ["secs", "min", "mins", "hour", "hours", "day", "week", "month"]
DURATIONS = ["S", "D", "W", "M", "Y"]


def validate_duration(duration: str) -> None:
    _validate(duration, "duration", DURATIONS)


def validate_size(size: str) -> None:
    _validate(size, "size", SIZES)


def validate_data_type(data_type: str) -> None:
    _validate_in(
        data_type,
        "data_type",
        [
            "TRADES",
            "MIDPOINT",
            "BID",
            "ASK",
            "BID_ASK",
            "ADJUSTED_LAST",
            "HISTORICAL_VOLATILITY",
            "OPTION_IMPLIED_VOLATILITY",
            "REBATE_RATE",
            "FEE_RATE",
            "YIELD_BID",
            "YIELD_ASK",
            "YIELD_BID_ASK",
            "YIELD_LAST",
        ],
    )


def main():

    now = datetime.now()

    class DateAction(argparse.Action):
        """Parses date strings."""

        def __call__(
            self,
            parser: argparse.ArgumentParser,
            namespace: argparse.Namespace,
            value: str,
            option_string: str = None,
        ):
            """Parse the date."""
            setattr(namespace, self.dest, parse(value))

    argp = argparse.ArgumentParser("""
    Downloader for Interactive Brokers bar data. Using TWS API, will download
    historical instrument data and place csv files in a specified directory.
    Handles basic errors and reports issues with data that it finds.

    Examples:
    Get the continuous 1 minute bars for the E-mini future from GLOBEX
        ./download_bars.py --security-type CONTFUT --start-date 20191201 --end-date 20191228 --exchange GLOBEX ES

    Get 1 minute bars for US Equity AMGN for a few days
        ./download_bars.py --size "1 min" --start-date 20200202 --end-date 20200207 AMGN
    """)
    argp.add_argument("symbol", nargs="+")
    argp.add_argument(
        "-d", "--debug", action="store_true", help="turn on debug logging"
    )
    argp.add_argument(
        "--logfile", help="log to file"
    )
    argp.add_argument(
        "-p", "--port", type=int, default=7496, help="local port for TWS connection"
    )
    argp.add_argument("--size", type=str, default="1 min", help="bar size")
    argp.add_argument("--duration", type=str, default="1 D", help="bar duration")
    argp.add_argument(
        "-t", "--data-type", type=str, default="TRADES", help="bar data type"
    )
    argp.add_argument(
        "--base-directory",
        type=str,
        default="data",
        help="base directory to write bar files",
    )
    argp.add_argument(
        "--currency", type=str, default="USD", help="currency for symbols"
    )
    argp.add_argument(
        "--exchange", type=str, default="SMART", help="exchange for symbols"
    )
    argp.add_argument(
        "--localsymbol", type=str, default="", help="local symbol (for futures)"
    )
    argp.add_argument(
        "--security-type", type=str, default="STK", help="security type for symbols"
    )
    argp.add_argument(
        "--useRTH", action="store_true", help="use Regular Trading Hours"
    )
    argp.add_argument(
        "--start-date",
        help="First day for bars",
        default=now - timedelta(days=2),
        action=DateAction,
    )
    argp.add_argument(
        "--end-date", help="Last day for bars", default=now, action=DateAction,
    )
    argp.add_argument(
        "--max-days", help="Set start date to earliest date", action="store_true",
    )
    args = argp.parse_args()

    logargs = dict(format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                   datefmt='%H:%M:%S')
    if args.debug:
        logargs['level'] = logging.DEBUG
    else:
        logargs['level'] = logging.INFO

    if args.logfile:
        logargs['filemode'] = 'a'
        logargs['filename'] = args.logfile

    logging.basicConfig(**logargs)

    try:
        validate_duration(args.duration)
        validate_size(args.size)
        args.data_type = args.data_type.upper()
        validate_data_type(args.data_type)
    except ValidationException as ve:
        logging.error(ve)
        sys.exit(1)

    logging.debug(f"args={args}")
    contracts = []
    for s in args.symbol:
        contract = make_contract(s, args.security_type, args.currency, args.exchange, args.localsymbol)
        contracts.append(contract)
        os.makedirs(make_download_path(args, contract), exist_ok=True)
    app = DownloadApp(contracts, args)
    app.connect("127.0.0.1", args.port, clientId=0)
    Thread(target=app.run).start()

    code = app.wait_done()
    app.disconnect()

    return code


if __name__ == "__main__":
    main()
