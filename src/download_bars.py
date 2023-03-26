#!/usr/bin/env python

import os
import sys
import argparse
import logging
import pytz

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
from ibapi.contract import Contract, ContractDetails
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
        self.contracts_specs = contracts
        self.contracts = []
        self.requests = {}
        self.bar_data = defaultdict(list)
        self.pending_ends = set()
        self.args = args
        self.current = self.args.end_date
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
            cid,  # tickerId, used to identify incoming data
            contract,
            self.current.strftime(f"%Y%m%d %H:%M:%S {self.args.timezone}"),
            self.duration,  # amount of time to go back
            self.args.size,  # bar size
            self.args.data_type,  # historical data type
            self.useRTH,  # useRTH (regular trading hours)
            1,  # format the date in yyyyMMdd HH:mm:ss
            False,  # keep up to date after snapshot
            [],  # chart options
        )

    def save_data(self, contract: Contract, bars: BarDataList) -> None:
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

        df["date"] = df["date"].apply(self._parse_timestamp)

        if self.daily_files():
            # just overwrite whatever is there
            path = "%s.csv" % make_download_path(self.args, contract)
            df.to_csv(path, index=False)
        else:
            # depending on how things moved along, we'll have data
            # from different dates.
            for d in df["date"].dt.date.unique():
                path = os.path.sep.join(
                    [
                        make_download_path(self.args, contract),
                        "%s.csv" % d.strftime("%Y%m%d"),
                    ]
                )
                new_bars = df.loc[df["date"].dt.date == d]
                # if a file exists, let's attempt to load it, merge our data in, and then save it
                if os.path.exists(path):
                    existing_bars = pd.read_csv(path, parse_dates=["date"])
                    combined = pd.concat([existing_bars, new_bars])
                    new_bars = combined.groupby("date").last().reset_index()

                new_bars.to_csv(path, index=False)

    def daily_files(self):
        return SIZES.index(self.args.size.split()[1]) >= 5

    def _parse_timestamp(self, ts: str) -> datetime:
        def _try_formats(_ts: str) -> datetime:
            if len(_ts) == 8:
                try:
                    _ts = datetime.strptime(_ts, "%Y%m%d")
                    return _ts
                except ValueError as ve:
                    logging.error("%s not parseable as date: %s", _ts, ve)
                    raise ve
            try:
                _ts = datetime.strptime(_ts, "%Y%m%d-%H:%M:%S")
            except ValueError as ve:
                try:
                    _ts = datetime.strptime(_ts, "%Y%m%d  %H:%M:%S")
                except ValueError as ve2:
                    _ts = datetime.strptime(_ts, "%Y-%m-%d %H:%M:%S%z")
            return _ts

        tokens = ts.split()
        if len(tokens) == 1:
            ts = _try_formats(ts)
            tz = pytz.timezone(self.args.timezone)
            ts = tz.localize(ts)
        else:
            ts = _try_formats(" ".join(tokens[:-1]))
            tz = pytz.timezone(tokens[-1])
            ts = tz.localize(ts)
        return ts

    @iswrapper
    def headTimestamp(self, reqId: int, headTimestamp: str) -> None:
        contract = self.requests.get(reqId)
        ts = self._parse_timestamp(headTimestamp)

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
        else:
            self.duration = f"{24 * 60 * 60} S"  # a day's worth

        self.historicalDataRequest(contract)

    @iswrapper
    def historicalData(self, reqId: int, bar) -> None:
        self.bar_data[reqId].append(bar)

    @iswrapper
    def historicalDataEnd(self, reqId: int, start: str, end: str) -> None:
        super().historicalDataEnd(reqId, start, end)
        logging.info("historicalDataEnd: %s %s %s", reqId, start, end)
        ts = self._parse_timestamp(start)

        self.handle_end(reqId, ts)

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
                for cd in self.contracts:
                    self.historicalDataRequest(cd.contract)

    @iswrapper
    def connectAck(self):
        logging.info("Connected")

    @iswrapper
    def nextValidId(self, order_id: int):
        super().nextValidId(order_id)

        self.next_valid_order_id = order_id
        logging.info("nextValidId: %s", order_id)
        # we can start now
        self.start()

    @iswrapper
    def contractDetails(self, reqId: int, contractDetails: ContractDetails) -> None:
        super().contractDetails(reqId, contractDetails)
        logging.debug("ContractDetails for %s: %s", reqId, contractDetails)
        self.contracts.append(contractDetails)

    @iswrapper
    def contractDetailsEnd(self, reqId: int) -> None:
        for cd in self.contracts:
            self.reqHeadTimeStamp(
                self.next_request_id(cd.contract),
                cd.contract,
                self.args.data_type,
                0,
                1,
            )

    def start(self):
        if self.started:
            return

        self.started = True
        for contract in self.contracts_specs:
            self.reqContractDetails(self.next_request_id(contract), contract)

    @iswrapper
    def error(
        self,
        req_id: TickerId,
        error_code: int,
        error: str,
        advancedOrderRejectJson: str = "",
    ):
        super().error(req_id, error_code, error)
        if req_id < 0:
            # we get error logs that really are just info
            logging.debug("Error. Id: %s Code %s Msg: %s", req_id, error_code, error)
        else:
            logging.error("Error. Id: %s Code %s Msg: %s", req_id, error_code, error)
            if error_code == 162:  # no data returned, keep going
                self.handle_end(req_id, self.next_start_time())
            elif error_code == 200:  # security doesn' exist
                logging.error("The security doesn't exist, check your parameters")

            # we will always exit on error since data will need to be validated
            self.send_done(0)


def make_contract(
    symbol: str, sec_type: str, currency: str, exchange: str, localsymbol: str
) -> Contract:
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


def environment_check(args) -> None:
    # print out some info about the environment
    logging.debug("Python version: %s", sys.version)
    logging.debug("IBPy version: %s", ibapi.__version__)
    logging.debug("Args: %s", args)

    version = ibapi.get_version_string()
    if "." not in version:
        print("Warning: IBPy version is not in the expected format. %s" % version)
        sys.exit(1)
    major = version.split(".")[0]
    try:
        major = int(major)
        if major < 10:
            print(
                "Warning: This script does not work with IBPy versions below 10: %s"
                % version
            )
            print(
                "Please upgrade to version 10 of IBPy, you can download it from https://interactivebrokers.github.io."
            )
            sys.exit(1)
    except ValueError:
        print("Warning: IBPy version is not in the expected format. %s" % version)
        sys.exit(1)


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

    argp = argparse.ArgumentParser(
        prog="TWSDownloadApp",
        description="""
    Downloader for Interactive Brokers bar data. Using TWS API, will download
    historical instrument data and place csv files in a specified directory.
    Handles basic errors and reports issues with data that it finds.
    """,
        epilog="""
    Examples:
    Get the continuous 1 minute bars for the E-mini future from CME
        ./download_bars.py --security-type CONTFUT --start-date 20191201 --end-date 20191228 --exchange CME ES

    Get 1 minute bars for US Equity AMGN for a few days
        ./download_bars.py --size "1 min" --start-date 20200202 --end-date 20200207 AMGN
    """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    argp.add_argument("symbol", nargs="+")
    argp.add_argument(
        "-d", "--debug", action="store_true", help="turn on debug logging"
    )
    argp.add_argument("--logfile", help="log to file")
    argp.add_argument(
        "-p", "--port", type=int, default=7496, help="local port for TWS connection"
    )
    argp.add_argument("--size", type=str, default="1 min", help="bar size")
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
    argp.add_argument("--useRTH", action="store_true", help="use Regular Trading Hours")
    argp.add_argument(
        "--timezone", type=str, help="Timezone for requests", default="UTC"
    )
    argp.add_argument(
        "--start-date",
        help="First day for bars",
        default=now - timedelta(days=2),
        action=DateAction,
    )
    argp.add_argument(
        "--end-date",
        help="Last day for bars",
        default=now,
        action=DateAction,
    )
    argp.add_argument(
        "--max-days",
        help="Set start date to earliest date",
        action="store_true",
    )
    args = argp.parse_args()

    logargs = dict(
        format="%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    if args.debug:
        logargs["level"] = logging.DEBUG
    else:
        logargs["level"] = logging.INFO

    if args.logfile:
        logargs["filemode"] = "a"
        logargs["filename"] = args.logfile

    logging.basicConfig(**logargs)

    environment_check(args)

    try:
        validate_size(args.size)
        args.data_type = args.data_type.upper()
        validate_data_type(args.data_type)
    except ValidationException as ve:
        logging.error(ve)
        sys.exit(1)

    tz = pytz.timezone(args.timezone)
    args.start_date = tz.localize(args.start_date)
    args.end_date = tz.localize(args.end_date)
    logging.debug(f"args={args}")
    contracts = []
    for s in args.symbol:
        contract = make_contract(
            s, args.security_type, args.currency, args.exchange, args.localsymbol
        )
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
