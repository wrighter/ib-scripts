#!/usr/bin/env python

""" A simple Interactive Brokers application that will fetch
current market data for a contract from the IB TWS/Gateway. """

import argparse
import logging
from typing import List

from ibapi import wrapper
from ibapi.common import TickerId, TickAttrib
from ibapi.client import EClient
from ibapi.contract import Contract
from ibapi.utils import iswrapper
from ibapi.ticktype import TickType, TickTypeEnum

ContractList = List[Contract]


class MarketDataApp(EClient, wrapper.EWrapper):
    def __init__(self, contracts: ContractList, args: argparse.Namespace):
        EClient.__init__(self, wrapper=self)
        wrapper.EWrapper.__init__(self)
        self.request_id = 0
        self.started = False
        self.contracts = contracts
        self.request_contracts = {}
        self.pending_ends = set()
        self.args = args

    def next_request_id(self):
        self.request_id += 1
        return self.request_id

    @iswrapper
    def tickPrice(
        self, reqId: TickerId, tickType: TickType, price: float, attrib: TickAttrib
    ):
        print(f"{reqId} {TickTypeEnum.to_str(tickType)} price: {price} {attrib}")

    @iswrapper
    def tickSize(self, reqId: TickerId, tickType: TickType, size: int):
        print(f"{reqId} {TickTypeEnum.to_str(tickType)} size: {size}")

    @iswrapper
    def tickString(self, reqId: TickerId, tickType: TickType, value: str):
        print(f"{reqId} {TickTypeEnum.to_str(tickType)} string: {value}")

    @iswrapper
    def tickGeneric(self, reqId: TickerId, tickType: TickType, value: float):
        print(f"{reqId} {TickTypeEnum.to_str(tickType)} generic: {value}")

    @iswrapper
    def tickSnapshotEnd(self, reqId: int):
        super().tickSnapshotEnd(reqId)
        self.pending_ends.remove(reqId)
        if self.args.snapshot and len(self.pending_ends) == 0:
            print("All snapshot requests complete.")
            self.done = True

    @iswrapper
    def connectAck(self):
        logging.info("Connected")

    @iswrapper
    def nextValidId(self, orderId: int):
        logging.info(f"Next valid order id: {orderId}")
        self.start()

    def start(self):
        if self.started:
            return

        self.started = True
        for contract in self.contracts:
            rid = self.next_request_id()
            self.reqMktData(rid, contract, "225", self.args.snapshot, False, [])
            #self.reqMktData(rid, contract, "233", self.args.snapshot, False, [])
            self.pending_ends.add(rid)
            self.request_contracts[rid] = contract

    @iswrapper
    def error(self, req_id: TickerId, error_code: int, error: str, advancedOrderRejectJson: str=""):
        super().error(req_id, error_code, error)
        print("Error. Id:", req_id, "Code:", error_code, "Msg:", error)


def make_contract(symbol: str, sec_type: str, currency: str, exchange: str):
    contract = Contract()
    contract.symbol = symbol
    contract.secType = sec_type
    contract.currency = currency
    contract.exchange = exchange
    return contract


def main():
    argp = argparse.ArgumentParser()
    argp.add_argument("symbol", action="append")
    argp.add_argument(
        "-d", "--debug", action="store_true", help="turn on debug logging"
    )
    argp.add_argument(
        "-p", "--port", type=int, default=7496, help="local port for TWS connection"
    )
    argp.add_argument(
        "--currency", type=str, default="USD", help="currency for symbols"
    )
    argp.add_argument(
        "--exchange", type=str, default="SMART", help="exchange for symbols"
    )
    argp.add_argument(
        "--security-type", type=str, default="STK", help="security type for symbols"
    )
    argp.add_argument(
        "--snapshot", action="store_true", help="return snapshots and exit"
    )

    args = argp.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    contracts = [
        make_contract(s, args.security_type, args.currency, args.exchange)
        for s in args.symbol
    ]
    app = MarketDataApp(contracts, args)
    app.connect("127.0.0.1", args.port, clientId=0)

    app.run()


if __name__ == "__main__":
    main()
