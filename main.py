#!usr/env/bin ipython

"""Main backtest program

This file contains the main program to run a backtest.

"""

import datetime

from fundamental_layer.backtest import Backtest
from fundamental_layer.data import HistoricCSVDataHandler, HistoricSQLDataHandler
from fundamental_layer.execution import SimulatedExecutionHandler
from fundamental_layer.portfolio import NaivePortfolio
from strategies.buy_and_hold_strategy import BuyAndHoldStrategy


if __name__ == "__main__":
    data_source = '~/documents/projects/backtester/'
    database = "mysql+mysqlconnector://root:godzaq-159753@localhost/stocks"
    symbol_list = ['AAPL']
    initial_capital = 100000.0
    start_date = datetime.datetime(2014,1,9,0,0,0)
    heartbeat = 0.0

    backtest = Backtest(data_source, 
                        symbol_list, 
                        initial_capital, 
                        heartbeat,
                        start_date,
                        HistoricCSVDataHandler, 
                        SimulatedExecutionHandler, 
                        NaivePortfolio, 
                        BuyAndHoldStrategy)
    
    backtest.simulate_trading()
