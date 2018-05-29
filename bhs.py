#!usr/env/bin ipython

"""Simple BuyAndHoldStrategy with Backtest
"""

import datetime
import numpy as np

from fundamental_layer.backtest import Backtest
from fundamental_layer.data import HistoricCSVDataHandler
from fundamental_layer.event import SignalEvent
from fundamental_layer.execution import SimulatedExecutionHandler
from fundamental_layer.portfolio import NaivePortfolio
from fundamental_layer.strategy import Strategy

class BuyAndHoldStrategy(Strategy):
    """
    This is an extremely simple strategy that goes LONG all of the 
    symbols as soon as a bar is received. It will never exit a position.

    It is primarily used as a testing mechanism for the Strategy class
    as well as a benchmark upon which to compare other strategies.
    """

    def __init__(self, bars, events):
        """
        Initialises the buy and hold strategy.

        Parameters:
        bars - The DataHandler object that provides bar information
        events - The Event Queue object.
        """
        self.bars = bars
        self.symbol_list = self.bars.symbol_list
        self.events = events

        # Once buy & hold signal is given, these are set to True
        self.bought = self._calculate_initial_bought()

    def _calculate_initial_bought(self):
        """
        Adds keys to the bought dictionary for all symbols
        and sets them to False.
        """
        bought = {}
        for s in self.symbol_list:
            bought[s] = False
        return bought

    def calculate_signals(self, event):
        """
        For "Buy and Hold" we generate a single signal per symbol
        and then no additional signals. This means we are 
        constantly long the market from the date of strategy
        initialisation.

        Parameters
        event - A MarketEvent object. 
        """
        strength = 1.0
        if event.type == 'MARKET':
            for s in self.symbol_list:
                bars = self.bars.get_latest_bars(s, N=1)
                if bars is not None and bars != []:
                    if self.bought[s] == False:
                        # (Symbol, Datetime, Type = LONG, SHORT or EXIT)
                        signal = SignalEvent(bars[0][0], bars[0][1], 'LONG', strength)
                        self.events.put(signal)
                        self.bought[s] = True


if __name__ == "__main__":
    data_source = '~/documents/projects/backtester/'
    symbol_list = ['AAPL']
    initial_capital = 100000.0
    start_date = datetime.datetime(1990,1,1,0,0,0)
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