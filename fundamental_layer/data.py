#!usr/env/bin ipython

"""DataHandler Class

This file is designed for handling data transferring from .csv file or from 
database. Bar information is updated per heartbeat.

"""

import datetime
import os, os.path
import pandas as pd
import numpy as np
import sqlalchemy

from abc import ABCMeta, abstractmethod

from event import MarketEvent

class DataHandler(object):
    """
    DataHandler is an abstract base class providing an interface for
    all subsequent (inherited) data handlers (both live and historic).

    The goal of a (derived) DataHandler object is to output a generated
    set of bars (OLHCVI) for each symbol requested. 

    This will replicate how a live strategy would function as current
    market data would be sent "down the pipe". Thus a historic and live
    system will be treated identically by the rest of the backtesting suite.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def get_latest_bars(self, symbol, N=1):
        """
        Returns the last N bars from the latest_symbol list,
        or fewer if less bars are available.
        """
        raise NotImplementedError("Should implement get_latest_bars()")

    @abstractmethod
    def update_bars(self):
        """
        Pushes the latest bar to the latest symbol structure
        for all symbols in the symbol list.
        """
        raise NotImplementedError("Should implement update_bars()")


class HistoricCSVDataHandler(DataHandler):
    """
    HistoricCSVDataHandler is designed to read CSV files for
    each requested symbol from disk and provide an interface
    to obtain the "latest" bar in a manner identical to a live
    trading interface. 
    """

    def __init__(self, events, csv_dir, symbol_list):
        """
        Initialises the historic data handler by requesting
        the location of the CSV files and a list of symbols.

        It will be assumed that all files are of the form
        'symbol.csv', where symbol is a string in the list.

        Parameters:
        events - The Event Queue.
        csv_dir - Absolute directory path to the CSV files.
        symbol_list - A list of symbol strings.
        """
        self.events = events
        self.csv_dir = csv_dir
        self.symbol_list = symbol_list

        self.symbol_data = {}
        self.latest_symbol_data = {}
        self.continue_backtest = True       

        self._open_convert_csv_files()

    def _open_convert_csv_files(self):
        """
        Opens the CSV files from the data directory, converting
        them into pandas DataFrames within a symbol dictionary.

        For this handler it will be assumed that the data is
        taken from DTN IQFeed. Thus its format will be respected.
        """
        comb_index = None
        for s in self.symbol_list:
            # Load the CSV file with no header information, indexed on date
            self.symbol_data[s] = pd.io.parsers.read_csv(
                                      os.path.join(self.csv_dir, '%s.csv' % s),
                                      header=0, index_col=0,
                                      names=['datetime','open','low','high',
                                                 'close','volume','oi']
                                  )

            # Combine the index to pad forward values
            if comb_index is None:
                comb_index = self.symbol_data[s].index
            else:
                comb_index.union(self.symbol_data[s].index)

            # Set the latest symbol_data to None
            self.latest_symbol_data[s] = []

        # Reindex the dataframes
        for s in self.symbol_list:
            self.symbol_data[s] = self.symbol_data[s].reindex(index=comb_index,
                                                            method='pad').iterrows()

    def _get_new_bar(self, symbol):
        """
        Returns the latest bar from the data feed as a tuple of 
        (sybmbol, datetime, open, low, high, close, volume).
        """
        for b in self.symbol_data[symbol]:
        #    yield tuple([symbol, datetime.datetime.strptime(b[0], '%Y-%m-%d %H:%M:%S'), 
        #                b[1][0], b[1][1], b[1][2], b[1][3], b[1][4]])
            yield tuple([symbol, datetime.datetime.strptime(b[0], '%m/%d/%y'), 
                        b[1][0], b[1][1], b[1][2], b[1][3], b[1][4]])

    def get_latest_bars(self, symbol, N=1):
        """
        Returns the last N bars from the latest_symbol list,
        or N-k if less available.
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print "That symbol is not available in the historical data set."
        else:
            return bars_list[-N:]

    def update_bars(self):
        """
        Pushes the latest bar to the latest_symbol_data structure
        for all symbols in the symbol list.
        """
        for s in self.symbol_list:
            try:
                bar = self._get_new_bar(s).next()
            except StopIteration:
                self.continue_backtest = False
            else:
                if bar is not None:
                    self.latest_symbol_data[s].append(bar)
        self.events.put(MarketEvent())


class HistoricSQLDataHandler(DataHandler):
    """
    Implemented by Javier Garcia, javier.macro.trader@gmail.com
    Modified by Albert Zhang
    HistoricSQLDataHandler is designed to read a SQLite database or a
    SQLAlchemy engine for each requested symbol from disk and 
    provide an interface to obtain the "latest" bar in a manner 
    identical to a live trading interface.

    ARG:
        database_path: the system path where the SQLite database is stored.
                
        symbol_list: list containing the name of the symbols
                to read in the database.
                The SQL consult is expected to have the following structure:
                [date, open, high, low, close, volume, price_change]
                taken from Tushare API

    IMPORTANT: for different symbols in differents time-zones you must assure
                the data is correctly syncronized. Athena does not check
                this.

    """
    def __init__(self, events, database, symbol_list, flavor='SQLAlchemy'):
        """
        Initialises the historic data handler by requesting
        the location of the database and a list of symbols.

        It will be assumed that all price data is in a table called 
        'symbols', where the field 'symbol' is a string in the list.

        Parameters:
        events - The Event Queue.
        csv_dir - Absolute directory path to the database
        symbol_list - A list of symbol strings
        flavor - the flavor of database connection, default is SQLAlchemy.
        """
        self.events = events
        self.database = database
        self.symbol_list = symbol_list
        self.flavor = flavor

        self.symbol_data = {}
        self.latest_symbol_data = {}
        self.continue_backtest = True
        self.bar_index = 0
        self.all_data_dic = {}  # access data in list form for testing

        self._open_convert_database_data()
    
    def _connect_to_database(self, database, flavor):
        """
        Connect to the database ....
        :param database: full path to SQLite3 database to connect or 
                         keywords for MySQL engine
        """
        if flavor == 'sqlite3':
            try:
                connection = sqlite3.connect(database, 
                    detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
                return connection
            except sqlite3.Error as err:
                print('Error connecting database', err.args[0])
        # TODO: this leg is not finished
        elif flavor == 'SQLAlchemy':
            try:
                engine = sqlalchemy.create_engine(database)
                return engine
            except sqlalchemy.exc as err:
                print('Error connecting database', err)
        
    def _get_prices(self, conn, symbol, cols):
        """
        Query the database and returns a dataframe with the chosen 7 columns.
        :param conn: Database connection
        :param symbol: Stock symbol 
        :param cols: A column represents column attributes in the database.
        """
        values_qry = '''SELECT {},{},{},{},{},{},{}
                        FROM {}'''.format(cols[0],
                                              cols[1],
                                              cols[2],
                                              cols[3],
                                              cols[4],
                                              cols[5],
                                              cols[6],
                                              symbol)
        return pd.read_sql(values_qry, conn, index_col='date') # `index_col` hard coded

    def _open_convert_database_data(self):
        """
        Opens the database files, converting them into 
        pandas DataFrames within a symbol dictionary.

        For this handler it will be assumed that the data is
        taken from Tushare API. Thus its format will be respected.
        """
        comb_index = None

        columns = ['date',
                   'open',
                   'high',
                   'low',
                   'close',
                   'volume',
                   'price_change']
        connection = self._connect_to_database(self.database, self.flavor)

        for symbol in self.symbol_list:
            self.symbol_data[symbol] = self._get_prices(connection, symbol, columns)

            # Combine the index to pad forward values
            if comb_index is None:
                comb_index = self.symbol_data[symbol].index
            else:
                comb_index.union(self.symbol_data[symbol].index)

            # Set the latest symbol_data to None
            self.latest_symbol_data[symbol] = []

        # Reindex the dataframes
        for symbol in self.symbol_list:
            self.all_data_dic[symbol] = self.symbol_data[symbol].\
                reindex(index=comb_index, method='pad')

            self.symbol_data[symbol] = self.symbol_data[symbol].\
                reindex(index=comb_index, method='pad').iterrows()


    def _get_new_bar(self, symbol):
        """
        Returns the latest bar from the data feed.
        """
        for b in self.symbol_data[symbol]:
            yield tuple([symbol, b[0], 
                        b[1][0], b[1][1], b[1][2], b[1][3], b[1][4]])

    def get_latest_bar(self, symbol):
        """
        Returns the last bar from the latest_symbol list.
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            raise KeyError("Symbol is not available in the data set.")
        else:
            if not bars_list:
                raise KeyError('latest_symbol_data has not been initialized.')
            else:
                return bars_list[-1]

    def get_latest_bars(self, symbol, bars=1):
        """
        Returns the last N bars from the latest_symbol list,
        or N-k if less available.
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            raise KeyError("Symbol is not available in the data set.")
        else:
            if not bars_list:
                raise KeyError('latest_symbol_data has not been initialized.')
            else:
                return bars_list[-bars:]

    def get_latest_bar_datetime(self, symbol):
        """
        Returns a Python datetime object for the last bar.
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            raise KeyError("Symbol is not available in the data set.")
        else:
            if not bars_list:
                raise KeyError ('latest_symbol_data has not been initialized.')
            else:
                return bars_list[-1][1]

    def update_bars(self):
        """
        Pushes the latest bar to the latest_symbol_data structure
        for all symbols in the symbol list.
        """
        for symbol in self.symbol_list:
            try:
                bars = self._get_new_bar(symbol).next()
            except StopIteration:
                self.continue_backtest = False
            else:
                if bars is not None:
                    self.latest_symbol_data[symbol].append(bars)
        self.events.put(MarketEvent())
