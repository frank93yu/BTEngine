# locate the path of database API
import sys
sys.path.append("../dbApi/")

# import database API
from dbAPI import MyDBApi_GeoDaily
from dbAPI import MyDBApi_GeoMinute

import numpy as np
import pandas as pd
import datetime

def initialize_DB_connection():
    # establish database connection for daily and minute data
    minutePriceDB = MyDBApi_GeoMinute({'user': 'root', 'password': '933900',\
        'host': '127.0.0.1', 'database': 'GeoTickersMinute15_17',})
    minutePriceDB.connect()
    dailyPriceDB = MyDBApi_GeoDaily({'user': 'root', 'password': '933900',\
        'host': '127.0.0.1', 'database': 'GeoTickersDaily15_17',})
    dailyPriceDB.connect()
    return dailyPriceDB, minutePriceDB

def get_ticker_universe(path='./allTickers.csv'):
    # get all the tickers in the backtest universe
    tempTickerList = pd.DataFrame.from_csv(path)
    tempTickerList = list(tempTickerList.index.values)
    tickerList = np.array([iTicker.split()[0] for iTicker in tempTickerList])
    return tickerList

orderBatchSample = [{"ticker": "AAP", "nshares": 20, "start_time": "10:00:00", "end_time": "16:00:00"}, {"ticker": "YUM", "nshares": -10, "start_time": "10:00:00", "end_time": "16:00:00"}]

class MyBacktestEngine():
    # the backtest engine
    def __init__(self):
        self.write_log("New back-test created!----------------------------------------------------\n")
        self.dailyPriceDB, self.minutePriceDB = initialize_DB_connection()
        self.ticker_universe = get_ticker_universe() # np.array of strings, containing all the tickers relevant

        # initialized in create_calendar method
        self.calendar = None # list of datetime type, all trading days
        self.start_date, self.end_date = None, None # strings denoting actual time frame

        # initialized in load_daily_close method
        self.allTickersDailyCloseDF = None # dataframe of all the close prices of all tickers in the universe
        self.ticker2index = None # A dict map ticker names to their corresponding column index in the numpy array below
        self.allTickersDailyCloseNP = None # numpy array of all the close prices for all tickers, for better iteration speed

        # initialized in initialize_positions method
        self.cash_position, self.total_position = None, None # np array containing cash postion series
        self.tickers_positions = None # np array containing tickers' postions serieses
        self.cash_position_taken = None # np array containing cash position taken
        self.tickers_positions_taken = None # np array containing tickers' positions taken


    def create_calendar(self, start_date, end_date, benchmark_ticker='SPY'):
        # using the dates available for benchmark to create a gobal trading calendar
        # it has to be made sure that benchmark has all the trading dates wanted available
        try:
            self.dailyPriceDB.query_by_date({'ticker': benchmark_ticker, \
                'start_date': start_date, 'end_date': end_date, 'datatypes': ['date', 'close']})
            self.calendar = [iDate for iDate, iClose in self.dailyPriceDB.cursor if iClose != None]
            self.start_date, self.end_date = str(self.calendar[0]), str(self.calendar[-1])
            print('Calendar created on trading days from ' + str(self.calendar[0]) + ' to ' + str(self.calendar[-1]) + '.')
            self.write_log('Calendar created on trading days from ' + self.start_date + ' to ' + self.end_date + '.\n')
            return(0)
        except:
            print('Calendar construction failed!')
            self.write_log('Calendar construction crashed with ' + benchmark_ticker + ' ' + start_date + ' ' + end_date + '.\n')
            return(-1)

    def load_daily_close(self):
        # load daily data for relevant tickers in to memory for PnL tracking
        # this method is useful in most cases, and PnL is mostly tracked on a daily bases
        # data on more frequent time scale is quried on call if PnL is tracked daily
        try:
            print('Loading daily data...')
            tempTickerData = []
            for iTicker in self.ticker_universe:
                try:
                    iQuery = {'ticker': iTicker, 'start_date': self.start_date, 'end_date': self.end_date, 'datatypes': ['date', 'close']}
                    self.dailyPriceDB.query_by_date(iQuery)
                    iTickerDataTable = [[date, close] for date, close in self.dailyPriceDB.cursor]
                    iTickerDataFrame = pd.DataFrame(iTickerDataTable)
                    iTickerDataFrame.columns = ['date', iTicker]
                    iTickerDataFrame = iTickerDataFrame.set_index('date')
                except:
                    print('Data for ' + iTicker + ' not available for this time frame!')
                    self.write_log('Data for ' + iTicker + ' not available for time frame: ' + self.start_date + ' to ' + self.end_date + '.')
                tempTickerData.append(iTickerDataFrame)
            allTickersData = pd.concat(tempTickerData, axis=1, join='outer')

            calendar = pd.DataFrame(self.calendar)
            calendar.columns = ['date']
            calendar = calendar.set_index('date')
            self.allTickersDailyCloseDF = calendar.merge(allTickersData, how='inner', left_index=True, right_index=True)

            tickerNames = list(self.allTickersDailyCloseDF.columns)
            tickerNamesIndex = range(len(tickerNames))
            self.ticker2index = dict(zip(tickerNames, tickerNamesIndex))

            self.allTickersDailyCloseNP = self.allTickersDailyCloseDF.as_matrix()
            print('Daily data is loaded successfully.')
            self.write_log('Daily data is loaded successfully.\n')
            return(0)
        except:
            print('Daily data fail to load!')
            self.write_log('Daily data fail to load!\n')
            return(-1)

    def initialize_positions(self, principle):
        # initialize all kinds of positions
        # zero level positions
        self.cash_position = np.zeros(self.allTickersDailyCloseNP.shape[0] + 1)
        self.cash_position[0] = principle
        self.tickers_positions = np.zeros((self.allTickersDailyCloseNP.shape[0] + 1, self.allTickersDailyCloseNP.shape[1]))
        self.total_position = np.zeros(self.allTickersDailyCloseNP.shape[0] + 1)
        self.total_position[0] = principle
        # first difference level positions
        self.cash_position_taken = np.zeros(self.allTickersDailyCloseNP.shape[0])
        self.tickers_positions_taken = np.zeros(self.allTickersDailyCloseNP.shape)

    def run(self):
        # iterate through the calendar
        dailyStrategy = Strategy()
        for iDate in range(len(self.calendar)):
            print(self.cash_position[iDate])
            #print(self.tickers_positions[iDate])
            print(self.total_position[iDate])
            iOrderBatch = dailyStrategy.run(self.calendar[iDate])
            self.place_orders_batch(iDate, iOrderBatch, True)
            self.daily_settlement(iDate)


    def place_orders_batch(self, i_date, order_batch, use_minute_data=False):

        for iOrder in order_batch:
            iTicker = iOrder['ticker']
            iOrderAmount = iOrder['nshares']
            if not use_minute_data:
                iCost = self.allTickersDailyCloseNP[i_date][self.ticker2index[iTicker]]
            else:
                iStartTime = iOrder['start_time']
                iEndTime = iOrder['end_time']
                iCost = self.get_minute_mean(iTicker, str(self.calendar[i_date]), '09:00:00', str(self.calendar[i_date]), '16:00:00')
            if iCost == None:
                iCost = 0
                print('Fatal data missing for ' + iTicker + ' on ' + str(self.calendar[i_date]) + ', trade aborted.')
            self.cash_position_taken[i_date] -= iCost*iOrderAmount
            self.tickers_positions_taken[i_date][self.ticker2index[iTicker]] += iOrderAmount
            print(str(self.calendar[i_date]) + ': ' + str(iOrderAmount) + ' shares of ' + iTicker + ' at ' + str(iCost) + '.')

    def get_minute_mean(self, ticker, start_date, start_time, end_date, end_time):
        self.minutePriceDB.query_by_datetime({'ticker': ticker, 'start_date': start_date, 'start_time': start_time, \
            'end_date': end_date, 'end_time': end_time, 'datatypes': ['close']})
        minuteClosePrice = np.array([close for close in self.minutePriceDB.cursor])
        return minuteClosePrice.mean()

    def daily_settlement(self, i_date):
        self.cash_position[i_date + 1] = self.cash_position[i_date] + self.cash_position_taken[i_date]
        self.tickers_positions[i_date + 1] = self.tickers_positions[i_date] + self.tickers_positions_taken[i_date]
        tickerTtlValue = 0
        for iTicker in self.ticker2index:
            ind_iTicker = self.ticker2index[iTicker]
            if self.allTickersDailyCloseNP[i_date][ind_iTicker] == None:
                if self.tickers_positions[i_date + 1][ind_iTicker] != 0:
                    print('Fatal data missing for ' + iTicker + ' on ' + str(self.calendar[i_date]) + ', position neglected.')
            elif np.isnan(self.allTickersDailyCloseNP[i_date][ind_iTicker]):
                if self.tickers_positions[i_date + 1][ind_iTicker] != 0:
                    print('Fatal data missing for ' + iTicker + ' on ' + str(self.calendar[i_date]) + ', position neglected.')
            else:
                tickerTtlValue += self.allTickersDailyCloseNP[i_date][ind_iTicker]*self.tickers_positions[i_date + 1][ind_iTicker]
        self.total_position[i_date + 1] = tickerTtlValue + self.cash_position[i_date + 1]

    def write_log(self, log):

        with open('backtest.log', 'a') as handle:
            handle.write(str(datetime.datetime.now()) + ": " + log + "\n")

class Strategy():
    def __init__(self):
        pass
    def run(self, i_date):
        return [{"ticker": "AAP", "nshares": 20, "start_time": "10:00:00", "end_time": "16:00:00"}, {"ticker": "YUM", "nshares": -10, "start_time": "10:00:00", "end_time": "16:00:00"}]

a = MyBacktestEngine()
a.create_calendar('2015-01-01', '2017-01-01')
a.load_daily_close()
a.initialize_positions(1000000)
a.run()
a.allTickersDailyCloseDF.to_csv("/home/frank/Desktop/test.csv")
