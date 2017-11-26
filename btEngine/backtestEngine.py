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


class MyBacktestEngine():
    # the backtest engine
    def __init__(self):
        self.write_log("New back-test created!----------------------------------------------------\n")
        self.dailyPriceDB, self.minutePriceDB = initialize_DB_connection()
        self.ticker_universe = get_ticker_universe() # np.array of strings, containing all the tickers relevant
        self.calendar = None # list of datetime type, all trading days
        self.start_date, self.end_date = None, None # strings denoting actual time frame
        self.allTickersDailyClose = None # dataframe of all the close prices of all tickers in the universe

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

    def load_daily_data(self):
        # load daily data for relevant tickers in to memory
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
            self.allTickersDailyClose = calendar.merge(allTickersData, how='inner', left_index=True, right_index=True)
            print('Daily data is loaded successfully.')
            self.write_log('Daily data is loaded successfully.\n')
            return(0)
        except:
            print('Daily data fail to load!')
            self.write_log('Daily data fail to load!\n')
            return(-1)



    def write_log(self, log):

        with open('backtest.log', 'a') as handle:
            handle.write(str(datetime.datetime.now()) + ": " + log + "\n")


a = MyBacktestEngine()
a.create_calendar('2015-01-01', '2017-01-01')
a.load_daily_data()
print("yoyo")
