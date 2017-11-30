# locate the path of database API
import sys
sys.path.append("../dbApi/")

# import database API
from dbAPI import MyDBApi_GeoDaily
from dbAPI import MyDBApi_GeoMinute

import numpy as np
import pandas as pd
import datetime
import time

def initialize_DB_connection():
    # establish database connection for daily and minute data
    minutePriceDB = MyDBApi_GeoMinute({'user': 'root', 'password': '933900',\
        'host': '127.0.0.1', 'database': 'GeoTickersMinute15_17',})
    minutePriceDB.connect()
    dailyPriceDB = MyDBApi_GeoDaily({'user': 'root', 'password': '933900',\
        'host': '127.0.0.1', 'database': 'GeoTickersDaily15_17',})
    dailyPriceDB.connect()
    dailyBetaSPYDB = MyDBApi_GeoDaily({'user': 'root', 'password': '933900',\
        'host': '127.0.0.1', 'database': 'GeoTickersBetaSPY15_17',})
    dailyBetaSPYDB.connect()
    return dailyPriceDB, minutePriceDB, dailyBetaSPYDB

def get_ticker_universe(path='/media/frank/SharedDisk/BTEngine/btEngine/allTickers.csv'):
    # get all the tickers in the backtest universe
    tempTickerList = pd.DataFrame.from_csv(path)
    tempTickerList = list(tempTickerList.index.values)
    tickerList = np.array([iTicker.split()[0] for iTicker in tempTickerList])
    return tickerList

class MyBacktestEngine():
    # the backtest engine
    def __init__(self):
        self.write_log("New back-test created!----------------------------------------------------\n")
        self.dailyPriceDB, self.minutePriceDB, self.dailyBetaSPYDB = initialize_DB_connection()
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

        self.all_trades = []


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
        self.write_log('Cash position initialized with ' + str(principle) + '.')

    def run(self, dailyStrategy):
        # iterate through the calendar
        timeStart = time.time()
        for iDate in range(len(self.calendar)):
            print('---------------' + str(self.calendar[iDate]) + '--------------')
            print('cash:' + str(self.cash_position[iDate]))
            #print(self.tickers_positions[iDate])
            print(self.total_position[iDate])
            iOrderBatch = dailyStrategy.run(self.calendar[iDate])
            self.place_orders_batch(iDate, iOrderBatch)
            self.daily_settlement(iDate)
            self.beta_hedge(iDate)
            self.daily_settlement(iDate)
        self.summary()
        print("Time elapsed: ", time.time() - timeStart)

    def summary(self):

        self.PnL = pd.DataFrame([self.calendar, list(self.total_position)[1:]])
        self.PnL = self.PnL.transpose()
        self.PnL.columns = ['date', 'PnL']
        self.PnL['return'] = self.PnL['PnL'].pct_change()
        self.PnL.to_csv("../summary/PnL.csv")
        self.all_trades = pd.DataFrame(self.all_trades)
        self.all_trades.columns = ['date', 'ticker', 'price', 'volume']
        self.all_trades.to_csv("../summary/TradeLog.csv")

    def place_orders_batch(self, i_date, order_batch, use_minute_data=False):

        for iOrder in order_batch:
            iTicker = iOrder['ticker']
            iOrderAmount = iOrder['volume']
            use_weight = iOrder['use_weight']
            if not use_minute_data:
                iCost = self.allTickersDailyCloseNP[i_date][self.ticker2index[iTicker]]
            else:
                iStartTime = iOrder['start_time']
                iEndTime = iOrder['end_time']
                iCost = self.get_minute_mean(iTicker, str(self.calendar[i_date]), '09:00:00', str(self.calendar[i_date]), '16:00:00')
            if iCost == None or np.isnan(iCost):
                iCost = 0
                print('Fatal data missing for ' + iTicker + ' on ' + str(self.calendar[i_date]) + ', trade aborted.')
                self.write_log('Fatal data missing for ' + iTicker + ' on ' + str(self.calendar[i_date]) + ', trade aborted.')
            print(iOrderAmount)
            if iOrderAmount == "close all":
                iOrderAmount = -self.tickers_positions[i_date][self.ticker2index[iTicker]]
            elif use_weight:
                print("cash: " + str(self.cash_position[i_date]))
                iOrderAmount = iOrderAmount*self.cash_position[i_date]/iCost
            self.cash_position_taken[i_date] -= iCost*iOrderAmount
            self.tickers_positions_taken[i_date][self.ticker2index[iTicker]] += iOrderAmount
            self.all_trades.append([self.calendar[i_date], iTicker, iCost, str(iOrderAmount)])
            print(str(self.calendar[i_date]) + ': ' + str(iOrderAmount) + ' shares of ' + iTicker + ' at ' + str(iCost) + '.')
            # self.write_log(str(self.calendar[i_date]) + ': ' + str(iOrderAmount) + ' shares of ' + iTicker + ' at ' + str(iCost) + '.')

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

    def beta_hedge(self, i_date, ticker="SPY"):
        last_hedge_position = self.tickers_positions[i_date][self.ticker2index[ticker]]
        this_hedge_position = 0
        # generate hedging positions
        datestr = str(self.calendar[i_date])
        for iTicker in self.ticker2index:

            iTickerPosition = self.tickers_positions[i_date + 1][self.ticker2index[iTicker]]
            if iTickerPosition == 0:
                continue

            self.dailyBetaSPYDB.query_by_date({'ticker': ticker, 'start_date': datestr, 'end_date': datestr, 'datatypes':['beta_SPY']})
            iBeta = [beta for beta in self.dailyBetaSPYDB.cursor]
            if iBeta == []:
                print("Beta value for " + ticker + " on " + datestr + " is not available!")
            else:
                iBeta = iBeta[0][0]
            if iBeta == None:
                print("Beta value for " + ticker + " on " + datestr + " is not available!")
            elif np.isnan(iBeta):
                print("Beta value for " + ticker + " on " + datestr + " is not available!")

            iTickerClose = self.allTickersDailyCloseNP[i_date][self.ticker2index[iTicker]]
            if iTickerClose == None:
                print("Close price for " + ticker + " on " + datestr + " is not available, Hedging failed!")
            elif np.isnan(iTickerClose):
                print("Close price for " + ticker + " on " + datestr + " is not available, Hedging failed!")

            hedgingTickerClose = self.allTickersDailyCloseNP[i_date][self.ticker2index[ticker]]
            if hedgingTickerClose == None:
                print("Close price for " + ticker + " on " + datestr + " is not available, Hedging failed!")
            elif np.isnan(hedgingTickerClose):
                print("Close price for " + ticker + " on " + datestr + " is not available, Hedging failed!")

            print(iTickerPosition, iBeta)
            iTickerCashPosition = iTickerPosition*iTickerClose
            hedgingTickerCashPosition = -iBeta*iTickerCashPosition
            hedgingTickerPosition = hedgingTickerCashPosition/hedgingTickerClose

            this_hedge_position += hedgingTickerPosition

        hedge_position_taken = this_hedge_position - last_hedge_position
        hedge_order = [{'ticker': ticker, 'volume': hedge_position_taken, "start_time": "10:00:00", "end_time": "16:00:00", "use_weight": False}]
        print(str(self.calendar[i_date]) + ': ' + str(hedge_position_taken) + ' shares of ' + ticker + ' is taken for hedging.')
        return hedge_order

    def write_log(self, log):

        with open('backtest.log', 'a') as handle:
            handle.write(str(datetime.datetime.now()) + ": " + log + "\n")


class Strategy():

    def __init__(self):
        self.subRevSignals = pd.DataFrame.from_csv("/media/frank/SharedDisk/BTEngine/subRevStrategy/bef_subRevSignalBT.csv")
    def run(self, i_date):
        self.subRevSignals['date'] = pd.to_datetime(self.subRevSignals['date'], format="%Y-%m-%d")
        thisBatch = self.subRevSignals[self.subRevSignals['date'] <= i_date]
        self.subRevSignals = self.subRevSignals[self.subRevSignals['date'] > i_date]
        orderList = []
        for irow in range(thisBatch.shape[0]):
            ticker = thisBatch.iloc[irow,0]
            volume = thisBatch.iloc[irow,2]
            if volume != 'close all':
                volume = float(volume)
            orderList.append({'ticker': ticker, 'volume': volume, "start_time": "10:00:00", "end_time": "16:00:00", "use_weight": True})
        return(orderList)

        #return [{"ticker": "AAP", "volume": .1, "start_time": "10:00:00", "end_time": "16:00:00", "use_weight": True}, {"ticker": "YUM", "volume": -.1, "start_time": "10:00:00", "end_time": "16:00:00", "use_weight": True}]
# strategy = Strategy()
# strategy.run(pd.to_datetime('2016-01-01', format="%Y-%m-%d"))

a = MyBacktestEngine()
a.create_calendar('2015-04-01', '2017-08-01')
a.load_daily_close()
a.initialize_positions(1000000)
strategy = Strategy()
a.run(strategy)
