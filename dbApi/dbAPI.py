import mysql.connector as msql
from mysql.connector import errorcode

# data structure examples provided below

# db_config = {'user': 'root', 'password': '933900',\
# 'host': '127.0.0.1', 'database': 'equities_DB_adj',}

# date_q_info = {'ticker': 'AMZN', 'start_date': '2016-01-01',\
# 'end_date': '2016-02-01', 'datatypes': ['date', 'open', 'close']}

class MyDBApi():
    # base class for DB
    def __init__(self, config = {'user': 'root', 'password': 'trinnacle17',\
    'host': '127.0.0.1', 'database': 'equities_DB',}):

        self.config = config

    def connect(self):

        self.cnx = msql.connect(**self.config)
        self.cursor = self.cnx.cursor()
        print ("Connection to database " + self.config["database"] + " is established.")

    def quit(self):

        self.cursor.close()
        self.cnx.close()
        print ("Connection to database " + self.config["database"] + " is closed.")

class MyDBApi_GeoMinute(MyDBApi):
    # subclass for minute data
    def __init__(self, config):

        MyDBApi.__init__(self, config)

    def query_by_datetime(self, info = {'ticker': 'APP', 'start_date': '2016-01-01', 'start_time': '09:00:00', \
    'end_date': '2016-02-01', 'end_time': '16:00:00', 'datatypes': ['date', 'open', 'close']}):

        if info['ticker'] in ["ALL", "AS", "ASC", "CALL", "FOR", "IN", "INT", "KEY", "KEYS", "LOCK", "LOOP", "MOD", "ON", "OK", "OUT", "TRUE"]:
            info['ticker'] = info['ticker'] + '_'

        datatypestr = ""
        for i_type in range(len(info['datatypes'])):
            datatypestr = datatypestr + info['datatypes'][i_type]
            if i_type != len(info['datatypes']) - 1:
                datatypestr = datatypestr + ', '
        qstr = "SELECT " + datatypestr + " FROM " + info['ticker'] + \
            " WHERE TIMESTAMP(date, time) BETWEEN '" + info['start_date'][0:10] + ' ' + info['start_time'][0:8] + "' AND '" + \
            info['end_date'][0:10] + ' '+ info['end_time'][0:8] + "'"
        self.cursor.execute(qstr)

class MyDBApi_GeoDaily(MyDBApi):
    # subclass for daily data
    def __init__(self, config):

        MyDBApi.__init__(self, config)

    def query_by_date(self, info = {'ticker': 'AAP', 'start_date': '2016-01-01',\
    'end_date': '2016-02-01', 'datatypes': ['date', 'open', 'close']}):

        if info['ticker'] in ["ALL", "AS", "ASC", "CALL", "FOR", "IN", "INT", "KEY", "KEYS", "LOCK", "LOOP", "MOD", "ON", "OK", "OUT", "TRUE"]:
            info['ticker'] = info['ticker'] + '_'

        datatypestr = ""
        for i_type in range(len(info['datatypes'])):
            datatypestr = datatypestr + info['datatypes'][i_type]
            if i_type != len(info['datatypes']) - 1:
                datatypestr = datatypestr + ', '
        qstr = "SELECT " + datatypestr + " FROM " + info['ticker'] + \
            " WHERE date BETWEEN '" + info['start_date'][0:10]+ "' AND '" + \
            info['end_date'][0:10] + "'"
        self.cursor.execute(qstr)

# # sample code
# # sample minute data query
# geoMinPriceDB = MyDBApi_GeoMinute({'user': 'root', 'password': '933900',\
# 'host': '127.0.0.1', 'database': 'GeoTickersMinute15_17',})
# geoMinPriceDB.connect()
# geoMinPriceDB.query_by_datetime({'ticker': 'ZUMZ', 'start_date': '2016-01-01', 'start_time': '09:00:00', \
# 'end_date': '2016-01-04', 'end_time': '16:00:00', 'datatypes': ['date', 'time', 'open', 'close']})
# for date, time, open, close in geoMinPriceDB.cursor:
#     print(date, time, open, close)
# geoMinPriceDB.quit()
#
# # sample daily data query
# geoDailyPriceDB = MyDBApi_GeoDaily({'user': 'root', 'password': '933900',\
# 'host': '127.0.0.1', 'database': 'GeoTickersDaily15_17',})
# geoDailyPriceDB.connect()
# geoDailyPriceDB.query_by_date({'ticker': 'ZUMZ', 'start_date': '2016-01-01', \
# 'end_date': '2016-01-04', 'datatypes': ['date', 'open', 'close']})
# for date, open, close in geoDailyPriceDB.cursor:
#     print(date, open, close)
# geoDailyPriceDB.quit()
#
# # sample daily data query
# geoDailyBetaDB = MyDBApi_GeoDaily({'user': 'root', 'password': '933900',\
# 'host': '127.0.0.1', 'database': 'GeoTickersBetaSPY15_17',})
# geoDailyBetaDB.connect()
# geoDailyBetaDB.query_by_date({'ticker': 'ZUMZ', 'start_date': '2016-01-01', \
# 'end_date': '2016-05-04', 'datatypes': ['date', 'beta_SPY']})
# for date, beta_SPY in geoDailyBetaDB.cursor:
#     print(date, beta_SPY)
# geoDailyBetaDB.quit()
