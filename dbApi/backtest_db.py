import mysql.connector as msql
from mysql.connector import errorcode

# data structure examples provided below

# db_config = {'user': 'root', 'password': '933900',\
# 'host': '127.0.0.1', 'database': 'equities_DB_adj',}

# date_q_info = {'ticker': 'AMZN', 'start_date': '2016-01-01',\
# 'end_date': '2016-02-01', 'datatypes': ['date', 'open', 'close']}

class MyDBApi():

    def __init__(self, config = {'user': 'root', 'password': 'trinnacle17',\
    'host': '127.0.0.1', 'database': 'equities_DB',}):
        self.config = config

    def connect(self):
        self.cnx = msql.connect(**self.config)
        self.cursor = self.cnx.cursor()

    def query_by_date(self, info = {'ticker': 'AMZN', 'start_date': '2016-01-01',\
    'end_date': '2016-02-01', 'datatypes': ['date', 'open', 'close']}):
        datatypestr = ""
        for i_type in range(len(info['datatypes'])):
            datatypestr = datatypestr + info['datatypes'][i_type]
            if i_type != len(info['datatypes']) - 1:
                datatypestr = datatypestr + ', '
        qstr = "SELECT " + datatypestr + " FROM " + info['ticker'] + \
            " WHERE date BETWEEN '" + info['start_date'][0:10]+ "' AND '" + \
            info['end_date'][0:10] + "'"
        self.cursor.execute(qstr)

    def quit(self):
        self.cursor.close()
        self.cnx.close()

class MyDBApi_GeoMinute(MyDBApi):

    def __init__(self, config):
        MyDBApi.__init__(self, config)

    def query_by_datetime(self, info = {'ticker': 'AMZN', 'start_date': '2016-01-01', 'start_time': '09:00:00', \
    'end_date': '2016-02-01', 'end_time': '16:00:00', 'datatypes': ['date', 'open', 'close']}):
        datatypestr = ""
        for i_type in range(len(info['datatypes'])):
            datatypestr = datatypestr + info['datatypes'][i_type]
            if i_type != len(info['datatypes']) - 1:
                datatypestr = datatypestr + ', '
        qstr = "SELECT " + datatypestr + " FROM " + info['ticker'] + \
            " WHERE TIMESTAMP(date, time) BETWEEN '" + info['start_date'][0:10] + ' ' + info['start_time'][0:8] + "' AND '" + \
            info['end_date'][0:10] + ' '+ info['end_time'][0:8] + "'"
        self.cursor.execute(qstr)

# sample code
geoMinDB = MyDBApi_GeoMinute({'user': 'root', 'password': '933900',\
'host': '127.0.0.1', 'database': 'GeoTickersMinute15_17',})
geoMinDB.connect()
geoMinDB.query_by_datetime({'ticker': 'ZUMZ', 'start_date': '2016-01-01', 'start_time': '09:00:00', \
'end_date': '2016-01-04', 'end_time': '16:00:00', 'datatypes': ['date', 'time', 'open', 'close']})
for date, time, open, close in geoMinDB.cursor:
    print(date, time, open, close)
