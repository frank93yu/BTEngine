import mysql.connector as msql
from mysql.connector import errorcode

# db_config = {'user': 'root', 'password': '933900',\
# 'host': '127.0.0.1', 'database': 'equities_DB_adj',}

# date_q_info = {'ticker': 'AMZN', 'start_date': '2016-01-01',\
# 'end_date': '2016-02-01', 'datatypes': ['date', 'open', 'close']}

class Backtest_db():
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
