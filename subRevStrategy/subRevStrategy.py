# locate the path of btEngine
import sys
sys.path.append("../btEngine/")

from backtestEngine import MyBacktestEngine;

geoStrategyBT = MyBacktestEngine()
class Strategy():
    def __init__(self):
        pass
    def run(self, i_date):
