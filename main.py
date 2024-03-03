from src import kucoin as ku 
from src import pg as pg 
from datetime import datetime, timedelta
import prefect as pf

tickers = ku.get_tickers_list()
mode = "update"
type = "1day"
table_name = "kucoin_candlesticks_daily"

# get date of today :
today = datetime.now().date()
yesterday = datetime.now().date() - timedelta(days=10)
# Convert to string
to_date_str = today.strftime("%Y-%m-%d")
from_date_str = yesterday.strftime("%Y-%m-%d")

@pf.flow(name = "Kucoin CandleSticks", log_prints=True, flow_run_name="kucoin_candlesticks_" + datetime.today().strftime("%Y%m%d_%H%M%S"))
def flow_kucoin_candlesticks_daily(mode=mode,type=type,from_date_str=from_date_str, to_date_str=to_date_str,table_name=table_name):
    # get ticker list 
    tickers = ku.get_tickers_list()
    tickers = tickers["symbol"].to_list()
    # request candlesticks
    if mode != "update":
        from_date_str="2022-01-01"
    # generate data
    data = ku.get_daily_candlesticks(tickers=tickers, type=type, from_date=from_date_str, to_date=to_date_str)
    # export data : 
    # pg.export_to_pg(table_name,data,overwrite="append")
    print(flow_kucoin_candlesticks_daily)

#flow_kucoin_candlesticks_daily()