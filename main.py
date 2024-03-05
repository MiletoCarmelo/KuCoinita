from src import kucoin as ku 
from src import xlsx as xs
from src import GoogleDrive as gd
from src import pg as pg 
from datetime import datetime, timedelta
import prefect as pf

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
    # filter quotCurrency == "USDT"
    tickers = tickers.loc[tickers["quoteCurrency"] == "USDT"]
    # filter not having 3S, 3L, 2S, 2L
    tickers = tickers.loc[~tickers["symbol"].str.contains("3S|3L|2S|2L")]
    # to list
    tickers = tickers["symbol"].to_list()
    # for test purpose take only the first 10 tickers 
    # tickers = tickers[:10]
    # request candlesticks
    if mode != "update":
        from_date_str="2022-01-01"
    # generate data
    data = ku.get_daily_candlesticks(tickers=tickers, type=type, from_date=from_date_str, to_date=to_date_str)
    # export data : 
    # pg.export_to_pg(table_name,data,overwrite="append")
    file_name = "kucoin_candlesticks_" + datetime.today().strftime("%Y%m%d_%H%M%S") + ".xlsx"
    # export to xslx 
    xs.export_toxlsx(data, "./" + file_name)
    # push to google drive : 
    id = gd.upload_file( file_path= "./" + file_name, folder_parent_id='1iTMazQALR7EgoRuxp-T3yTMXNWMUgGHX')

if __name__ == "__main__":
    flow_kucoin_candlesticks_daily()