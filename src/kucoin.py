from datetime import datetime, timedelta
import requests
import pandas as pd
import nest_asyncio
import asyncio
import prefect as pf
from tqdm.asyncio import tqdm


# get date of today :
today = datetime.now().date()
yesterday = datetime.now().date() - timedelta(days=10)
# Convert to string
to_date_str = today.strftime("%Y-%m-%d")
from_date_str = yesterday.strftime("%Y-%m-%d")


############################################################################################################################################
#### request for candelsticks ##############################################################################################################
############################################################################################################################################


def _daily_volume_no_async(ticker="BTC-USDT", type="1day", from_date=from_date_str, to_date=to_date_str):
    # Convert date string to datetime object
    from_date = datetime.strptime(from_date, '%Y-%m-%d')
    to_date = datetime.strptime(to_date, '%Y-%m-%d') - timedelta(seconds=1)
    # convert from_date and to_date to unix time :
    from_date_unix = int(from_date.timestamp())
    to_date_unix = int(to_date.timestamp())
    # request
    req = "https://api.kucoin.com/api/v1/market/candles?type={}&symbol={}&startAt={}&endAt={}".format(
        type, ticker, from_date_unix, to_date_unix
    )
    response = requests.get(req)
    # convert to dataframe 
    # Check if the status code is 200
    if response.status_code == 200:
        # Convert the response to a DataFrame
        df = pd.DataFrame(response.json()['data'])
        # rename col : 
        df.columns = ["unix_time", "open", "close", "high", "low", "volume", "turnover"]
        # set col symbol : 
        df["ticker"] = ticker
        # convert unix_time to utc datetime : 
        # Convert Unix time to UTC datetime
        df["datetimeutc"] = [datetime.utcfromtimestamp(float(i)) for i in df.unix_time]
        # return 
        return df[["datetimeutc","unix_time","ticker","open","close","high","low","volume","turnover"]]
    else:
        print('Request failed with status code', response.status_code)

async def _daily_volume(ticker="BTC-USDT", type="1day", from_date=from_date_str, to_date=to_date_str):
    # Convert date string to datetime object
    from_date = datetime.strptime(from_date, '%Y-%m-%d')
    to_date = datetime.strptime(to_date, '%Y-%m-%d') - timedelta(seconds=1)
    # convert from_date and to_date to unix time :
    from_date_unix = int(from_date.timestamp())
    to_date_unix = int(to_date.timestamp())
    # request
    req = "https://api.kucoin.com/api/v1/market/candles?type={}&symbol={}&startAt={}&endAt={}".format(
        type, ticker, from_date_unix, to_date_unix
    )
    response = requests.get(req)
    # convert to dataframe 
    # Check if the status code is 200
    if response.status_code == 200:
        # Convert the response to a DataFrame
        if 'data' in response.json():
            df = pd.DataFrame(response.json()['data'])
            if len(df.columns) == 7:  # Check if the first item in data has 7 elements
                df.columns = ["unix_time", "open", "close", "high", "low", "volume", "turnover"]
                # set col symbol : 
                df["ticker"] = ticker
                # convert unix_time to utc datetime : 
                df["datetimeutc"] = [datetime.utcfromtimestamp(float(i)) for i in df.unix_time]
                # return 
                return df[["datetimeutc","unix_time","ticker","open","close","high","low","volume","turnover"]]
            else:
                print(f"Unexpected data format. Expected 7 elements, got {len(df.columns)}")
                return pd.DataFrame(columns = ["unix_time", "open", "close", "high", "low", "volume", "turnover"])
        else:
            return pd.DataFrame(columns = ["unix_time", "open", "close", "high", "low", "volume", "turnover"])
    else:
        # print('Request failed with status code', response.status_code)
        return pd.DataFrame(columns = ["unix_time", "open", "close", "high", "low", "volume", "turnover"])

async def _daily_volume_task(tickers, type="1day", from_date=from_date_str, to_date=to_date_str):
    pbar = tqdm(total=len(tickers), position=0, ncols=90)  # Set total to the number of tasks
    async def task_wrapper(ticker):
        result = await _daily_volume(ticker, type, from_date, to_date)
        pbar.update()  # Update the progress bar
        return result
    tasks = [task_wrapper(ticker[1]) for ticker in enumerate(tickers)]
    results = await asyncio.gather(*tasks)
    pbar.close()  # Close the progress bar when done
    return pd.concat(results)
        
    # pd.concat(results)

# @pf.task(name="[API] get candelsticks")
def get_daily_candlesticks(tickers = ["BTC-USDT", "ETH-USDT"],type="1day", from_date=from_date_str, to_date=to_date_str, units=100): 
    data_return = pd.DataFrame()
    # divid chunks :
    chunks = [tickers[i:i + units] for i in range(0, len(tickers), 100)]
    nest_asyncio.apply()
    for i in range(len(chunks)) :
        t = chunks[i]
        # apply nest_asyncio to allow nestedd use of asyncio0s event loop: 
        # get the event loop 
        loop = asyncio.get_event_loop()
        # print info : 
        print("starting chunk >> " + str(i + 1) + " (" + str(units) + "units)")
        # get all responses : 
        data = loop.run_until_complete(_daily_volume_task(t, type, from_date, to_date))
        # loop.stop()
        # push to data_return : 
        data_return = data_return._append(data)
    return data_return


def get_daily_candlesticks_no_async(tickers = ["BTC-USDT", "ETH-USDT"],type="1day", from_date=from_date_str, to_date=to_date_str): 
    # apply nest_asyncio to allow nestedd use of asyncio0s event loop: 
    data_all = _daily_volume_no_async(tickers[0], type, from_date, to_date)
    for i in tqdm(range(1,len(tickers))):
        data = _daily_volume_no_async(tickers[i], type, from_date, to_date)
        data_all = pd.concat([data_all, data])
    # data concat : 
    return data_all


############################################################################################################################################
#### request for symbol list  ##############################################################################################################
############################################################################################################################################

# @pf.task(name="[API] get tickers list")
def get_tickers_list(quotCurrency=None, enableTrading=True): 
    req = "https://api.kucoin.com/api/v2/symbols"
    response = requests.get(req)
    data = pd.DataFrame(response.json()['data'])
    # filter enableTrading
    data = data.loc[data["enableTrading"]==enableTrading]
    if quotCurrency!=None: 
        return data.loc[data["quoteCurrency"]==quotCurrency]
    else: 
        return data
