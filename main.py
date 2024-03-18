from src import kucoin as ku 
from src import xlsx as xs
from src import GoogleDrive as gd
from src import pg as pg 
from datetime import datetime, timedelta
import prefect as pf
import os 
import pandas as pd

mode = "update"
type = "1day"
table_name = "kucoin_candlesticks_daily"

# get date of today :
today = datetime.now().date()
yesterday = datetime.now().date() - timedelta(days=1)
# Convert to string
to_date_str = today.strftime("%Y-%m-%d")
from_date_str = yesterday.strftime("%Y-%m-%d")


# @pf.task(name="[gdrive] update file") > because the function has two sub tasks, it is not a task
def update_file_to_google_drive(data, file_name): 
    # export data : 
    # get list file to see if exists and replace :
    list_files = gd.list_files(parent_folder_id='1iTMazQALR7EgoRuxp-T3yTMXNWMUgGHX')
    if list_files.empty == False :
        file_id = list_files.loc[list_files["name"]==file_name].reset_index()
        if len(file_id) > 0 :
            gd.delete_files(file_or_folder_id=file_id["id"][0])
    # export to xslx 
    xs.export_toxlsx(data, "./" + file_name)
    # push to google drive : 
    id = gd.upload_file( file_path= "./" + file_name, folder_parent_id='1iTMazQALR7EgoRuxp-T3yTMXNWMUgGHX')
    # remove file : 
    if os.path.isfile(file_name):
        os.remove(file_name)
    print(file_name + " > exported to google drive")


@pf.task(name="[gdrive] update kucoin_all_volume file")
def add_to_kucoin_all_file(data, file_name_all):
    # get list file to see if exists and replace :
    list_files = gd.list_files(parent_folder_id='1iTMazQALR7EgoRuxp-T3yTMXNWMUgGHX')
    file_id = list_files.loc[list_files["name"]==file_name_all]
    if len(file_id) > 0 : 
        # download file : 
        gd.download_file(file_id["id"][0], destination_path = "./" + file_name_all)
        # import xlsx : 
        old_data = pd.read_excel(file_name_all, sheet_name=None)
        # get the sheet names
        sheet_names = list(old_data.keys())
        # get the first sheet data : 
        old_data = old_data[sheet_names[0]]
        # convert all columns to float64 except ticker column : 
        cols = [col for col in old_data.columns if col != 'ticker']
        old_data[cols] = old_data[cols].apply(pd.to_numeric, errors='coerce', downcast='float')
        # left join data to old data : 
        data = data.merge(old_data, on='ticker')
        # Get a list of column names, excluding 'ticker'
        cols = [col for col in data.columns if col != 'ticker']
        # Sort the list of column names
        cols.sort()
        # Add 'ticker' to the front of the list
        cols = ['ticker'] + cols
        # Reorder the DataFrame
        data = data[cols]
        if len(file_id) > 0 :
            gd.delete_files(file_or_folder_id=file_id["id"][0])
    # export to xslx 
    xs.export_toxlsx(data, "./" + file_name_all)
    # push to google drive : 
    id = gd.upload_file( file_path= "./" + file_name_all, folder_parent_id='1iTMazQALR7EgoRuxp-T3yTMXNWMUgGHX')
    # remove file : 
    if os.path.isfile(file_name_all):
        os.remove(file_name_all)


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
    # get distinct dates in data : 
    dates = data["datetimeutc"].unique().tolist()
    for d in dates : 
        df = data.loc[data["datetimeutc"]==d]
        # updating to drive : 
        file_name = "kucoin_candlesticks_" + d.strftime("%Y%m%d") + ".xlsx"
        # update file : 
        update_file_to_google_drive(df, file_name)
        # transform df to kucoin_all_volume file 
        df = df[["ticker", "volume"]]
        # rename volume col by d str 
        df.columns = ["ticker", d.strftime("%Y-%m-%d")]
        # get last version of file "kucoin_all.xlsx" : 
        file_name_all = "kucoin_all_volume.xlsx" 
        # add to all_volume_file : 
        add_to_kucoin_all_file(df, file_name_all)


########################### method 2 ##############################

@pf.task(name="[data] generate data for ric.")
def generate_file_for_ricardo(df):
    # select volume : 
    df = df[["datetimeutc", "ticker", "volume"]]
    # copy 
    df_cp = df.copy()
    # convert type of datetimeutc to string
    df_cp['datetimeutc'] = df_cp['datetimeutc'].dt.strftime('%Y-%m-%d')
    pivot_df = df_cp.pivot(index='ticker', columns='datetimeutc', values='volume')
    # resert level columns name : 
    pivot_df.reset_index(inplace=True)
    return pivot_df
    
@pf.task(name="[data] generate stat")
def generate_statistics(df, mean_lower_than = 100000, pic_max_1 = 400000, pic_max_2 = 600000): 
    # select volume : 
    df = df[["datetimeutc", "ticker", "volume", "high", "close"]].drop_duplicates()
    # convert ticker to string and volume to float : 
    df['ticker'] = df['ticker'].astype(str)
    df['volume'] = df['volume'].astype(float)
    df['close'] = df['close'].astype(float)
    df['high'] = df['high'].astype(float)
    # date for last year : 
    to_date = datetime.strptime(to_date_str, "%Y-%m-%d")
    from_date = to_date - timedelta(days=365)
    # global mean volume last year :
    groupedVol1Y = df.loc[df["datetimeutc"] >= from_date].groupby('ticker')['volume'].agg(['mean'])
    # rename mean and median by meanVolume and medianVolume 
    groupedVol1Y = groupedVol1Y.rename(columns={'mean': 'meanVolume1Year'})
    # merge first output : 
    grouped = groupedVol1Y.copy()
    # calcul diff vol at day i and mean vol last year
    df = df.merge(groupedVol1Y, on="ticker", how="left")
    df["VolumeDeltaMean1Y"] = df["volume"] - df["meanVolume1Year"]
    # is mean overall under cumstum  
    grouped['HasVolumeMean1YearUnder' + str(mean_lower_than)] = grouped['meanVolume1Year'] < mean_lower_than
    # Calculate number of days higher than custum pic_max_1
    days_above_custum = pd.DataFrame(df[df['volume'] > pic_max_1].groupby('ticker')['volume'].agg(['count', 'mean']))
    days_above_custum = days_above_custum.rename(columns={'count': 'NbDaysForVolumePicsHigherThan' + str(pic_max_1), 'mean': 'MeanVolume1YearPicsHigherThan' + str(pic_max_1)})
    grouped = grouped.merge(days_above_custum, on="ticker", how="left")
    # Calculate number of days higher than custum pic_max_2
    days_above_custum_2 = pd.DataFrame(df[df['volume'] > pic_max_2].groupby('ticker')['volume'].agg(['count', 'mean']))
    days_above_custum_2 = days_above_custum_2.rename(columns={'count': 'NbDaysForVolumePicsHigherThan' + str(pic_max_2), 'mean': 'MeanVolume1YearPicsHigherThan' + str(pic_max_2)})
    grouped = grouped.merge(days_above_custum_2, on="ticker", how="left")
    # Calculate number of days higher than 5 sigma
    days_above_5x_mean = pd.DataFrame(df[df['volume'] > 5 * df["meanVolume1Year"]].groupby('ticker')['volume'].agg(['count', 'mean']))
    days_above_5x_mean = days_above_5x_mean.rename(columns={'count': 'NbDaysForVolumePicsHigherThan5Sigma', 'mean': 'MeanVolume1YearPicsHigherThan5Sigma'})
    grouped = grouped.merge(days_above_5x_mean, on="ticker", how="left")
    # reset index : 
    grouped = grouped.reset_index()
    # add 24 last hours price stat : 
    stat24h = ku.stats_24h()
    stat24h = stat24h[["ticker", "last", 'high', 'volValue']].rename(columns={'last': 'lastPrice', 'volValue' : 'meanVolume24Hours'})
    # calcul the diff price betwenn High and lastPrice = highestSnipePrice
    stat24h['SnipePrice24Hours'] = stat24h['high'] - stat24h['lastPrice']
    # calcul the diff price betwenn High and lastPrice = highestSnipePrice
    stat24h['SnipePriceVariation24Hours'] = (stat24h['high'] - stat24h['lastPrice'])/(stat24h['high'])
    # is mean 24 h volme under cumstum  
    stat24h['HasVolumeMean24HoursUnder' + str(mean_lower_than)] = stat24h['meanVolume24Hours'] < mean_lower_than
    # left join 
    grouped = grouped.merge(stat24h[["ticker", "SnipePriceVariation24Hours",'HasVolumeMean24HoursUnder' + str(mean_lower_than)]], on="ticker",how="left")
    return grouped


@pf.flow(name = "Kucoin", log_prints=True, flow_run_name="kucoin_" + datetime.today().strftime("%Y%m%d_%H%M%S"))
def flow_kucoin_candlesticks_update_to_yesterday(type="1day",from_date_str="2021-01-01", to_date_str=datetime.now().date().strftime("%Y-%m-%d"), max_mean = 15000, min_peak_1=400000, min_peak_2=600000):
    # get ticker list 
    tickers = ku.get_tickers_list()
    # filter quotCurrency == "USDT"
    tickers = tickers.loc[tickers["quoteCurrency"] == "USDT"]
    # filter not having 3S, 3L, 2S, 2L
    tickers = tickers.loc[~tickers["symbol"].str.contains("3S|3L|2S|2L")]
    # to list
    tickers = tickers["symbol"].to_list()
    # generate data
    data = ku.get_daily_candlesticks(tickers=tickers, type=type, from_date=from_date_str, to_date=to_date_str)
        # sort_values datetimeutc ticker 
    data = data.sort_values(['datetimeutc', 'ticker'])
    # generate data for ric : 
    data_ric = generate_file_for_ricardo(data)  
        # replace all NaN by 0 : 
    data_ric = data_ric.fillna(0.0)
    file_name = "kucoin_volume.xlsx"
        # update file : 
    update_file_to_google_drive(data_ric, file_name)
        # update raw data : 
    file_name = "kucoin_history.xlsx"
    # update file : 
    update_file_to_google_drive(data, file_name)
    # generate statistics : 
    data_stat = generate_statistics(data, mean_lower_than = max_mean, pic_max_1 = min_peak_1, pic_max_2 = min_peak_2 )
    # update stats data 
    file_name = "kucoin_statistcs.xlsx"
    # update file : 
    update_file_to_google_drive(data_stat, file_name)
        

if __name__ == "__main__":
    flow_kucoin_candlesticks_update_to_yesterday()

    