from src import kucoin as ku 
from src import xlsx as xs
from src import GoogleDrive as gd
from src import pg as pg 
from datetime import datetime, timedelta
import prefect as pf
import os 
import pandas

mode = "update"
type = "1day"
table_name = "kucoin_candlesticks_daily"

# get date of today :
today = datetime.now().date()
yesterday = datetime.now().date() - timedelta(days=1)
# Convert to string
to_date_str = today.strftime("%Y-%m-%d")
from_date_str = yesterday.strftime("%Y-%m-%d")


# @pf.task(name="[gdrive] update file")
def update_file_to_google_drive(data, file_name): 
    # export data : 
    # get list file to see if exists and replace :
    list_files = gd.list_files(parent_folder_id='1iTMazQALR7EgoRuxp-T3yTMXNWMUgGHX')
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


# @pf.task(name="[gdrive] update kucoin_all_volume file")
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

# @pf.task(name="[data] generate data for ric.")
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
    
# @pf.task(name="[data] generate stat")
def generate_statistics(df, sigma=3, median_lower_than = 500): 
    # select volume : 
    df = df[["datetimeutc", "ticker", "volume"]]
    # convert ticker to string and volume to float : 
    df['ticker'] = df['ticker'].astype(str)
    df['volume'] = df['volume'].astype(float)
    # group by and calcul median, mean, number of days higher than 3 times the mean, number de days higher than 3 time the median : 
    grouped = df.groupby('ticker')['volume'].agg(['mean', 'median'])
    # Calculate number of days higher than 3 times the mean
    grouped['days_above_3x_mean'] = df[df['volume'] > 3 * df.groupby('ticker')['volume'].transform('mean')].groupby('ticker')['volume'].count()
    # Calculate number of days higher than 3 times the median
    grouped['days_above_3x_median'] = df[df['volume'] > 3 * df.groupby('ticker')['volume'].transform('median')].groupby('ticker')['volume'].count()
    # add a column ver means < 500 
    grouped['median' + str(median_lower_than)] = grouped['median'] < median_lower_than
    grouped['mean' + str(median_lower_than)] = grouped['mean'] < median_lower_than
    # reset index
    grouped = grouped.reset_index()
    # sort by mean
    grouped = grouped.sort_values("mean")
    return grouped[["ticker", "median"+ str(median_lower_than), "mean"+ str(median_lower_than), "mean", "median", "days_above_3x_mean", "days_above_3x_median"]]


@pf.flow(name = "Kucoin", log_prints=True, flow_run_name="kucoin_" + datetime.today().strftime("%Y%m%d_%H%M%S"))
def flow_kucoin_candlesticks_update_to_yesterday(type=type,from_date_str="2021-01-01", to_date_str=to_date_str):
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
    os.remove(file_name)
    # generate statistics : 
    data_stat = generate_statistics(data, sigma=3, median_lower_than = 500)
    # update stats data 
    file_name = "kucoin_statistcs.xlsx"
    # update file : 
    update_file_to_google_drive(data_stat, file_name)
    os.remove(file_name)
    

if __name__ == "__main__":
    flow_kucoin_candlesticks_update_to_yesterday()

    