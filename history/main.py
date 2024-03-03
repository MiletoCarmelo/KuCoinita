# get the unixtime 
import time
import requests
import pandas as pd
from datetime import datetime
import os
import sys
sys.path.insert(0, './outputs')
sys.path.append('./src')
# import google_drive module
import src.GoogleDrive as gd
unixtime = int(time.time())
# create api request : 
req = "https://api.kucoin.com/api/v1/market/allTickers?timestamp={}".format(unixtime)
# request the api
r = requests.get(req)
# convert to dataframe
df = pd.DataFrame(r.json()['data']['ticker'])
# get time stamp 
df['timestamp'] = r.json()['data']['time']
# arrange by vol desc 
df = df.sort_values(by='vol', ascending=False)
# get date from timestamp column
df['date'] = df['timestamp'].apply(lambda x: datetime.fromtimestamp(x/1000.0).strftime('%Y-%m-%d'))
# convert columns type to numeric
df['symbol'] = df['symbol'].astype('str')
df['symbolName'] = df['symbolName'].astype('str')
df['buy'] = df['buy'].astype('float')
df['bestBidSize'] = df['bestBidSize'].astype('float')
df['sell'] = df['sell'].astype('float')
df['bestAskSize'] = df['bestAskSize'].astype('float')
df['changePrice'] = df['changePrice'].astype('float')
df['changeRate'] = df['changeRate'].astype('float')
df['high'] = df['high'].astype('float')
df['low'] = df['low'].astype('float')
df['vol'] = df['vol'].astype('float')
df['volValue'] = df['volValue'].astype('float')
df['last'] = df['last'].astype('float')
df['averagePrice'] = df['averagePrice'].astype('float')
df['takerFeeRate'] = df['takerFeeRate'].astype('float')
df['makerFeeRate'] = df['makerFeeRate'].astype('float')
df['takerCoefficient'] = df['takerCoefficient'].astype('float')
df['makerCoefficient'] = df['makerCoefficient'].astype('float')
df['timestamp'] = df['timestamp'].astype('int')
# convert date column to datetime with format '%Y-%m-%d'
df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
# get time string for label sheets : 
time_label = df['date'].unique()[0]
# if file kucoin_daily.xlsm exists in google drive download it
list_file = gd.list_files('1iTMazQALR7EgoRuxp-T3yTMXNWMUgGHX')
# if list_file is null replace it with empty dataframe with name column empty: 
if list_file.empty:
    list_file = pd.DataFrame(columns=['name', 'id'])
if 'kucoin_daily.xlsx' in list_file.name.values:
    file_id = list_file[list_file.name == 'kucoin_daily.xlsx'].id.values[0]
    gd.download_file(file_id, './outputs/kucoin_daily.xlsx')
if os.path.isfile('./outputs/kucoin_overall.xlsx') == True:
    os.remove('./outputs/kucoin_overall.xlsx')
if 'kucoin_daily.xlsx' in list_file.name.values:
    file_id = list_file[list_file.name == 'kucoin_overall.xlsx'].id.values[0]
    gd.download_file(file_id, './outputs/kucoin_overall.xlsx')
if os.path.isfile('./outputs/kucoin_daily.xlsx') == False:
    df.to_excel('./outputs/kucoin_daily.xlsx',index=False, sheet_name= time_label.strftime('%Y-%m-%d'))
else:
    with pd.ExcelWriter('./outputs/kucoin_daily.xlsx', mode="a", engine="openpyxl", if_sheet_exists='overlay') as writer:
        df.to_excel(writer, sheet_name=time_label.strftime('%Y-%m-%d'), index=False) 
# import old data
if os.path.isfile('./outputs/kucoin_overall.xlsx'):
    old_data = pd.read_excel('./outputs/kucoin_overall.xlsx', sheet_name=None)
    # get the sheet names
    sheet_names = list(old_data.keys())
    # get the first sheet data : 
    old_data = old_data[sheet_names[0]]
    old_data = old_data._append(df)
else : 
    old_data = df
# sort by date desc and symbol asc
old_data.sort_values(by=['date', 'symbol'], ascending=[False, True], inplace=True)
# remove file if exists
if os.path.isfile('./outputs/kucoin_overall.xlsx'):
    os.remove('./outputs/kucoin_overall.xlsx')
# write to excel
with pd.ExcelWriter('./outputs/kucoin_overall.xlsx', engine="openpyxl") as writer:
    old_data.to_excel(writer, sheet_name="overall", index=False)
# get list files id in folder "data_history" in google drive : 
list_ids = gd.list_files('1iTMazQALR7EgoRuxp-T3yTMXNWMUgGHX')
# if list_ids is none replace it with dataframe of id column empty : 
if list_ids.empty:
    list_ids = pd.DataFrame(columns=['name', 'id'])
for i in list_ids.id:
    gd.delete_files(i)
# upload files to google drive
id1 = gd.upload_file( file_path='./outputs/kucoin_daily.xlsx', folder_parent_id='1iTMazQALR7EgoRuxp-T3yTMXNWMUgGHX')
id2 = gd.upload_file( file_path='./outputs/kucoin_overall.xlsx', folder_parent_id='1iTMazQALR7EgoRuxp-T3yTMXNWMUgGHX')