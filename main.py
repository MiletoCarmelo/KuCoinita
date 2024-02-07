import time
import requests
import pandas as pd
from datetime import datetime
from openpyxl import load_workbook
import os.path
import prefect as pf

unixtime = int(time.time())


@pf.task(name="[Kc] request kucoin")
def request_kucoin():
    req = "https://api.kucoin.com/api/v1/market/allTickers?timestamp={}".format(unixtime)
    return requests.get(req)


@pf.task(name="[Kc] convert to df")
def convert_to_df(r):
    df = pd.DataFrame(r.json()['data']['ticker'])
    df['timestamp'] = r.json()['data']['time']
    df = df.sort_values(by='vol', ascending=False)
    return df


@pf.task(name="[Kc] save excel")
def save_kucoin_excel(df,name='kucoin.xlsx'):
    df['date'] = df['timestamp'].apply(lambda x: datetime.fromtimestamp(x/1000.0).strftime('%Y-%m-%d'))
    time_label = df['date'].unique()[0]
    if os.path.isfile(name) == False:
        df.to_excel(name, sheet_name='{}'.format(time_label), index=False)
    else:
        book = load_workbook(name)
        writer = pd.ExcelWriter(name, engine='openpyxl')
        writer.book = book
        writer.sheets = dict((ws.title, ws) for ws in book.worksheets)
        df.to_excel(writer, '{}'.format(time_label))
        writer.save()
        writer.close()


@pf.flow(   name="kucoin daily",
            log_prints=True,
            flow_run_name="kc_" + datetime.today().strftime("%Y%m%d_%H%M%S"))
def kc_flow():
    r = request_kucoin()
    df = convert_to_df(r)
    save_kucoin_excel(df)