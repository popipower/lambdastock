import pandas as pd
import pandas_ta as ta
import boto3
import json
from datetime import datetime
import pytz
import os

def handler(event, context):
    start = datetime.now()
    df = pd.DataFrame()
    envTicker = os.environ['tickers']
    if envTicker:
        tickers = envTicker.split(",")
    else:
        tickers = ["AAPL", "AMAT", "AMD", "CHWY", "CAR", "CELH", "DIS", "DOCU", "FB", "NKE", "ORCL", "KO", "MU", "PENN", "SBUX", "SPY", "V", "WMT"]

    header = "           Rating, EMA14, EMA21, RSI \n"
    final_signal = header;
    # final_signal = Parallel(n_jobs=10)(delayed(processTicker)(df, final_signal, ticker) for ticker in tickers)
    for ticker in tickers:
        final_signal = processTicker(df, final_signal, ticker)
    # final_signal = str(header+listToString(final_signal))
    notify(final_signal)
    end = datetime.now()
    print((end - start))
    return final_signal


def listToString(s):
    str1 = ""
    for ele in s:
        str1 += ele
    return str1


def processTicker(df, final_signal, ticker):
    df = df.ta.ticker(ticker, interval="1h", period="6mo")
    pd.set_option('display.max_columns', None)
    rsi = ta.rsi(df["Close"])
    emaSlow = ta.ema(df["Close"], length=50)
    emaFast = ta.ema(df["Close"], length=14)
    emaMid = ta.ema(df["Close"], length=21)
    df = pd.concat([df, rsi, emaFast, emaMid, emaSlow], axis=1)
    df = df.round(2)
    df["BEAR"] = df["EMA_14"] < df["EMA_21"]
    df["BULL"] = df["EMA_14"] > df["EMA_21"]
    rating = "SELL" if (df["BEAR"].values[-1] and df["RSI_14"].values[-1] < 49) else "BUY" \
        if (df["BULL"].values[-1] and df["RSI_14"].values[-1] > 49) else "NEU"
    final_signal = final_signal + ticker + "-> " + rating + " , " + str(df["EMA_14"].values[-1]) + \
                   " , " + str(df["EMA_21"].values[-1]) + " , " + str(df["RSI_14"].values[-1]) + "\n"
    return final_signal


def notify(final_signal):
    pdt = pytz.timezone('America/Los_Angeles')
    datetime_ist = datetime.now(pdt)
    datetime_ist.strftime('%Y:%m:%d %H:%M')
    client = boto3.client('sns')
    response = client.publish(
        TargetArn='arn:aws:sns:us-east-2:863707103675:stocks',
        Message=json.dumps({'default': json.dumps(final_signal),
                            'email': final_signal}),
        Subject='Stock Update at ' + str(datetime_ist),
        MessageStructure='json'
    )
    #print("Response: {}".format(response))


#handler(None, None);
# df_final = df_final.concat(tickers)
