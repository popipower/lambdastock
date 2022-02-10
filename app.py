import math

import numpy as np
import pandas as pd
import pandas_ta as ta
import boto3
import json
from datetime import datetime, date, timedelta
import pytz
import os
from numerize import numerize

htmlResponseStart = "<html><head><style>body,html{font-family:Verdana,sans-serif;font-size:2px;line-height:1}html{" \
                    "overflow-x:scroll;}.w3-amber,.w3-hover-amber:hover{" \
                    "color:#000!important;background-color:#ffc107!important}.w3-hover-light-green:hover,.w3-light-green{" \
                    "color:#000!important;background-color:#8bc34a!important}.w3-deep-orange,.w3-hover-deep-orange:hover{" \
                    "color:#fff!important;background-color:#ff5722!important}.w3-responsive{" \
                    "display:block;overflow-x:scroll;}table.w3-table-all,table.ws-table-all{" \
                    "margin:1px;align:center}.ws-table-all{" \
                    "border-collapse:collapse;border-spacing:0;width:40%;display:table;border:1px solid " \
                    "#ccc;align:center}.ws-table-all tr{border-bottom:1px solid #ddd}.ws-table-all tr:nth-child(odd){" \
                    "background-color:#fff}.ws-table-all tr:nth-child(even){background-color:#e7e9eb}.ws-table-all td," \
                    ".ws-table-all th{padding:1px 1px;display:table-cell;text-align:center;vertical-align:top;font-size:11px; border: 1px solid lightgrey}.ws-table" \
                    "-all " \
                    "td:first-child,.ws-table-all th:first-child{padding-left:1px}.w3-blue-grey," \
                    ".w3-hover-blue-grey:hover" \
                    ",.w3-blue-gray,.w3-hover-blue-gray:hover{color:#fff!important;background-color:#607d8b!important}" \
                    ".w3-light-grey,.w3-hover-light-grey:hover,.w3-light-gray,.w3-hover-light-gray:hover" \
                    "{color:#000!important;background-color:#f1f1f1!important}" \
                    "</style></head><body><div " \
                    "class=\"w3-responsive\"><table class=\"ws-table-all ws-green\"> <tbody><tr class =\"w3-blue-grey\">" \
                    "<th style=\"width:10%\">↑↓</th> <th " \
                    "style=\"width:14%\">Tick</th> <th " \
                    "style=\"width:14%\">CMP</th> <th style=\"width:14%\">EMA14</th> <th style=\"width:14%\">EMA21" \
                    "</th> <th style=\"width:14%\">RSI14</th><th style=\"width:10%\">Sig</th> <th style=\"width:10%\">Vol</th> </tr> "

htmlResponseEnd = "</tbody></table></div></body></html>"


def handler(event, context):
    start = datetime.now()
    df = pd.DataFrame()

    envTicker = os.environ.get('tickers', "AAPL,^IXIC")
    isRawResponse = os.environ.get('isRawResponse', "N")

    recipient = os.environ.get('recipient', '')
    sender = os.environ.get('sender', 'Stock Notification<>')
    lastCloseTimeWithZone = os.environ.get('lastCloseTimeWithZone', ' 15:30:00-0500')

    if envTicker is not None:
        tickers = envTicker.split(",")
    else:
        tickers = ["AAPL", "AMAT", "AMD", "CHWY", "CAR", "CELH", "DIS", "DOCU", "FB", "NKE", "ORCL", "KO", "MU", "PENN",
                   "SBUX", "SPY", "V", "WMT"]
    final_signal = ""
    for ticker in tickers:
        final_signal = processTicker(df, final_signal, ticker, isRawResponse, lastCloseTimeWithZone)
    if isRawResponse == 'Y':
        notify(final_signal)
    else:
        final_signal = htmlResponseStart + final_signal + htmlResponseEnd
        send_html_email(final_signal, recipient, sender)

    end = datetime.now()
    print((end - start))
    print(final_signal)
    return final_signal


def listToString(s):
    str1 = ""
    for ele in s:
        str1 += ele
    return str1


def findPercentageChangeToday(currentPrice, df, lastCloseTimeWithZone):
    today = date.today()
    yesterday = today - timedelta(days=1)
    while True:
        pastValueDf = df[df["index"] == pd.Timestamp(yesterday.strftime("%Y-%m-%d") + lastCloseTimeWithZone)]
        if pastValueDf.empty:
            yesterday = yesterday - timedelta(days=1)
        else:
            pastValue = pastValueDf["Close"].values[-1]
            break
    change = (currentPrice - pastValue) * 100 / pastValue
    return change


def processTicker(df, final_signal, ticker, isRawResponse, lastCloseTimeWithZone):
    df = df.ta.ticker(ticker, interval="1h", period="6mo").reset_index()


    # help(ta.donchian(df))

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
    currentPrice = str(df["Close"].values[-1])
    percentageChangeToday = findPercentageChangeToday(float(currentPrice), df, lastCloseTimeWithZone)
    currentVolume = df["Volume"].values[-1]
    volumeChange = (currentVolume - df["Volume"].values[-2])*100/df["Volume"].values[-2]
    if isRawResponse == 'Y':
        final_signal = final_signal + ticker + "-> " + rating + "," + str(df["EMA_14"].values[-1]) + \
                       "," + str(df["EMA_21"].values[-1]) + "," + str(df["RSI_14"].values[-1]) + "\n"

    else:
        percentageChangeTodayCss = "\"w3-deep-orange\"" if percentageChangeToday < 0 else "\"w3-light-green\""
        volumeChangeCss = "\"w3-deep-orange\"" if volumeChange < 0 else "\"w3-light-green\""
        if rating == 'BUY':
            selectCSS = "\"w3-light-green\""
        elif rating == 'SELL':
            selectCSS = "\"w3-deep-orange\""
        else:
            selectCSS = "\"w3-amber\""
        final_signal = final_signal + "<tr><td class =" + percentageChangeTodayCss + ">" + str(math.fabs(round(percentageChangeToday,1))) \
                       + "</td><td class =\"w3-light-grey\">" + ticker + "</td><td class =\"w3-light-grey\">" \
                       + currentPrice + "</td><td class =\"w3-light-grey\">" + \
                       str(df["EMA_14"].values[-1]) + "</td><td class =\"w3-light-grey\">" + \
                       str(df["EMA_21"].values[-1]) + "</td><td class =\"w3-light-grey\">" + str(
            df["RSI_14"].values[-1]) + \
                       "</td><td class=" + selectCSS + ">" + rating + "</td><td class=" + volumeChangeCss + ">" + str(numerize.numerize(np.float64(currentVolume).item(),2)) + "</td></tr>"

    return final_signal


# print(df[df["index"]== pd.Timestamp(year = 2022,  month = 2, day = 5,
#                 hour = 15, minute= 30, second = 0, tz = 'EST')])
def notify(final_signal):
    pdt = pytz.timezone('America/Los_Angeles')
    datetime_ist = datetime.now(pdt)
    datetime_ist.strftime('%Y/%m/%d %H:%M')
    client = boto3.client('sns')
    response = client.publish(
        TargetArn='arn:aws:sns:us-east-2:123:stocks',
        Message=json.dumps({'default': json.dumps(final_signal),
                            'email': final_signal}),
        Subject='Stock Update at ' + str(datetime_ist),
        MessageStructure='json'
    )
    # print("Response: {}".format(response))


def send_html_email(html, recipient, sender):
    ses_client = boto3.client("ses", region_name="us-east-2")
    CHARSET = "UTF-8"
    pdt = pytz.timezone('America/Los_Angeles')
    datetime_ist = datetime.now(pdt)

    response = ses_client.send_email(
        Destination={
            "ToAddresses": recipient.split(","),
        },
        Message={
            "Body": {
                "Html": {
                    "Charset": CHARSET,
                    "Data": html,
                }
            },
            "Subject": {
                "Charset": CHARSET,
                "Data": "Stock Update at " + datetime_ist.strftime('%Y-%m-%d %H:%M'),
            },
        },
        Source=sender,
    )


#handler(None, None)
# df_final = df_final.concat(tickers)
