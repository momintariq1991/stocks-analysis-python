import pandas as pd
import database as db
import technicals
import time

pd.set_option('display.max_columns', 25)
pd.set_option('display.width', 400)
date = 'date'
open = 'open'
high = 'high'
low = 'low'
close = 'close'
volume = 'volume'

def load_data_from_db(connection, ticker):
    cursor = connection.cursor()
    cursor.execute('select * from ' + ticker + '_historical_prices')
    historical_prices_tuple = cursor.fetchall()
    return historical_prices_tuple

def build_dataframe(data, ticker):
    df = pd.DataFrame(data=data, columns=[date, open, high, low, close, volume])
    df = df.iloc[::-1]
    df.index = range(len(df))
    df = technicals.ema(df, timeperiod=50)
    df = technicals.rsi(df, timeperiod=14)
    df = technicals.macd(df, fastperiod=12, slowperiod=26, signalperiod=9)
    df = technicals.sar(df, acceleration=.02, maximum=.2)
    df = technicals.aroon(df, timeperiod=24)
    df = technicals.adosc(df, fastperiod=3, slowperiod=10)
    df = technicals.ema_decision_col(df)
    df = technicals.rsi_decision_col(df)
    df = technicals.macd_decision_col(df)
    df = technicals.sar_decision_col(df)
    df = technicals.aroon_decision_col(df)
    df = technicals.adosc_decision_col(df)
    df = technicals.trade_enter_exit(df)
    # print(help(talib.ADOSC))
    # df.to_csv('csvs/' + ticker + '_analysis.csv', index=False)
    writer = pd.ExcelWriter('excels/' + ticker + '.xlsx')
    df.to_excel(writer, index=False)
    writer.save()
    print('generated csv for ' + ticker)
    return df

if __name__ == '__main__':
    start_time = time.time()
    connection = db.open_connection()
    tickers = ['nflx', 'amzn', 'aapl', 'fb', 'ge']
    for ticker in tickers:
        data = load_data_from_db(connection, ticker)
        df = build_dataframe(data, ticker)
        print('processed ' + ticker)
        # print(df)
    db.close_connection(connection)
    elapsed_time = time.time() - start_time
    print('took ' + str(elapsed_time / 60) + ' minutes to execute')