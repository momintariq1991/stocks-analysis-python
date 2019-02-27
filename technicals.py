import pandas as pd
import talib

date = 'date'
open = 'open'
high = 'high'
low = 'low'
close = 'close'
volume = 'volume'

#################################################################################
# INDICATORS
#################################################################################

#Moving Average
def ma(df, n):
    ma = pd.Series(talib.SMA(df[close], timeperiod=n), name='sma')
    df = df.join(ma)
    return df

#Exponential Moving Average
def ema(df, timeperiod):
    ema = pd.Series(talib.EMA(df[close], timeperiod=timeperiod), name='ema')
    df = df.join(ema)
    return df

#Relative Strength Index
def rsi(df, timeperiod):
    rsi = pd.Series(talib.RSI(df[close], timeperiod=timeperiod), name='rsi')
    df = df.join(rsi)
    return df

#Bollinger Bands
def bollinger_bands(df, n):
    upper, middle, lower = talib.BBANDS(df[close], n, 2, 2)
    df = df.join(pd.Series(upper, name='boll_upper_' + str(n)))
    df = df.join(pd.Series(middle, name='boll_middle_' + str(n)))
    df = df.join(pd.Series(lower, name='boll_lower_' + str(n)))
    return df

#Moving Average Convergence Divergence
def macd(df, fastperiod, slowperiod, signalperiod):
    macd, macd_signal, macd_hist = talib.MACD(df[close], fastperiod=fastperiod, slowperiod=slowperiod, signalperiod=signalperiod)
    df = df.join(pd.Series(macd, name='macd'))
    df = df.join(pd.Series(macd_signal, name='macd_signal'))
    df = df.join(pd.Series(macd_hist, name='macd_hist'))
    return df

#Parabolic SAR
def sar(df, acceleration, maximum):
    sar = talib.SAR(df[high], df[low], acceleration=acceleration, maximum=maximum)
    df = df.join(pd.Series(sar, name='sar'))
    return df

#AROON
def aroon(df, timeperiod):
    aroon_down, aroon_up = talib.AROON(df[high], df[low], timeperiod=timeperiod)
    df = df.join(pd.Series(aroon_down, name='aroon_down'))
    df = df.join(pd.Series(aroon_up, name='aroon_up'))
    return df

#Accumulation Distribution Oscillator
def adosc(df, fastperiod, slowperiod):
    adosc = talib.ADOSC(df[high], df[low], df[close], df[volume], fastperiod=fastperiod, slowperiod=slowperiod)
    df = df.join(pd.Series(adosc, name='adosc'))
    return df

#Average True Range
def atr(df, n):
    atr = talib.ATR(df[high], df[low], df[close], timeperiod=n)
    df = df.join(pd.Series(atr, name='atr'))
    return df

#Absolute Price Oscillator
def apo(df, fp, sp):
    apo = talib.APO(df[close], fastperiod=fp, slowperiod=sp)
    df = df.join(pd.Series(apo, name='apo'))
    return df

#Commodity Channel Index
def cci(df, n):
    cci = talib.CCI(df[high], df[low], df[close], timeperiod=n)
    df = df.join(pd.Series(cci, name='cci'))
    return df

#Average Directional Movement Index
def adx(df, n):
    adx = talib.ADX(df[high], df[low], df[close], timeperiod=n)
    df = df.join(pd.Series(adx, name='adx'))
    return df

#Chande Momentum Oscillator
def cmo(df, n):
    cmo = talib.CMO(df[close], timeperiod=n)
    df = df.join(pd.Series(cmo, name='cmo'))
    return df

#Money Flow Index
def mfi(df, n):
    mfi = talib.MFI(df[high], df[low], df[close], df[volume], timeperiod=n)
    df = df.join(pd.Series(mfi, name='mfi'))
    return df

#################################################################################
# INDICATORS DECISIONS
#################################################################################

def ema_decision_col(df):
    df['ema_decision'] = df.apply(ema_decision_row, axis=1)
    return df

def ema_decision_row(row):
    if float(row['close']) > row['ema']:
        val = 'buy'
    else:
        val = 'sell'
    return val

def macd_decision_col(df):
    df['macd_decision'] = df.apply(macd_decision_row, axis=1)
    return df

def macd_decision_row(row):
    if row['macd_hist'] > 0:
        val = 'buy'
    else:
        val = 'sell'
    return val

def rsi_decision_col(df):
    df['rsi_decision'] = df.apply(rsi_decision_row, axis=1)
    return df

def rsi_decision_row(row):
    if row['rsi'] > 50:
        val = 'buy'
    else:
        val = 'sell'
    return val

def aroon_decision_col(df):
    df['aroon_decision'] = df.apply(aroon_decision_row, axis=1)
    return df

def aroon_decision_row(row):
    if row['aroon_up'] > row['aroon_down']:
        val = 'buy'
    else:
        val = 'sell'
    return val

def sar_decision_col(df):
    df['sar_decision'] = df.apply(sar_decision_row, axis=1)
    return df

def sar_decision_row(row):
    if row['sar'] < float(row['close']):
        val = 'buy'
    else:
        val = 'sell'
    return val

def adosc_decision_col(df):
    df['adosc_decision'] = df.apply(adosc_decision_row, axis=1)
    return df

def adosc_decision_row(row):
    if row['adosc'] > 0:
        val = 'buy'
    else:
        val = 'sell'
    return val

#################################################################################
# TRADE ENTRY / EXIT DECISIONS
#################################################################################

def trade_enter_exit(df):
    df['trade'] = ''
    for index, row in df.iterrows():
        if index > 50:
            prev_row = df.iloc[index-1]
            prev_ema_decision = prev_row['ema_decision']
            prev_rsi_decision = prev_row['rsi_decision']
            prev_macd_decision = prev_row['macd_decision']
            prev_sar_decision = prev_row['sar_decision']
            prev_aroon_decision = prev_row['aroon_decision']
            prev_adosc_decision = prev_row['adosc_decision']
            prev_decision_array = [prev_ema_decision, prev_rsi_decision, prev_macd_decision, prev_sar_decision, prev_aroon_decision, prev_adosc_decision]

            curr_ema_decision = row['ema_decision']
            curr_rsi_decision = row['rsi_decision']
            curr_macd_decision = row['macd_decision']
            curr_sar_decision = row['sar_decision']
            curr_aroon_decision = row['aroon_decision']
            curr_adosc_decision = row['adosc_decision']
            curr_decision_array = [curr_ema_decision, curr_rsi_decision, curr_macd_decision, curr_sar_decision, curr_aroon_decision, curr_adosc_decision]
            if curr_decision_array.count('buy') == 6 and prev_decision_array.count('buy') < 6:
                df.set_value(index, 'trade', 'enter')

            if curr_decision_array.count('buy') < 6 and prev_decision_array.count('buy') == 6:
                df.set_value(index, 'trade', 'exit')
    return df