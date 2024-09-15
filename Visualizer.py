import numpy as np
import matplotlib.dates as mpdates
import matplotlib.pyplot as plt
from mplfinance.original_flavor import candlestick_ohlc
from MySQL import SqlConnect
import talib


def Setting(name):
    sql = SqlConnect()
    df = sql.fetch_data(name)
    df = df[-300:]
    dohlc, volume = Ohlc_Volume(df)
    rsi = TechIndicator(df)
    Plot_chart(dohlc, volume, rsi, name)

def TechIndicator(df):
    # 'close' 열을 float로 변환하여 talib에 전달
    close_prices = df['close'].astype(float).values
    rsi = talib.RSI(close_prices, timeperiod=14)
    return rsi


def Ohlc_Volume(df):
    volume = df['volume']
    x = np.arange(len(df.index))
    ohlc = df[['open', 'high', 'low', 'close']].astype(int).values
    dohlc = np.hstack((np.reshape(x, (-1, 1)), ohlc))
    return dohlc, volume


def Plot_chart(dohlc,volume,rsi,name):
    fig, axes = plt.subplots(3, 1, figsize=(6, 4))
    candlestick_ohlc(axes[0], dohlc, width=0.6, colordown='b', colorup='r', alpha=1)
    axes[0].set_title('Candle Price Chart')


    for term, color in zip([5,10,20,60,120,240],['red','blue','yellow','green','black']):
        close_prices = dohlc[:, 4].astype(float)  # 'close' 가격 추출
        ma = talib.MA(close_prices, timeperiod=term)  # 10일 이동 평균 계산

        axes[0].plot(np.arange(len(ma)), ma, label=f'{term}-Day MA', color=color, linestyle='-')

    date_format = mpdates.DateFormatter('%Y/%m/%d')

    years = mpdates.YearLocator()
    axes[0].xaxis.set_major_locator(years)
    axes[0].xaxis.set_major_formatter(date_format)

    fig.autofmt_xdate()

    axes[1].bar(range(len(dohlc)), volume, color='k', width=0.6, align='center')  # 여기에 거래량 차트 데이터 추가 가능
    axes[1].set_title('Volume Chart')

    axes[2].plot(np.arange(len(rsi)), rsi, label=f'rsi 14', color='r')
    axes[2].set_title('14day RSI')

    fig.tight_layout()

    fig.savefig(f"img/chart_{name}.png", dpi=300)

    plt.show()
