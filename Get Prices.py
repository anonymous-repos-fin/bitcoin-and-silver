import os
import numpy as np
import pandas as pd
import yfinance as yf
import pandas_datareader.data as web

def get_stock(ticker, start_date, end_date, s_window, l_window):
    try:
        #yf.pdr_override()  # Uncomment if necessary
        df = yf.download(ticker, start=start_date, end=end_date)
        # Add additional columns
        if 'Adj Close' not in df.columns:
            df['Adj Close'] = df['Close']

        #df = web.get_data_yahoo(ticker, start=start_date, end=end_date)
        df['Return'] = df['Adj Close'].pct_change()
        df['Return'].fillna(0, inplace=True)
        df['Date'] = df.index
        df['Date'] = pd.to_datetime(df['Date'])
        df['Month'] = df['Date'].dt.month
        df['Year'] = df['Date'].dt.year
        df['Day'] = df['Date'].dt.day
        for col in ['Open', 'High', 'Low', 'Close', 'Adj Close']:
            df[col] = df[col].round(2)
        df['Weekday'] = df['Date'].dt.day_name()
        df['Week_Number'] = df['Date'].dt.strftime('%U')
        df['Year_Week'] = df['Date'].dt.strftime('%Y-%U')
        df['Short_MA'] = df['Adj Close'].rolling(window=s_window, min_periods=1).mean()
        df['Long_MA'] = df['Adj Close'].rolling(window=l_window, min_periods=1).mean()
        col_list = ['Date', 'Year', 'Month', 'Day', 'Weekday',
                    'Week_Number', 'Year_Week', 'Open',
                    'High', 'Low', 'Close', 'Volume', 'Adj Close',
                    'Return', 'Short_MA', 'Long_MA']
        num_lines = len(df)
        df = df[col_list]
        print('Read', num_lines, 'lines of data for ticker:', ticker)
        return df
    except Exception as error:
        print(error)
        return None

tickers = ["SI=F"]
start_date = '2014-01-01'
end_date = '2024-12-31'
input_dir = os.getcwd()

for ticker in tickers:
    try:    
        output_file = os.path.join(input_dir, f'Data/{ticker}.csv')
        df = get_stock(ticker, start_date, end_date, s_window=14, l_window=50)
        if df is not None:
            df.to_csv(output_file, index=False)
            print(f'Wrote {len(df)} lines to file: {output_file}')
    except Exception as e:
        print(e)
        print('Failed to get Yahoo stock data for ticker:', ticker)
