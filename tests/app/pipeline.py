from datetime import datetime, timedelta
from dagster import op, In, Out
import yfinance as yf
import pandas as pd


@op(ins={"ticker": In(dagster_type=str)},
    out=Out(pd.DataFrame))
def download_data(context, ticker: str) -> pd.DataFrame:
    # Calculate start and end dates for the download
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=1)

    # Download the data
    data = yf.download(ticker, start=start_date, end=end_date, interval="1m")

    # Filter to yesterday's data only
    yesterday = (datetime.now() - timedelta(days=1)).date()
    data = data.loc[data.index.date == yesterday]

    return data


@op(ins={"data", In(dagster_type=pd.DataFrame)},
    out=Out(bool))
def validate_data(context, data: pd.DataFrame) -> bool:
    if data.empty:
        raise ValueError("Dataframe is empty")
    else:
        return True
    

@op(ins={"data", In(dagster_type=pd.DataFrame)},
    out=Out(pd.DataFrame))
def clean_data(context, data: pd.DataFrame) -> pd.DataFrame:
    # Remove Adj Close columns from Data
    data.drop("Adj Close", axis=1, inplace=True)

    # Return the updated DataFrame
    return data


@op(ins={"data", In(dagster_type=pd.DataFrame)},
    out=Out(pd.DataFrame))
def transform_data(context, data: pd.DataFrame) -> pd.DataFrame:
    # Ensure that data is not empty
    assert context.resources.validate_data(data)
    
    # Function to determine rolling VWAP
    def rolling_vwap(df):
        df['Price'] = (df['High'] + df['Low'] + df['Close']) / 3
        df['Cumulative PV'] = df['Price'] * df['Volume']
        df['Cumulative Volume'] = df['Volume'].cumsum()
        df['Rolling PV'] = df['Cumulative PV'].rolling('15min', min_periods=1).sum()
        df['Rolling Volume'] = df['Volume'].rolling('15min', min_periods=1).sum()
        vwap = df['Rolling PV'] / df['Rolling Volume']
        return vwap
    
    # Add the rolling VWAP to the DataFrame
    data['VWAP'] = rolling_vwap(data)
    
    # Forward fill the last VWAP rows to replace NaN values
    data['VWAP'].ffill(inplace=True)
    
    # Calculate the cumulative dollar value of all trades
    dollar_value = (data['Close'] * data['Volume']).cumsum()
    
    # Add the dollar value column to the data DataFrame
    data['DollarValue'] = dollar_value
    
    # Return the updated DataFrame
    return data

@op(ins={"data", In(dagster_type=pd.DataFrame)})
def write_to_csv(context, data):
    # Get the daily date of the data
    filepath = f"./output/{str(data.index[0].date())}.csv"
    
    # Write the transformed data to a CSV file
    data.to_csv(filepath)

    # Log a message to confirm that the data has been written to the file
    context.log.info(f"Data written to file: {filepath}")