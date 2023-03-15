# Data Pipelines in Python

Data pipelines are an essential tool for managing and processing data in modern software systems. A data pipeline is a set of interconnected components that process data as it flows through the system. These components can include data sources, write down functions, transformation functions, and other data processing operations, such as validation and cleaning. Pipelines are a way to automate the process of collecting, transforming, and analyzing data. They are a series of steps that take raw data, clean it, process it, and store it in a way that can be easily analyzed or used in other applications. Pipelines are particularly useful when dealing with large amounts of data or when working with data that needs to be constantly updated.

Pipelines help in automating the data processing, making it easier and quicker to collect, store, and analyze data. By breaking down data processing into smaller, more manageable tasks, pipelines make it easier to maintain and troubleshoot the system.

In this blog we will look at building a simple data pipeline using `dagster` and `yfinance`, collecting some simple market data, validating that the expected data is present, cleaning and enriching it, before saving the resultant data to disk and using it in a notebook to represent an end user accessing the data we would schedule for collection in our pipeline.

Installation of libraries used in our `venv`:
```
pip install pandas yfinance dagster dagit
```

## Building a Simple Pipeline

Before beginning to code our pipeline there are `definitions` available for input and outputs of function in dagster, that allow high level type validation as the pipeline runs.

We can specify input and output definitions for any `op` (explained in the next section) using the `In` and `Out` classes from the dagster library. This helps to ensure that the inputs and outputs of the `op` are correctly typed and validated by the dagster framework at runtime.

### Data Sourcing

The first step in building our data pipeline is to define a data source that downloads historical data for a specific ticker using yfinance. We will use the `yfinance.download()` function to pull historical data in 1 minute increments for that ticker.

Our first `dagster` component is an `op`. They are reusable components that can be copmbined together to create more complex pipelines, they take one or more inputs, do a task and produce one or more outputs. 

An `op` function takes two arguments: context and inputs. The context argument provides access to information about the pipeline and the execution environment, such as loggers, configuration data, and metadata about the pipeline. The `ins` argument is a dictionary that contains the inputs to the op.

The output of an op is defined using the `Out` class from the dagster library. An output definition specifies the name and type of the output produced by the `op`.

Here is our simple data sourcing function:

```
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
```

This will allow us to collect our data from `yfinance` for as many tickers as we want in a reusable fashion.

### Validating the Data

Obviously data validation is a huge aspect of data engineering but beyond the scope of this blog, as such our data validation step is very lightweight and a placeholder for more robust testing in a production environment. 

Here we will just test that the returned data from our first step is not an empty DataFrame.

```
@op(out=Out(bool))
def validate_data(context, data):
    if data.empty:
        raise ValueError("Dataframe is empty")
    else:
        return True
```

This op takes a single input argument, `data`, which should be the DataFrame returned by the `download_data` op. 

### Cleaning and Enriching the Dataset

We also want to provide some simple cleaning functionality, for example we have no need of the "Adj Close" column returned to us by `yfinance`. This can be removed as a cleaning operation.

```
@op(ins={"data", In(dagster_type=pd.DataFrame)},
    out=Out(pd.DataFrame))
def clean_data(context, data: pd.DataFrame) -> pd.DataFrame:
    # Remove Adj Close columns from Data
    data.drop("Adj Close", axis=1, inplace=True)

    # Return the updated DataFrame
    return data
```

Now that we have some data that is ready to work with, we can add our own metrics and enrich the DataFrame before saving it down.

Currently our data looks like this:

|Datetime |Open	|High	|Low	|Close	|Volume|	
|--|--|--|--|--|--|				
|2023-03-14 09:30:00-04:00|	295.970001|	297.450012|	295.970001|	296.579987|	110684|
|2023-03-14 09:31:00-04:00|	296.579987|	296.789886|	294.809998|	295.149994|	22967|
|2023-03-14 09:32:00-04:00|	295.170013|	295.549988|	294.679993|	295.190002|	22126|
|...|	...|	...|	...|	...|	...|

Our next step is transforming the data with some enrichments. We will add a cumulative value of all the trades for a ticker along with a 15 minute rolling VWAP, and add tehm to the DataFrame.

```
@op(ins={"data", In(dagster_type=pd.DataFrame)},
    out=Out(pd.DataFrame))
def transform_data(context, data: pd.DataFrame) -> pd.DataFrame:
    # Ensure that data is not empty
    assert context.resources.validate_data(data)
    
    # Function to determine rolling VWAP
    def rolling_vwap(df):
        df['Datetime'] = pd.to_datetime(df['Datetime'])
        df.set_index('Datetime', inplace=True)
        df['Typical Price'] = (df['High'] + df['Low'] + df['Close']) / 3
        df['Cumulative TPV'] = df['Typical Price'] * df['Volume']
        df['Cumulative Volume'] = df['Volume'].cumsum()
        df['Rolling TPV'] = df['Cumulative TPV'].rolling('15min', min_periods=1).sum()
        df['Rolling Volume'] = df['Volume'].rolling('15min', min_periods=1).sum()
        vwap = df['Rolling TPV'] / df['Rolling Volume']
        return vwap
    
    # Add the rolling VWAP to the DataFrame
    data['VWAP'] = rolling_vwap(data.reset_index())
    
    # Forward fill the last VWAP rows to replace NaN values
    data['VWAP'].ffill(inplace=True)
    
    # Calculate the cumulative dollar value of all trades
    dollar_value = (data['Close'] * data['Volume']).cumsum()
    
    # Add the dollar value column to the data DataFrame
    data['DollarValue'] = dollar_value
    
    # Return the updated DataFrame
    return data
```

### Write Down

For the purposes of this blog we will just be saving our data locally to a csv, however you can include code here to store the data in any format locally or remotely in the cloud based on your needs, just as you would without the `op` decorated function. This is also the first `op` where we use the `context` argument and access some of the pipeline functionality for logging, here just to write a job success message.

We will store our data in a csv per day with a naming convention to match that logic.

```
@op(ins={"data", In(dagster_type=pd.DataFrame)})
def write_to_csv(context, data):
    # Get the daily date of the data
    filepath = f"{str(data.index[0].date())}.csv"
    
    # Write the transformed data to a CSV file
    data.to_csv(filepath)

    # Log a message to confirm that the data has been written to the file
    context.log.info(f"Data written to file: {filepath}")
```

## Building a Pipeline

We now have all of the required component functions to build a pipeline that will capture, clean, validate, transform and load some market data for us.

Time to put them together in a pipeline.

All of our `op` functions are in a file called `ops.py`. We can import them into a new file where we define our pipeline execution steps, this will be done in a dagster `graph`.