# Data Pipelines in Python

Data pipelines are an essential tool for managing and processing data in modern software systems. A data pipeline is a set of interconnected components that process data as it flows through the system. These components can include data sources, write down functions, transformation functions, and other data processing operations, such as validation and cleaning. Pipelines are a way to automate the process of collecting, transforming, and analyzing data. They are a series of steps that take raw data, clean it, process it, and store it in a way that can be easily analyzed or used in other applications. Pipelines are particularly useful when dealing with large amounts of data or when working with data that needs to be constantly updated.

Pipelines help in automating the data processing, making it easier and quicker to collect, store, and analyze data. By breaking down data processing into smaller, more manageable tasks, pipelines make it easier to maintain and troubleshoot the system.

In this blog we will look at building a simple data pipeline using `dagster` and `yfinance`, collecting some simple market data, validating that the expected data is present, cleaning and enriching it, before saving the resultant data to disk and using it in a notebook to represent an end user accessing the data we would schedule for collection in our pipeline.

Installation of libraries used in our `venv`:
```
pip install pandas yfinance dagster dagit
```
