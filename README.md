# Interactive Brokers data download scripts

This project contains Python scripts for downloading data using the Interactive Brokers TWS API. 

The process of getting the API up and running to collect real time data is described in some detail in [this article](https://www.wrighters.io/how-to-connect-to-interactive-brokers-using-python/). The details of using the API to download historical data are described in [this article](https://www.wrighters.io/how-to-get-historical-market-data-from-interactive-brokers-using-python/).

## Basic setup

1. Download TWS software from [Interactive Brokers](https://www.interactivebrokers.com/). 
1. Create and fund your account
1. Pay for data for the product you want to access
1. Download TWS API from [here](https://interactivebrokers.github.io/).
1. Setup your Python environment (using pyenv, virtualenv, anaconda, whatever...)
1. Install the TWS API in your environment 
1. Install the dependencies

The steps above are mostly manual since you need to do some personalized setup, but the last install step can be done as follows:

```
python -m pip install pip-tools
pip-compile pyproject.toml
pip-sync
```


