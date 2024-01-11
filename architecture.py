import os
from azure.storage.filedatalake import (
    DataLakeServiceClient,
    DataLakeDirectoryClient,
    FileSystemClient
)
from azure.identity import DefaultAzureCredential
import pandas as pd
import yfinance as yf
import json

def get_service_client_account_key(account_name, account_key) -> DataLakeServiceClient:
    account_url = f"https://{account_name}.dfs.core.windows.net"
    service_client = DataLakeServiceClient(account_url, credential=account_key)

    return service_client


def create_file_system(service_client: DataLakeServiceClient, file_system_name: str) -> FileSystemClient:
    file_system_client = service_client.create_file_system(file_system=file_system_name)

    return file_system_client


def create_directory(file_system_client: FileSystemClient, directory_name: str) -> DataLakeDirectoryClient:
    directory_client = file_system_client.create_directory(directory_name)

    return directory_client

def upload_file_to_directory(directory_client: DataLakeDirectoryClient, local_path: str, file_name: str):
    file_client = directory_client.get_file_client(file_name)

    with open(file=os.path.join(local_path, file_name), mode="rb") as data:
        file_client.upload_data(data, overwrite=True)

class Parser():
    def parse_sp_index(page_link):
        df = pd.read_html(page_link)
        return df


# my_df = Parser.parse_sp_index("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]
# list_of_tickers = list(my_df.loc[:, "Symbol"])

# service_client = get_service_client_account_key("stockstreamingstorage", "snSgBZKUqoJ6BRM+gpkhF8OB7T4t8dmzaz3hwbLk8U+leCsoxjSOmgSuosJSUl7cOEzIyZ+p/3W7+AStKD1Q1Q==")
# file_system_client = create_file_system(service_client, "stockdata")

# for ticker in list_of_tickers:
#     directory_client = create_directory(file_system_client, ticker)
    

def get_current_price(symbol):
    todays_data = yf.download("AAPL", "2020-1-1", interval = "1d")
    return todays_data

print(get_current_price('AAPL').shape)

# get_current_price("AAPL").to_json("file.json", orient = "records", lines = True)

# directory_client = DataLakeDirectoryClient("https://stockstreamingstorage.dfs.core.windows.net", "stockdata", "AAPL", "snSgBZKUqoJ6BRM+gpkhF8OB7T4t8dmzaz3hwbLk8U+leCsoxjSOmgSuosJSUl7cOEzIyZ+p/3W7+AStKD1Q1Q==")
# upload_file_to_directory(directory_client, r"C:\Users\nchud\Projects\RealTimeStock", "file.json")