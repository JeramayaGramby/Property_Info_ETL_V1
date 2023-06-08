# Importing all necessary libraries
import requests
import logging
import json
from decouple import config
import pandas as pd
import boto3
from datetime import datetime
import nasdaqdatalink

pd.set_option('display.max_columns', None)
nasdaqdatalink.ApiConfig.verify_ssl = False

class Zillow_Data_ETL_V1:
    def __init__(self,nasdaq_api_key, parsed_zillow_data):
        self.nasdaq_api_key = nasdaq_api_key
        self.parsed_zillow_data = parsed_zillow_data
    
    def zillow_data_collector(self):
        self.nasdaq_api_key = config('NASDAQ_API_KEY')
        parsed_zillow_data = nasdaqdatalink.get('ZILLOW/DATA', start_date="2003-05-31", 
                                                end_date="2023-05-31")
        
    def zillow_data_transformer(self, parsed_zillow_data, df: pd.DataFrame) -> bool:
        if self.parsed_zillow_data.empty:
            print(f'The Spotify API returned empty data. Please check your credentials')
            return False

        # Comment this out and then change it later
        if pd.Series(df['uri']).is_unique:
            print(f'Primary key check succeeded')
        else: 
            raise Exception(f'Duplicate primary keys were found inside dataframe. Check Estated API')
        
        pass
    
    def zillow_data_loader(self):
        data_transfer_question = str(input(f''))
        email_question = str(input(f''))

        
        pass
