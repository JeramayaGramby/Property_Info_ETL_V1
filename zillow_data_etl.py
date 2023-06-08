# Importing all necessary libraries
import requests
import logging
from logging.handlers import SysLogHandler
import json
from decouple import config
import pandas as pd
import boto3
from datetime import datetime
import nasdaqdatalink
import yagmail as yag

# These code files need logging, its own virtual environment, and AWS Credentials

# Logging functionality
PAPERTRAIL_HOST = 'logs.papertrailapp.com'
PAPERTRAIL_PORT = 37554
zillow_etl_logger=logging.getLogger('zillow_etl_logs')
zillow_etl_logger.setLevel(logging.DEBUG)
handler = SysLogHandler(address=(PAPERTRAIL_HOST,PAPERTRAIL_PORT))

pd.set_option('display.max_columns', None)
nasdaqdatalink.ApiConfig.verify_ssl = False

class Zillow_Data_ETL_V1:
    def __init__(self,nasdaq_api_key, parsed_zillow_data):
        self.nasdaq_api_key = nasdaq_api_key
        self.parsed_zillow_data = parsed_zillow_data
    
    def zillow_data_collector(self):
        self.nasdaq_api_key = config('NASDAQ_API_KEY')
        self.parsed_zillow_data = nasdaqdatalink.get('ZILLOW/DATA', 
                                                     start_date="2003-05-31",end_date="2023-05-31")
        
    def zillow_data_transformer(self, df: pd.DataFrame) -> bool:
        if self.parsed_zillow_data.empty:
            print(f'The Spotify API returned empty data. Please check your credentials')
            # Put a logging statement here
            return False

        # Comment this out and then change it later
        if pd.Series(df['uri']).is_unique:
            print(f'Primary key check succeeded')
        else: 
            raise Exception(f'Duplicate primary keys were found inside dataframe. Check NASDAQ API')
    
    def manual_zillow_data_loader(self):
        parsed_zillow_data_csv = self.parsed_zillow_data.to_csv('parsed_zillow_data {datetime.now}')
        data_transfer_question = str(input(f'Would you like to send this data to your email or S3 bucket? (Enter the word email or s3):'))

        if data_transfer_question == 'email':
            email_address = str(input(f'Enter the email address you would like to send the Zillow data to:'))

            text = f"""
                    Thank you for using the Zillow Data ETL.
                    Your excel file is attached to this email."""
                                
            subject = f'{datetime.now()} Zillow Data Request'
            try:    
                with open(parsed_zillow_data_csv, 'r') as attachment:
                    yag.send(to=email_address,
                    subject=subject,
                    contents=text,
                    attachments=attachment)
            except Exception as e:
                print(e)


        if data_transfer_question == 's3':
            ACCESS_KEY=str(input(f'Enter your Access Key:'))
            SECRET_ACCESS_KEY=str(input(f'Enter your Secret Access Key:'))
            SESSION_TOKEN=str(input(f'Enter your session token:'))
            session = boto3.Session(
                aws_access_key_id=ACCESS_KEY, 
                aws_secret_access_key=SECRET_ACCESS_KEY, 
                aws_session_token=SESSION_TOKEN
            )
            s3_client = boto3.client('s3')
            bucket=str(input(f'Enter the name of the bucket you would like to send the file to:'))
            object_name=str(f'{datetime.now()} {file}')
            s3_client.upload_file(file,bucket,object_name)
            print(f'Your database file has been successfully uploaded to {bucket} with the name {object_name}')
        
        return file,object_name,bucket
            
    # Automatic zillow data loader uses a dag in Airflow to run the above code
    #def automatic_zillow_data_loader(self):
    #    with blank as DAG():
    #        pass