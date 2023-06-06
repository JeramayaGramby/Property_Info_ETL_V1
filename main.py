# Importing all necessary libraries
import os
import requests
import warnings
import logging
import json
from decouple import config
import pandas as pd
import yagmail as yag
from datetime import datetime


# Defining all functions
# Use a logger within the function to post a successful api response to the log
# Send all data to the person's email address
# Wrap all this code in a try and except block

class Individual_Property_Info_ETL_V1:
    def __init__(self,street_address,city,state,zip_code,api_key,parsed_property_data,):
        self.street_address = street_address
        self.city = city
        self.state = state
        self.zip_code = zip_code
        self.api_key = api_key
        self.parsed_property_data = parsed_property_data

    def property_data_collector(self):
        self.street_address = str(input(f'Enter the full Property Address (Example: 423 Main Street:'))
        self.city = str(input(f'Enter the city that the home is located in:'))
        self.state = str(input(f'Enter the state that the home is located in:'))
        self.zip_code = str(input(f'Enter the zip code the home is located in:'))
        self.api_key = config('ESTATED_API_KEY')
    
        base_url='https://apis.estated.com/v4/property'
        params = (
            ('api_key', self.api_key),
            ('street_address', self.street_address),
            ('city', self.city),
            ('state', self.state),
            ('zip_code', self.zip_code),
        )

        response = requests.get(base_url, params=params)
        print(f'Successful API Response!')
        logging.basicConfig()
        # These lines turn the JSON response into a dataframe
        # Address, Parcel, Structure, taxes, assessments, market_assessments, valuation, 
        # owner, deeds, boundary
        # The address needs to be the unique identifier appended to the 
        estated_api_response = response.json()
        parsed_property_data = pd.json_normalize(data=estated_api_response['data'])
        
        # Acquiring data surrounding all the deeds issued to the property owner
        address_data = estated_api_response['data']['address']
        parsed_address_data = pd.DataFrame(data=address_data)

        # Acquiring all the market valuation data on a property (approximate value)
        boundary_data = estated_api_response['data']['boundary']
        parsed_boundary_data = pd.DataFrame(data=boundary_data)

        # Acquiring data surrounding all the deeds issued to the property owner
        deeds_data = estated_api_response['data']['deeds']
        parsed_deeds_data = pd.DataFrame(data=deeds_data)

        # Acquiring data surrounding the land parcel to the property owner
        parcel_data = estated_api_response['data']['parcel']
        parsed_parcel_data = pd.DataFrame(data=parcel_data)

        # Acquiring data surrounding all the deeds issued to the property owner
        structure_data = estated_api_response['data']['structure']
        parsed_structure_data = pd.DataFrame(data=structure_data)   

        # Acquiring all the tax assessment data on a property
        tax_assessment_data = estated_api_response['data']['assessments']
        parsed_tax_assessment_data = pd.DataFrame(data=tax_assessment_data)

        # Acquiring all the market valuation data on a property (approximate property value)
        valuation_data = estated_api_response['data']['market_assessments']
        parsed_valuation_data = pd.DataFrame(data=valuation_data)

        dataframe_list=[parsed_address_data,parsed_boundary_data,parsed_deeds_data,parsed_parcel_data,
                        parsed_valuation_data,parsed_tax_assessment_data]
        
        for dataframe in dataframe_list:
            if dataframe.empty:
                print(f'The Estated API returned empty data. Please check your credentials')
                return False
    
            if pd.Series(dataframe['uri']).is_unique:
                print(f'Primary key check succeeded')
            else: 
                logging.error()
                raise Exception(f'Duplicate primary keys were found inside dataframe. Check Estated API')

        # Turning all data into Excel files that can be sent to the email address
        # Also turn the data into sqlite files that can be stored in AWS or wherever

        parsed_property_data.to_csv('', index=False)
    # Must convert the response into JSON and then check the dataframe for empty values
    # The Response should be logged
    def property_data_validator(property_data_extractor, df: pd.DataFrame) -> bool:
        # Turn the print statements into log messages

        if df.empty:
            print(f'The Spotify API returned empty data. Please check your credentials')
            return False
    
        if pd.Series(df['uri']).is_unique:
            print(f'Primary key check succeeded')
        else: 
            raise Exception(f'Duplicate primary keys were found inside dataframe. Check Estated API')
        

    def property_data_loader(self, property_data_extractor):
        email_address = config('EMAIL_ADDRESS')
        subject = f'{self.street_address} Total Data'
        contents = 
        with open(database, 'r') as attachment:
            yag.send(to=[email_address],
            subject=subject,
            contents=text,
            attachments=attachment
        ) 
        pass