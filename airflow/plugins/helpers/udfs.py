import pandas as pd
import configparser;
from pyspark.sql.functions import udf
config = configparser.ConfigParser();
config.read('config.cfg')
@udf
def get_country(code):
    '''
        @description:
             convert country code to country name. codes are provided in config file in a key value pair.
        @params:
            code(INT): country code
        @returns:
            (str): country name
    '''
    if code is None or code == '':
        return 'not provided'
    if code not in config['RESIDENTIAL_COUNTRY']:
        return f'invalid code {code}'
    return config['RESIDENTIAL_COUNTRY'][code]

@udf
def get_date(date):
    '''
        @description:
             convert SAS date to datetime in python
        @params:
            date(INT): date in SAS format to be converted
        @returns:
            (str): 'not provided' if date is None or 'not privided'
            (date): converted date
    '''
    if date is None:
        return 'not provided'
    if date == 'not provided':
        return 'not provided'
    formatted_datetime =  pd.to_datetime(pd.to_timedelta(date, unit='D') + pd.Timestamp('1960-1-1'))
    return str(formatted_datetime.date())

@udf
def get_visa_type(visa_code):
    '''
        @description:
             convert visa code to actual visa type as defined in config file.
             this defines the type of visa given to the immigrant
        @params:
            visa_code(INT): visa code
        @returns:
            (str): visa type (student, pleasure, business)
    '''
    if visa_code is None or visa_code == '':
        return 'not provided'
    if visa_code == 'not provided':
        return 'not provided'
    return config['VISA_TYPES'][visa_code];

@udf
def get_address(abbr):
    '''
        @description:
             convert US states abbreviation to state name. 
             abbreviations are provided in config file in a key value pair.
        @params:
            abbr(STR): state two-letter abbreviation
        @returns:
            (str): state name
    '''
    if (abbr == 'not provided') or abbr is None:
        return 'not provided'
    if abbr not in config['US_STATES']:
        return f'invalid code {abbr}'
    return config['US_STATES'][abbr]

@udf
def get_mode(code):
    '''
        @description:
             convert transport code to transport type. 
             codes are provided in config file in a key value pair.
        @params:
            code(INT): transport code identifier
        @returns:
            (str): mode of transport - air, land, sea
    '''
    if  code is None or code == "" or code == "null":
        return 4
    return int(code)

@udf
def get_transport_mode(code):
    '''
        @description:
             convert transport code to transport type. 
             codes are provided in config file in a key value pair.
        @params:
            code(INT): transport code identifier
        @returns:
            (str): mode of transport - air, land, sea
    '''
    if code == 4 or code is None:
        return 'not provided'
    if str(code) not in config['TRANSPORT_MODE']:
        return f'invalid code {code}'
    return config['TRANSPORT_MODE'][str(code)]
