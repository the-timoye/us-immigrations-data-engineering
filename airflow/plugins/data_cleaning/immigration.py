from pyspark.sql.types import  StringType, IntegerType, StringType;
from pyspark.sql.functions import monotonically_increasing_id, to_date;
from helpers.helpers import remane_columns
from helpers.udfs import get_address, get_country, get_date, get_mode, get_transport_mode, get_visa_type

new_column_names = {
    'i94yr': 'year',
    'i94mon': 'month',
    'i94res': 'resident_country_code',
    'arrdate': 'arrival_date',
    'i94mode': 'mode',
    'i94addr': 'address',
    'depdate': 'departure_date',
    'i94bir': 'age',
    'i94visa': 'visa_code',
    'biryear': 'birth_year'
}

def clean_immigration_data(
    dataframe
):
    """
        @description:
            Performs data cleaning functions like renaming the columns in the DataFrame, replacing null values with "not provided", 
            and changing columns to the appropraite datatypes.
        @params:
            dataframe (DataFrame): The spark dataframe to be cleaned.
        @returns:
            A cleaned dataframe with renamed columns, no null values and appropraite column datatypes
    """
    
    print('========================================= CLEANING IMMIGRATION DATA =========================================')

    new_df = remane_columns(dataframe, new_column_names)

    new_df = new_df.select(
        new_df.year.cast(IntegerType()), 
        new_df.month.cast(IntegerType()), 
        new_df.resident_country_code.cast(IntegerType()),
        new_df.arrival_date, 
        new_df.address,
        new_df.departure_date,
        new_df.age.cast(IntegerType()),
        new_df.visa_code.cast(IntegerType()),
        new_df.birth_year.cast(IntegerType()),
        new_df.gender, 
        new_df.airline,
        new_df.mode.cast(IntegerType())
    ).na.fill(value='not provided').distinct()

    # get country name for residential code
    new_df = new_df.withColumn(
        'resident_country', 
        get_country(
            new_df.resident_country_code.cast(StringType()))
        ).withColumn(
            'arrival_date',
            to_date(get_date(new_df.arrival_date))
    ).withColumn(
        'departure_date',
        get_date(new_df.departure_date)
    ).withColumn(
        'visa_type',
        get_visa_type(new_df.visa_code.cast(StringType()))
    ).withColumn(
        'state_address',
        get_address(new_df.address)
    ).withColumn(
        'mode',
        get_mode(new_df.mode)
    )
    new_df = new_df.withColumn(
        'transport_mode',
        get_transport_mode(new_df.mode)
    ).withColumn('immigrant_id', monotonically_increasing_id())
    return new_df