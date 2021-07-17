from pyspark.sql.functions import monotonically_increasing_id
from helpers.helpers import remane_columns
from pyspark.sql.types import FloatType, IntegerType, StringType


new_column_names = {
    'City': 'city',
    'State': 'state',
    'Median Age': 'median_age',
    'Female Population': 'female_population',
    'Male Population': 'male_population',
    'Total Population': 'total_population',
    'Number of Veterans': 'num_of_veterans',
    'Foreign-born': 'no_of_immigrants',
    'Average Household Size': 'avg_household_size',
    'State Code': 'state_code',
    'Race': 'race'
}

def clean_cities_data(
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
    print('========================================= CLEANING CITIES DATA =========================================')
    new_df = remane_columns(dataframe, new_column_names)

    new_df = new_df.select(
        new_df.city.cast(StringType()),
        new_df.state.cast(StringType()),
        new_df.median_age.cast(FloatType()),
        new_df.male_population.cast(IntegerType()),
        new_df.female_population.cast(IntegerType()),
        new_df.total_population.cast(IntegerType()),
        new_df.num_of_veterans.cast(IntegerType()),
        new_df.no_of_immigrants.cast(IntegerType()),
        new_df.avg_household_size.cast(FloatType()),
        new_df.state_code.cast(StringType()),
        new_df.race.cast(StringType())
    ).na.fill(value='not provided').distinct()

    new_df = new_df.withColumn('city_id', monotonically_increasing_id())
    
    return new_df