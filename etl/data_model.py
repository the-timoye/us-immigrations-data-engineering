
from data_cleaning.cities import clean_cities_data
from data_cleaning.immigration import clean_immigration_data

def model_data(spark_session):
    IMMIGRATION_DATA  = 'datasets/immigration_data/*.parquet';
    immigration_df = spark_session.read.format('parquet').load(IMMIGRATION_DATA, inferSchema=True , header=True);
    CITIES_DATA = 'datasets/us-cities-demographics.csv';
    cities_df = spark_session.read.format('csv').load(CITIES_DATA, sep=";", inferSchema=True , header=True);

    cleaned_immigration_df = clean_immigration_data(immigration_df)
    cleaned_cities_data = clean_cities_data(cities_df)

    us_cities = cleaned_cities_data.select(["city", "state_code"]).distinct()
    us_states = cleaned_cities_data.select(["state_code", "state"]).distinct()
    us_geography = cleaned_cities_data.select([
        "male_population", 
        "female_population", 
        "total_population", 
        "num_of_veterans", 
        "no_of_immigrants", 
        "avg_household_size"]).distinct();
    visa_types = cleaned_immigration_df.select(["visa_code", "visa_type"]).distinct()
    travels_info = cleaned_immigration_df.select(["arrival_date", "departure_date", "airline"]).distinct()
    transport_modes = cleaned_immigration_df.select(["mode", "transport_mode"]).distinct()
    immigrants = cleaned_immigration_df.select(["age", "birth_year", "gender", "resident_country"]).distinct()
    immigrants_facts = cleaned_immigration_df.select(["visa_code", "mode", "year", "month"]).distinct()


    datasets = [
        cleaned_immigration_df,
        cleaned_cities_data,
        us_cities, 
        us_states, 
        us_geography, 
        visa_types,
        travels_info,
        transport_modes,
        immigrants,
        immigrants_facts
    ]
    return datasets

tables = [
"staging_immigrations_table",
"staging_cities_table", 
"us_cities", 
"us_states",
"us_geography",
"visa_types",
"travels_info",
"transport_modes",
"immigrants",
"immigrants_facts"
]

