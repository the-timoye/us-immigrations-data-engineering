import pandas as pd

cities_demography = pd.read_csv('datasets/us-cities-demographics.csv', sep=';');
airport_codes = pd.read_csv('datasets/airport-codes_csv.csv');
global_temperatures = pd.read_csv('datasets/GlobalLandTemperaturesByCountry.csv');
immigration_data = pd.read_csv('datasets/immigration_data_sample.csv');

# dataset sneakpeak
print('===================================================================================== CITIES DEMOGRAPHY ===================================================================================== ')
print(cities_demography.head());
print('='*200)
print('===================================================================================== AIRPORT CODES ===================================================================================== ')
print('='*200)
print(airport_codes.head());
print('='*200)
print('===================================================================================== GLOBAL TEMPERATURES ===================================================================================== ')
print('='*200)
print(global_temperatures.head());
print('===================================================================================== IMMIGRATION DATA ===================================================================================== ')
print('='*200)
print(immigration_data.head());

# check datatypes
print('='*200)
print('===================================================================================== CITIES DEMOGRAPHY DATATYPES ===================================================================================== ')
print('='*200)
print(cities_demography.dtypes);
print('='*200)
print('===================================================================================== AIRPORT CODES DATATYPES ===================================================================================== ')
print('='*200)
print(airport_codes.dtypes);
print('='*200)
print('===================================================================================== GLOBAL TEMPERATURES DATATYPES ===================================================================================== ')
print('='*200)
print(global_temperatures.dtypes);
print('===================================================================================== IMMIGRATION DATATYPES ===================================================================================== ')
print('='*200)
print(immigration_data.dtypes);