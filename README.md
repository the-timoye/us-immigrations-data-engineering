# udacity-capstone-project
Udacity's Data Engineering Capstone Project

### THE PROBLEM (USE CASE)
A company decides to analyse the US immigration database to check student immigrants. The following are examples of questions they need answers to through their analysis:
    - What are the age ranges of immigrant students? <br>
    - How long do these immigrants stay in the US brfore departure? <br>
    - What is the ration of male to female immigrants for the available years? <br>
    - How many business men stay in the United State for more than 3 months> <br>
The company would also like to have answers to questioins like: <br>
    - What are the countries of residents of these immigrants, and the major race of people in the states they would be  residing? <br>
    - What cities get the most student immigrants? Population and most popular race in those cities? <br>


### SOLUTION
The goal of this project is to ensure the right data is gathered for a successful analysis by the team. Data Engineering tools like pandas, Amazon Redshift, Spark, and Airflow will be used for exploration, storage and manupilation of data. Each step will be discussed below.

### DATA IDENTIFICATION AND GATHERING
To compute these analysis, datasets will be downloaded from the immigration and US Citites demography websites since the analysis team will be answering questions based on these two sets. <br>
    - Immigration dataset: https://travel.trade.gov/research/reports/i94/historical/2016.html <br>
    - US cities dataset: https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/ <br>
    
### DATA EXPLORATION

The `explore_data.py` prints a few rows on data for each dataset we will be making use of. It also displays the datatypes of each of the columns of each dataset.
This exploration is done with the Pandas library. Pandas, because of it's ease of use, and ability to load datasets of different formats (csv, parquet, json) into dataframes, and check for anomalies, nulls and outliers, column types.