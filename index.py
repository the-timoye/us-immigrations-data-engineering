
from os import sep
from data_cleaning.cities import clean_cities_data
from data_cleaning.immigration import clean_immigration_data
import configparser;
from pyspark.sql import SparkSession;

config = configparser.ConfigParser();
config.read('config.cfg')
spark_session = SparkSession.builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.1.2").getOrCreate();
IMMIGRATION_DATA  = 'datasets/immigration_data/*.parquet';
immigration_df = spark_session.read.format('parquet').load(IMMIGRATION_DATA, inferSchema=True , header=True);
CITIES_DATA = 'datasets/us-cities-demographics.csv';
cities_df = spark_session.read.format('csv').load(CITIES_DATA, sep=";", inferSchema=True , header=True);

# Hadoop Configuration
sc = spark_session.sparkContext
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", config['AWS']['KEY'] )
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", config['AWS']['SECRET'] )
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")

def write_to_s3():
    cleaned_immigration_df = clean_immigration_data(immigration_df)
    cleaned_cities_data = clean_cities_data(cities_df)
    print('========================================= WRITING TO IMMIGRATION FILES S3 =========================================')
    cleaned_immigration_df.write.mode('append').parquet(f"s3a://{config['S3']['BUCKET']}/staging_immigrations_table.parquet")

    print('========================================= WRITING TO CITIES FILES S3 =========================================')
    cleaned_cities_data.write.mode('append').parquet(f"s3a://{config['S3']['BUCKET']}/staging_cities_table.parquet")
    
    print('========================================= WRITING TO CITIES FILES S3 =========================================')
    cleaned_cities_data.write.mode('append').parquet(f"s3a://{config['S3']['BUCKET']}/staging_cities_table.parquet")
    print('done')

def main ():
    write_to_s3();
    
    

if __name__ == '__main__':
    main()