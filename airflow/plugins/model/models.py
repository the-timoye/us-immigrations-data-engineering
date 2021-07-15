
from pyspark.sql import SparkSession;
from pyspark.sql.functions import monotonically_increasing_id
from data_cleaning.cities import clean_cities_data
from data_cleaning.immigration import clean_immigration_data
import configparser

config = configparser.ConfigParser()

config.read('config.cfg')

def model_data():
    spark_session = SparkSession.builder.appName('Yenyenyen').config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.1.2").getOrCreate();

    sc = spark_session.sparkContext
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", config['AWS']['KEY'] )
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", config['AWS']['SECRET'] )
    sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")
    sc._jsc.hadoopConfiguration().set("parquet.enable.summary-metadata", "false")

    IMMIGRATION_DATA  = 'datasets/immigration_data/*.parquet';
    immigration_df = spark_session.read.format('parquet').load(IMMIGRATION_DATA, inferSchema=True , header=True);
    CITIES_DATA = 'datasets/us-cities-demographics.csv';
    cities_df = spark_session.read.format('csv').load(CITIES_DATA, sep=";", inferSchema=True , header=True);

    cleaned_immigration_df = clean_immigration_data(immigration_df)
    cleaned_cities_data = clean_cities_data(cities_df)

    print(f'========================================= WRITING staging_immigrations_table TABLE TO S3 =========================================')
    cleaned_immigration_df.repartition(1).write.mode('overwrite').parquet(f"s3a://{config['S3']['BUCKET']}/staging_immigrations_table.parquet")

    print(f'========================================= WRITING staging_cities_table TABLE TO S3 =========================================')
    cleaned_cities_data.repartition(1).write.mode('overwrite').parquet(f"s3a://{config['S3']['BUCKET']}/staging_cities_table.parquet")

    return 'Done'


tables = ['staging_immigrations_table', 'staging_cities_table', ]