
from data_cleaning.immigration import clean_immigration_data
import configparser;
from pyspark.sql import SparkSession;

config = configparser.ConfigParser();
config.read('config.cfg')
spark_session = SparkSession.builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.1.2").getOrCreate();
IMMIGRATION_DATA  = 'datasets/immigration_data/*.parquet'
CITIES_DATA = '/datasets/us-cities-demographics.csv';
cities_df = spark_session.read.format('parquet').load(IMMIGRATION_DATA, inferSchema=True , header=True);

sc = spark_session.sparkContext

# Hadoop Configuration
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", config['AWS']['KEY'] )
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", config['AWS']['SECRET'] )
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")

def write_to_s3():
    immigration_df = spark_session.read.format('parquet').load(IMMIGRATION_DATA, inferSchema=True , header=True);
    cleaned_immigration_df = clean_immigration_data(immigration_df)
    print('========================================= WRITING TO S3 =========================================')
    # cleaned_immigration_df.write.mode('append').parquet(f"s3a://{config['S3']['BUCKET']}/immi.parquet")
    cleaned_immigration_df.write.mode('append').parquet(f"datasets/immigration_data/immi.parquet")
    print('done')

def main ():
    # pathlist = Path('datasets/immigration_data').rglob('*.parquet')
    # for path in pathlist:
    
    write_to_s3();

if __name__ == '__main__':
    main()