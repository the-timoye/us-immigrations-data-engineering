
import configparser;
from pyspark.sql import SparkSession;

config = configparser.ConfigParser();
config.read('config.cfg')
spark_session = SparkSession.builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.1.2").getOrCreate();

# Hadoop Configuration
sc = spark_session.sparkContext
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", config['AWS']['KEY'] )
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", config['AWS']['SECRET'] )
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")

def main ():
    immi_df = spark_session.read.format('parquet').load(f"s3a://{config['S3']['BUCKET']}/{config['S3']['IMMIGRATION_KEY']}/.parquet", inferSchema=True , header=True);
    print(f'Immi count {immi_df.count()}')

    cities_df = spark_session.read.format('parquet').load(f"s3a://{config['S3']['BUCKET']}/{config['S3']['CITIES_KEY']}/.parquet", inferSchema=True , header=True);
    print(f'Immi count {cities_df.count()}')
    
if __name__ == '__main__':
    main()