from etl.local_to_s3 import load_to_s3
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
    load_to_s3(spark_session, config);
    
    
if __name__ == '__main__':
    main()