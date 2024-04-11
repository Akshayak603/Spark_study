import configparser

from pyspark import SparkConf

def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("spark.conf")

    for (key,val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)
    return spark_conf

def load_df(spark,file):
    return spark.read \
            .option("header","true") \
            .option("inferSchema","true") \
            .csv(file)

def group_count(df):
    return df.where("Age<40") \
             .select("Age","Gender","Country") \
             .groupBy("Country") \
             .count()
