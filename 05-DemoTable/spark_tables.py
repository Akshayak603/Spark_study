from pyspark.sql import *
from lib.logger import Log4j
import os
if __name__=='__main__':

    spark= SparkSession.builder \
                        .master("local[3]") \
                        .appName("write") \
                        .enableHiveSupport() \
                        .getOrCreate()

    logger= Log4j(spark)

    source_df = spark.read \
                .format('parquet') \
                .load('data/flight*.parquet')

    spark.sql("create database if not exists airline_db")
    # one way else db.table_name while writing
    spark.catalog.setCurrentDatabase("airline_db")

    # internal table if we use partitionBy here, so we might get so many tables
    # if that column has large different values
    # so here we can restrict partitionBy with Bucket method simple on hash based
    # like if create 5 buckets then it will create a hash number around our group by columns and %5 that
    # then data will be passed to that %5 bucket


    source_df.write \
            .format("csv") \
            .mode("overwrite") \
            .bucketBy(5,"OP_CARRIER", "ORIGIN") \
            .sortBy("OP_CARRIER", "ORIGIN") \
            .saveAsTable('flight_data_tbl')

    logger.info(spark.catalog.listTables("airline_db"))