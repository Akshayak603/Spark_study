from pyspark.sql import *
from lib.logger import Log4j
from pyspark.sql.functions import spark_partition_id
if __name__=='__main__':

    spark= SparkSession.builder \
                        .master("local[3]") \
                        .appName("write") \
                        .getOrCreate()

    logger= Log4j(spark)

    source_df = spark.read \
                .format('parquet') \
                .load('data/flight*.parquet')

    # random write
    """source_df.write \
            .format("json") \
            .mode("overwrite") \
            .option("path","dataset/json/") \
            .save()

    # to know the number of partitions
    logger.info("Num Partition: "+ str(source_df.rdd.getNumPartitions()))
    source_df.groupby(spark_partition_id()).count().show()"""

    # with Partition (blind)

    """partitiondf = source_df.repartition(5)
    partitiondf.write \
        .format("json") \
        .mode("overwrite") \
        .option("path", "dataset/json_repartition/") \
        .save()

    # to know the number of partitions
    logger.info("Num Partition: " + str(source_df.rdd.getNumPartitions()))
    source_df.groupby(spark_partition_id()).count().show()"""

    # now if we want to partition our files by specific column like filtering with max_records in each file
    source_df.write \
        .format("json") \
        .mode("overwrite") \
        .partitionBy("OP_CARRIER", "ORIGIN") \
        .option("maxRecordsPerFile", 10000) \
        .option("path", "dataset/json/") \
        .save()

    # to know the number of partitions
    logger.info("Num Partition: " + str(source_df.rdd.getNumPartitions()))
    source_df.groupby(spark_partition_id()).count().show()




