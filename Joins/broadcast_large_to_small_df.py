from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Shuffle Join Demo") \
        .master("local[3]") \
        .getOrCreate()

    logger = Log4j(spark)

    flight_time_df1 = spark.read.json("data/d1/")
    flight_time_df2 = spark.read.json("data/d2/")

    spark.conf.set("spark.sql.shuffle.partitions", 3)

    join_expr = flight_time_df1.id == flight_time_df2.id
    # shuffle join
    join_df = flight_time_df1.join(flight_time_df2, join_expr, "inner")
    #bradcast join
    join_df_b = flight_time_df1.join(broadcast(flight_time_df2), join_expr, "inner")

    join_df.collect()
    input("press a key to stop...")