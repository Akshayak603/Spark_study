from pyspark.sql import *
from pyspark.sql.types import StructType, StructField, DateType, StringType, IntegerType
from lib.logger import Log4j
if __name__=='__main__':

    spark= SparkSession.builder \
                        .master("local[3]") \
                        .appName("schema") \
                        .getOrCreate()

    logger = Log4j(spark)

    # as we can see there are schema issues with csv and json
    # so we will avoid those via
    # programmable schem or string ddl

    #programmable
    flightSchemaStruct = StructType([
        StructField("FL_DATE", DateType()),
        StructField("OP_CARRIER", StringType()),
        StructField("OP_CARRIER_FL_NUM", IntegerType()),
        StructField("ORIGIN", StringType()),
        StructField("ORIGIN_CITY_NAME", StringType()),
        StructField("DEST", StringType()),
        StructField("DEST_CITY_NAME", StringType()),
        StructField("CRS_DEP_TIME", IntegerType()),
        StructField("DEP_TIME", IntegerType()),
        StructField("WHEELS_ON", IntegerType()),
        StructField("TAXI_IN", IntegerType()),
        StructField("CRS_ARR_TIME", IntegerType()),
        StructField("ARR_TIME", IntegerType()),
        StructField("CANCELLED", IntegerType()),
        StructField("DISTANCE", IntegerType())
    ])

    #ddl
    flightSchemaDDL = """FL_DATE DATE, OP_CARRIER STRING, OP_CARRIER_FL_NUM INT, ORIGIN STRING, 
              ORIGIN_CITY_NAME STRING, DEST STRING, DEST_CITY_NAME STRING, CRS_DEP_TIME INT, DEP_TIME INT, 
              WHEELS_ON INT, TAXI_IN INT, CRS_ARR_TIME INT, ARR_TIME INT, CANCELLED INT, DISTANCE INT"""


    csv_df= spark.read \
                 .format("csv") \
                 .schema(flightSchemaStruct) \
                 .option("mode", "FAILFAST") \
                 .option("dateformat", "M/d/y") \
                 .option("header", "true") \
                 .load("data/flight*.csv")

    csv_df.show(3)
    logger.info("CSV:"+ csv_df.schema.simpleString())

    json_df= spark.read \
                 .format("json") \
                 .schema(flightSchemaDDL) \
                 .load("data/flight*.json")

    json_df.show(3)
    logger.info("JSON:" + json_df.schema.simpleString())

    parquet_df = spark.read \
        .format("parquet") \
        .load("data/flight*.parquet")

    parquet_df.show(3)
    logger.info("Parquet:" + parquet_df.schema.simpleString())




