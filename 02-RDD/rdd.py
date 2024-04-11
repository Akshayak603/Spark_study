from pyspark.sql import *
from pyspark import SparkConf
from lib.logger import Log4j
import sys
from collections import namedtuple

SurveyRecord = namedtuple("SurveyRecord", ["Age","Gender","Country","State"])
if __name__ == '__main__':
    # Creation of RDD
    conf = SparkConf() \
        .setMaster("local[3]") \
        .setAppName("Rdd")

    # DataFrame APIs Spark Session is a high level API which is a improvement of Spark Context
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    # Creating Spark Context from Spark Session to use RDD which ia traditional method
    sc = spark.sparkContext
    logger = Log4j(spark)

    if len(sys.argv)!=2 :
        logger.error("Input file is missing")
        sys.exit(-1)

    # RDD reads text files only they are not good for working with csv, parquet, json files support is not there
    # so at first it reads the data
    linesRDD = sc.textFile(sys.argv[1])

    # Process of RDD

    # Most of the RDD supports lambda f(x) so it helps in basic transformation like map, reduce, filter

    # after reading it distributes the data
    partitionRDD= linesRDD.repartition(2)

    # after repartition we can put transformation here we are hardcoding everything about group by etc (outdated

    colsRDD = partitionRDD.map(lambda line: line.replace('"', '').split(","))
    selectRDD = colsRDD.map(lambda cols: SurveyRecord(int(cols[1]), cols[2], cols[3], cols[4]))
    filteredRDD = selectRDD.filter(lambda r: r.Age < 40)
    kvRDD = filteredRDD.map(lambda r: (r.Country, 1))
    countRDD = kvRDD.reduceByKey(lambda v1, v2: v1 + v2)

    colsList = countRDD.collect()
    for x in colsList:
        logger.info(x)
    # countRDD= kvRDD.reduceByKey(lambda v1, v2: v1+v2)
    #
    # colsList = countRDD.collect()
    # for x in colsList:
    #     logger.info(x)





