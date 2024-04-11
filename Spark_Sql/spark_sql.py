from pyspark.sql import *
import sys
from lib.logger import Log4j

if __name__=="__main__":

    spark= SparkSession.builder \
                        .master("local[3]") \
                        .appName("Spark SQL") \
                        .getOrCreate()
    logger = Log4j(spark)
    if len(sys.argv)!=2:
        logger.info("Error no file")
        sys.exit(-1)

    survey_df= spark.read.csv(sys.argv[1],header=True, inferSchema=True)

    survey_df.createOrReplaceTempView("survey_temp")
    countDf= spark.sql("select country, count(*) from survey_temp where age<40 group by country")
    countDf.show()

