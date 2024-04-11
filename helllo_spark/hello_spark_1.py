from pyspark.sql import *
import sys

from lib.logger import Log4j
from lib.utils import get_spark_app_config, load_df, group_count

if __name__ == '__main__':

    conf = get_spark_app_config()

    spark = SparkSession.builder \
            .config(conf=conf) \
            .getOrCreate()
    
    logger = Log4j(spark)

    logger.info("Starting HelloSpark")

    if len(sys.argv)!=2:
        logger.error("File missing")
        sys.exit(-1)

    survey_df= load_df(spark, sys.argv[1])

    # two parts of data to ensure this we have to make only 2 partition in shuffle and sort too in config
    repartition_df= survey_df.repartition(2)

    filtered_df = group_count(repartition_df)

    logger.info(filtered_df.collect())

    import pdb
    pdb.set_trace()

    # to keep our application alive
    input("Press Enter")
    # grouped_df = filtered_df.groupBy("Country")
    # count_df = grouped_df.count()
    # count_df.show()

    # survey_df.show()

    # conf_out = spark.sparkContext.getConf()
    # logger.info(conf_out.toDebugString())
    logger.info("Finished HelloSpark")

    spark.stop()
