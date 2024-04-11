import re
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

from lib.logger import Log4j

def parse_gender(gender):
    female_pattern = r"^f$|f.m|w.m"
    male_pattern = r"^m$|ma|m.l"

    if re.search(female_pattern, gender.lower()):
        return "Female"
    elif re.search(male_pattern, gender.lower()):
        return "Male"
    else:
        return "Unknown"


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("UDF Demo") \
        .master("local[2]") \
        .getOrCreate()

    logger = Log4j(spark)

    survey_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("data/survey.csv")

    survey_df.show(10)

    # column object expression

    # register python function with UDF in this scenario it will not register this in catalog
    parse_gender_udf= udf(parse_gender, StringType())
    [logger.info(f) for f in spark.catalog.listFunctions() if "parse_gender" in f.name]
    survey_df2= survey_df.withColumn("Gender", parse_gender_udf("Gender"))
    survey_df2.show(10)

    # in this scenario it will register this in catalog as we are using it as a SQL expression
    spark.udf.register("parse_gender_udf", parse_gender, StringType())
    [logger.info(f) for f in spark.catalog.listFunctions() if "parse_gender" in f.name]
    survey_df3= survey_df.withColumn("Gender", expr("parse_gender_udf(Gender)"))
    survey_df3.show(10)