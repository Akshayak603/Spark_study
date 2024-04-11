from pyspark.sql import *
from pyspark.sql import functions as f
from lib.logger import Log4j

if __name__=='__main__':

    spark = SparkSession.builder \
                        .master("local[3]") \
                        .appName("AggDemo") \
                        .getOrCreate()

    logger= Log4j(spark)

    invoice_df= spark.read \
                    .format("csv") \
                    .option("header","true") \
                    .option("inferSchema", "true") \
                    .load("data/invoices.csv")

    # Simple Aggregation
    invoice_df.select(f.count("*").alias("count *"),
                      f.sum("Quantity").alias("Total Quantity"),
                      f.round(f.avg("UnitPrice"),2).alias("AvgPrice"),
                      f.countDistinct("InvoiceNo").alias("CountDistinct")).show()

    # Sql like expression

    invoice_df.selectExpr(
        "count(1) as `count 1`",
        "count(StockCode) as `count field`",
        "sum(Quantity) as TotalQuantity"
    ).show()

    # Grouping Aggregation
    invoice_df.createOrReplaceTempView("sales")
    summary_sql= spark.sql("""
    Select Country, InvoiceNo,
    sum(Quantity) as TotalQuantity,
    round(sum(Quantity* UnitPrice),2) as InvoiceValue
    from sales
    group by Country, InvoiceNo""")
    summary_sql.show(10)

    summary_df = invoice_df \
        .groupBy("Country", "InvoiceNo") \
        .agg(f.sum("Quantity").alias("TotalQuantity"),
             f.round(f.sum(f.expr("Quantity * UnitPrice")), 2).alias("InvoiceValue"),
             f.expr("round(sum(Quantity * UnitPrice),2) as InvoiceValueExpr")
             )

    summary_df.show()

    # invoice_df.printSchema()

    challenge_sql= spark.sql(
        """
        select Country, weekofyear(to_date(InvoiceDate, 'MM-dd-yyyy H.mm')) as WeekNumber,
        count(Distinct InvoiceNo) as NumVoices, sum(Quantity) as TotalQuantity,
        round(sum(Quantity * UnitPrice),2) as InvoiceValue
        from sales
        group by Country, weekofyear(to_date(InvoiceDate, 'MM-dd-yyyy H.mm'))
        """
    )

    challenge_sql.show(20)

    # now with dataframe

    NumVoices= f.countDistinct("InvoiceNo").alias("NumInvoices")
    TotalQuantity= f.sum("Quantity").alias("TotalQuantity")
    InvoiceValue= f.expr("round(sum(Quantity * UnitPrice),2) as InvoiceValue")

    ex_summary_df= invoice_df.withColumn("InvoiceDate", f.to_date("InvoiceDate","dd-MM-yyyy H.mm")) \
                            .where("year(InvoiceDate) == 2010") \
                            .withColumn("WeekNumber", f.weekofyear(f.col("InvoiceDate"))) \
                            .groupby("Country", "WeekNumber") \
                            .agg( NumVoices, TotalQuantity, InvoiceValue)

    ex_summary_df.coalesce(1) \
                .write \
                .format("parquet") \
                .mode("overwrite") \
                .save("output")

    ex_summary_df.sort("Country","WeekNumber").show()

    ex_summary_df.createOrReplaceTempView("grouped_sales")

    # Window Aggregates Partitioning by df

    running_total= Window.partitionBy("Country") \
                    .orderBy("WeekNumber") \
                    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    ex_summary_df.withColumn("Running_Total", f.sum("InvoiceValue").over(running_total)).show()

    window_sql= spark.sql("""
    select sum(InvoiceValue) over( partition by Country order by WeekNumber  RANGE BETWEEN UNBOUNDED PRECEDING AND
    CURRENT ROW) as RunningTotal from grouped_sales
    """)
    window_sql.show()

    # ranks
    # summary_df_1 = spark.read.csv("data/invoices.csv")
    #
    # summary_df_1.sort("Country", "WeekNumber").show()
    #
    # rank_window = Window.partitionBy("Country") \
    #     .orderBy(f.col("InvoiceValue").desc()) \
    #     .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    #
    # df_1 = summary_df_1.withColumn("Rank", f.dense_rank().over(rank_window)) \
    #     .where(f.col("Rank") == 1) \
    #     .sort("Country", "WeekNumber") \
    #     .show()
