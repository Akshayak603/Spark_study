
from unittest import TestCase
from pyspark.sql import SparkSession
from lib.utils import load_df, group_count


class UtilsTestCase(TestCase):
    '''To ensure it will run first'''
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder \
            .master("local[3]") \
            .appName("Hello Spark") \
            .getOrCreate()

    def test_load_data(self):
        sample_df = load_df(self.spark, "../02-RDD/data/sample.csv")
        result_count = sample_df.count()
        self.assertEqual(result_count, 9, "Record count should be 9")

    def test_country_count(self):
        sample_df = load_df(self.spark, "../02-RDD/data/sample.csv")
        count_list = group_count(sample_df).collect()
        dictionary_result = {i[0]: i[1] for i in count_list}
        self.assertEqual(dictionary_result["United States"], 4, "USA count should be 4")
        self.assertEqual(dictionary_result["Canada"], 2, "Canada count should be 2")
        self.assertEqual(dictionary_result["United Kingdom"], 1, "UK count should be 1")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()