import os
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"


import unittest    # noqa

from pyspark.sql import SparkSession    # noqa
from etl import remove_extra_spaces    # noqa
from pyspark.testing.utils import assertDataFrameEqual    # noqa


# Define unit test base class
class PySparkTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("Sample PySpark ETL").getOrCreate()  # noqa

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


# Define unit test
class TestTranformation(PySparkTestCase):
    def test_single_space(self):
        sample_data = [{"name": "John    D.", "age": 30},
                       {"name": "Alice   G.", "age": 25},
                       {"name": "Bob  T.", "age": 35},
                       {"name": "Eve   A.", "age": 28}]

        # Create a Spark DataFrame
        original_df = self.spark.createDataFrame(sample_data)

        # Apply the transformation function from before
        transformed_df = remove_extra_spaces(original_df, "name")

        expected_data = [
            {"name": "John D.", "age": 30},
            {"name": "Alice G.", "age": 25},
            {"name": "Bob T.", "age": 35},
            {"name": "Eve A.", "age": 28}
        ]

        expected_df = self.spark.createDataFrame(expected_data)

        assertDataFrameEqual(transformed_df, expected_df)
