import pandas as pd

from datetime import datetime, date
from environment import print
from pyspark.sql import SparkSession, Row, types


# Util functions
def create_spark_session():
    # spark = SparkSession.builder.master('local').appName('ml').getOrCreate()
    spark = SparkSession.builder.getOrCreate()

    # Setting the log level
    spark.sparkContext.setLogLevel("WARN")

    # Printing the environment
    print(f'Spark version: {spark} - {spark.version}')

    # Print the tables in the catalog
    print('Available tables:', spark.catalog.listTables())
    return spark


spark = create_spark_session()

# Reading data
print('\nReading table:', end='\n')
spark.read.json('data-sources/people.json').show()

# Create a PySpark DataFrame from a list of rows
df = spark.createDataFrame([
    Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),  # noqa
    Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),  # noqa
    Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))   # noqa
])
print('Creating a simple spark dataframe:', df)

# Create a PySpark DataFrame with an explicit schema.
df = spark.createDataFrame([
    (1, 2., 'string1', date(2000, 1, 1), datetime(2000, 1, 1, 12, 0)),
    (2, 3., 'string2', date(2000, 2, 1), datetime(2000, 1, 2, 12, 0)),
    (3, 4., 'string3', date(2000, 3, 1), datetime(2000, 1, 3, 12, 0))
], schema='a long, b double, c string, d date, e timestamp')
print('Creating a spark dataframe with an schema:', df)

# Create a PySpark DataFrame from a pandas DataFrame
pandas_df = pd.DataFrame({
    'a': [1, 2, 3],
    'b': [2., 3., 4.],
    'c': ['string1', 'string2', 'string3'],
    'd': [date(2000, 1, 1), date(2000, 2, 1), date(2000, 3, 1)],
    'e': [datetime(2000, 1, 1, 12, 0), datetime(2000, 1, 2, 12, 0), datetime(2000, 1, 3, 12, 0)]  # noqa
})
data_schema = types.StructType([
    types.StructField('a', types.IntegerType(), True),
    types.StructField('b', types.FloatType(), True),
    types.StructField('c', types.StringType(), True),
    types.StructField('d', types.DateType(), True),
    types.StructField('e', types.TimestampType(), True),
])
df = spark.createDataFrame(pandas_df)
print('Creating a spark dataframe from pandas (Infering schema):', df)
df.printSchema()
df.show()

df = spark.createDataFrame(pandas_df, schema=data_schema)
print('Creating a spark dataframe from pandas (Explicit schema):', df)
print('Object type', type(df))
df.printSchema()
df.show()
