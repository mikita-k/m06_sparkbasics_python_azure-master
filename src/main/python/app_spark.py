from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType


spark = SparkSession.builder\
    .appName("test spark session")\
    .config("spark.some.config.option", "some-value")\
    .getOrCreate()

"""
    .config("fs.azure.account.auth.type.bd201stacc.dfs.core.windows.net", "OAuth")\
    .config("fs.azure.account.oauth.provider.type.bd201stacc.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")\
    .config("fs.azure.account.oauth2.client.id.bd201stacc.dfs.core.windows.net", "f3905ff9-16d4-43ac-9011-842b661d556d")\
    .config("fs.azure.account.oauth2.client.secret.bd201stacc.dfs.core.windows.net", "mAwIU~M4~xMYHi4YX_uT8qQ.ta2.LTYZxT")\
    .config("fs.azure.account.oauth2.client.endpoint.bd201stacc.dfs.core.windows.net", "https://login.microsoftonline.com/b41b72d0-4e9f-4c26-8a69-f949f367c91d/oauth2/token")\
"""

sc = spark.sparkContext

path = "lesson1.csv"

df = spark.read.csv(path)
df.show()

df2 = spark.read.option("delimiter", ';').option("header", True).csv(path)
df2.show()

df2.filter(col("age") < 20).show()
df2.filter("age > 23").show()
df2.printSchema()

schema = StructType([
    StructField('first', StringType(), True),
    StructField('last', StringType(), True),
    StructField('age', IntegerType(), True),
    StructField('state', StringType(), True)
])

print("next code does not work:")
df3 = spark.read.option("delimiter", ';').option("header", True).schema(schema).csv(path)
df3.printSchema()
