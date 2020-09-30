from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

spark = SparkSession.builder.appName("MinTemperatures").getOrCreate()

schema = StructType([ \
                     StructField("userID", IntegerType(), True), \
                     StructField("productID", IntegerType(), True), \
                     StructField("amount", FloatType(), True)])

df = spark.read.schema(schema).csv("/Users/Rattlehead/Documents/repo/spark/customer-orders.csv")
df.printSchema()

df.groupBy("userID").sum("amount").show()

dfa = df.groupBy("userID").agg(func.round(func.sum("amount"), 2).alias("total_spent"))
dfa.show()

dfas = dfa.sort("total_spent")
dfas.show(dfas.count())

spark.stop()