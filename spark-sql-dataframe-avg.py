from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("/Users/Rattlehead/Documents/repo/spark/fakefriends-header.csv")
    
print("Here is our inferred schema:")
people.printSchema()

print("Group by age")
people.groupBy("age").avg("friends").show()


spark.stop()

