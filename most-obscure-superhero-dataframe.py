from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostObscureSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("/Users/Rattlehead/Documents/repo/spark/Marvel-Names.txt")

lines = spark.read.text("/Users/Rattlehead/Documents/repo/spark/Marvel-Graph.txt")

connections = lines.withColumn("id", func.split(func.col("value"), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.col("value"), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))
    
mostObscure = connections.filter(func.col("connections") == 1)


mostObscure = mostObscure.join(names, "id")
mostObscure.show()
