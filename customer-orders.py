from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CustomerOrders")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    orders = float(fields[2])
    customerId = int(fields[0])
    return (customerId, orders)

lines = sc.textFile("/Users/Rattlehead/Documents/SparkCourse/customer-orders.csv")
rdd = lines.map(parseLine)
totalsByOrders = rdd.reduceByKey(lambda x, y: x + y)
results = totalsByOrders.collect()
for result in results:
    print(result)
