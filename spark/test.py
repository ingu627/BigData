from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName('streaming1') \
    .master("local") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
    
static = spark.read.json('/Users/hyunseokjung/data/spark_guide/activity-data/')
dataSchema = static.schema

streaming = spark.readStream.schema(dataSchema) \
    .option("maxFilesPerTrigger", 1) \
    .json('/Users/hyunseokjung/data/spark_guide/activity-data/')
    
activityCounts = streaming.groupBy("gt").count()
spark.conf.set("spark.sql.shuffle.partitions", 5)

activityQuery = activityCounts.writeStream \
    .queryName("activity_counts") \
    .format("memory").outputMode("complete") \
    .start()

from time import sleep

for _ in range(5):
    spark.sql('SELECT * FROM activity_counts').show()
    sleep(1)