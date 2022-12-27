import findspark

findspark.init('/Users/hyunseokjung/spark-3.3.0')

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10-assembly_2.12-3.3.1, org.apache.spark:spark-sql-kafka-0-10_2.12-3.3.1, org.apache.kafka:kafka-clients-3.3.1:3.3.0'

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql.functions import udf
from pyspark.sql.functions import col, pandas_udf, split
from pyspark.sql import SparkSession

kafka_bootstrap_servers = '127.0.0.1:9092'
topic = 'recommender-system'

spark = SparkSession \
    .builder \
    .appName("PySparkShell") \
    .getOrCreate()

df = spark \
    .readStream\
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss","False") \
    .option("subscribe", topic) \
    .load()
    



# Original setting

# Zookeeper Server 
# /Users/hyunseokjung/kafka_2.12-3.3.1/bin/zookeeper-server-start.sh /Users/hyunseokjung/kafka_2.12-3.3.1/config/zookeeper.properties
# Kafka Server 
# /Users/hyunseokjung/kafka_2.12-3.3.1/bin/kafka-server-start.sh /Users/hyunseokjung/kafka_2.12-3.3.1/config/server.properties
# Kafka Topic
# /Users/hyunseokjung/kafka_2.12-3.3.1/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --create --topic recommender-system --partitions 1 --replication-factor 1
# Kafka Producer
# /Users/hyunseokjung/kafka_2.12-3.3.1/bin/kafka-console-producer.sh --bootstrap-server 127.0.0.1:9092 --topic recommender-system
# Kafka Consumer
# /Users/hyunseokjung/kafka_2.12-3.3.1/bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic recommender-system

# csv to kafka topic
# /Users/hyunseokjung/kafka_2.12-3.3.1/bin/kafka-console-producer.sh --bootstrap-server 127.0.0.1:9092 --topic recommender-system1 < /Users/hyunseokjung/data/ml-latest-small/ratings.csv
# csv consumer



# this code setting

# Zookeeper Server 
# /Users/hyunseokjung/kafka_2.12-3.3.1/bin/zookeeper-server-start.sh /Users/hyunseokjung/kafka_2.12-3.3.1/config/zookeeper.properties
# Kafka Server 
# /Users/hyunseokjung/kafka_2.12-3.3.1/bin/kafka-server-start.sh /Users/hyunseokjung/kafka_2.12-3.3.1/config/server.properties
# Kafka Topic
# /Users/hyunseokjung/kafka_2.12-3.3.1/bin/kafka-topics.sh --create --zookeeper localhost:2181 --topic pyspark-kafka-demo --replication-factor 1 --partitions 3
# python
# python /Users/hyunseokjung/Github/BigData/recommender_system/kafka_demo.py
# kafka producer
# /Users/hyunseokjung/kafka_2.12-3.3.1/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic pyspark-kafka-demo