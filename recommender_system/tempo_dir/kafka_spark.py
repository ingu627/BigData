from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__ == "__main__":
    
    sc = SparkContext(appName = "Kafka Spark Demo")
    ssc = StreamingContext(sc, 60)
    message = KafkaUtils.createDirectStream(
        ssc, 
        topics=['testtopic'],
        kafkaParams={'metadata.broker.list':'localhost:9092'})
    
    words = message.map(lambda x: x[1]).flatMap(lambda x: x.split(' '))
    
    wordcount = words.map(lambda x: (x, 1)).reduceByKey(lambda a, b: a+b)
    
    wordcount.pprint()
    
    
    ssc.start()
    ssc.awaitTermination()

# Zookeeper Server 
# /Users/hyunseokjung/kafka_2.12-3.3.1/bin/zookeeper-server-start.sh /Users/hyunseokjung/kafka_2.12-3.3.1/config/zookeeper.properties
# Kafka Server 
# /Users/hyunseokjung/kafka_2.12-3.3.1/bin/kafka-server-start.sh /Users/hyunseokjung/kafka_2.12-3.3.1/config/server.properties
# Kafka Topic
# /Users/hyunseokjung/kafka_2.12-3.3.1/bin/kafka-topics.sh --create --zookeeper localhost:2181 --topic video-stream-event --replication-factor 1 --partitions 3
# Kafka Producer
# /Users/hyunseokjung/kafka_2.12-3.3.1/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic video-stream-event
# Kafka Consumer
# /Users/hyunseokjung/kafka_2.12-3.3.1/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic video-stream-event --from-beginning