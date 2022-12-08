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

# /Users/hyunseokjung/kafka_2.12-3.3.1/bin/kafka-server-start.sh -daemon /Users/hyunseokjung/kafka_2.12-3.3.1/config/server.properties
# /Users/hyunseokjung/kafka_2.12-3.3.1/bin/kafka-topics.sh --describe --bootstrap-server 127.0.0.1:9092 --topic reco-train