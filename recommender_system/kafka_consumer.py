from kafka import KafkaConsumer 
from json import loads
import time
import datetime

consumer=KafkaConsumer("userid", 
                        bootstrap_servers=['127.0.0.1:9092'], 
                        group_id='my-group',
                        auto_offset_reset="earliest",
                        enable_auto_commit=True, 
                        consumer_timeout_ms=1000 # 1000초 이후에 메시지가 오지 않으면 없는 것으로 취급.
            )
start = time.time() # 현재 시간
print("START= ", start)
for message in consumer:
    print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value))

# print("Elapsed time= ",(time.time()-start)) # 걸리는 시간