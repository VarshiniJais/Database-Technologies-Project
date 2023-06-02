from kafka import KafkaConsumer
import json

# create Kafka consumer
consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='earliest',
                         value_deserializer=lambda x:
                         json.loads(x.decode('utf-8')))

# subscribe to topics
topics = ["sports", "technology"]
consumer.subscribe(topics)

# consume and print data from each topic
for message in consumer:
    print("Topic:", message.topic)
    print("Data:", message.value)

