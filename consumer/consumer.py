import json

from kafka import KafkaConsumer, KafkaProducer

MAX_RETRY = 5
producer = KafkaProducer(bootstrap_servers=['kafka_0:9092', 'kafka_1:9093', 'kafka_2:9094',], value_serializer=lambda x: json.dumps(x).encode('utf-8'))

consumer = KafkaConsumer("main_topic", bootstrap_servers=['kafka_0:9092', 'kafka_1:9093', 'kafka_2:9094',], auto_offset_reset='earliest', group_id="0")

print("starting the consumer")
for msg in consumer:
    try:
        print(msg)
        print(msg.value)
        if msg.value.decode() != json.dumps({"type": "message"}):
            ack = producer.send("dead_letter", json.loads(msg.value.decode()))
            metadata = ack.get()
            print(f"Send: '{metadata.topic}' partition: {metadata.partition} offset: {metadata.offset}")
            producer.flush()
        else:
            print("Done")
    except Exception as e:
        print(e)
