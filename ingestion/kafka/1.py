msg_count = 1000000
msg_size = 100
msg_payload = ('kafkatest' * 20).encode()[:msg_size]
print(msg_payload)
print(len(msg_payload))

bootstrap_servers=''
producer_timings = {}
consumer_timings = {}

def calculate_thoughput(timing, n_messages=1000000, msg_size=100):
    print("Processed {0} messsages in {1:.2f} seconds".format(n_messages, timing))
    print("{0:.2f} MB/s".format((msg_size * n_messages) / timing / (1024*1024)))
    print("{0:.2f} Msgs/s".format(n_messages / timing))

from kafka import KafkaProducer

def python_kafka_producer_performance():
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    producer_start = time.time()
    topic = 'python-kafka-topic'
    for i in range(msg_count):
        producer.send(topic, msg_payload)
        
    producer.flush() # clear all local buffers and produce pending messages
        
    return time.time() - producer_start

producer_timings['python_kafka_producer'] = python_kafka_producer_performance()
calculate_thoughput(producer_timings['python_kafka_producer'])
