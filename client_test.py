from confluent_kafka import Producer

if __name__ == '__main__':
    producer = Producer({ 'bootstrap.servers': 'localhost:9092'})


    def delivery_callback(err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
                            (msg.topic(), msg.partition(), msg.offset()))

    topic = 'my-topic'
    producer.produce(topic, 'hello_world', callback=delivery_callback)
    producer.flush()