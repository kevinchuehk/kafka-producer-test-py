from confluent_kafka import Producer
import sys
import json

if __name__ == '__main__':
    producer = Producer({ 'bootstrap.servers': 'localhost:9092'})


    def delivery_callback(err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
                            (msg.topic(), msg.partition(), msg.offset()))

    topic = 'to-motechat'
    value = { 'topic' : 'echo', 'name': 'hello', 'data': 'hello_world'}
    dump = json.dumps(value).encode('utf-8')
    producer.produce(topic, dump, callback=delivery_callback)
    producer.flush()