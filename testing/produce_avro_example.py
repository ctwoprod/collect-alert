from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

value_schema = avro.load('AlertNotification.avsc')
key_schema = avro.load('AlertNotification.avsc')
value = {"name": "Value"}
key = {"name": "Key"}

avroProducer = AvroProducer({'bootstrap.servers': 'localhost', 'schema.registry.url': 'http://localhost:8081/schema'}, default_key_schema=key_schema, default_value_schema=value_schema)
avroProducer.produce(topic='test', value=value, key=key)
avroProducer.flush()


#authentication
#read from properties file
#fix the avro schema
