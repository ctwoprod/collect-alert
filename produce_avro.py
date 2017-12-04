from kafka import SimpleProducer, KafkaClient
import avro.schema
import io, random
from avro.io import DatumWriter

# To send messages synchronously
kafka = KafkaClient('localhost:9092')
producer = SimpleProducer(kafka)

# Kafka topic
topic = "test"

# Path to user.avsc avro schema
schema_path="AlertNotification.avsc"
schema = avro.schema.parse(open(schema_path).read())


#send test 1
writer = avro.io.DatumWriter(schema)
bytes_writer = io.BytesIO()
encoder = avro.io.BinaryEncoder(bytes_writer)
writer.write({"source": "HIVE", "message": "Ada error","severity":"error","timestamp":"31122017010101"}, encoder)
raw_bytes = bytes_writer.getvalue()
producer.send_messages(topic, raw_bytes)

#send test 2
writer = avro.io.DatumWriter(schema)
bytes_writer = io.BytesIO()
encoder = avro.io.BinaryEncoder(bytes_writer)
writer.write({"source": "IMPALA", "message": "Ada WARNING","severity":"warning","timestamp":"31122017020202"}, encoder)
raw_bytes = bytes_writer.getvalue()
producer.send_messages(topic, raw_bytes)
