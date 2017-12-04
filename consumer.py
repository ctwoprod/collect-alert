from kafka import KafkaConsumer
import avro.schema
import avro.io
import io

# To consume messages
consumer = KafkaConsumer('test',
                         group_id='group1',
                         bootstrap_servers=['localhost:9092'])

schema_path="AlertNotification.avsc"
schema = avro.schema.parse(open(schema_path).read())

for msg in consumer:
    bytes_reader = io.BytesIO(msg.value)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(schema)
    field1 = reader.read(decoder)
    print field1
