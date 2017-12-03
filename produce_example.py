from confluent_kafka import Producer

p = Producer({'bootstrap.servers': 'localhost:9092'})
p.produce('test', key='hello', value='world2')
p.flush(30)
