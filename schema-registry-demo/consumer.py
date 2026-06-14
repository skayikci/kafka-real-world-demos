"""
No guessing. No if/else. No UNKNOWN.
Every message comes in with the same field names — guaranteed by Schema Registry.
"""

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

schema_registry_client = SchemaRegistryClient({"url": "http://localhost:8081"})
avro_deserializer = AvroDeserializer(schema_registry_client)

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "clean-consumer",
    "auto.offset.reset": "earliest",
})
consumer.subscribe(["image-events"])

print("Consuming Avro messages — every field is exactly where you expect it.")
print("=" * 55)

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
            continue

        event = avro_deserializer(msg.value(), SerializationContext("image-events", MessageField.VALUE))
        print(f"✓  imageId={event['imageId']}  title={event['title']}  uploadedBy={event['uploadedBy']}")

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
