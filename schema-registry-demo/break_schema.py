"""
What happens when a team tries to send imgId instead of imageId.

Schema Registry has BACKWARD compatibility set on this topic.
The message never reaches Kafka.
"""

import json
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
import time

TOPIC = "image-events"

BREAKING_SCHEMA = json.dumps({
    "type": "record",
    "name": "ImageEvent",
    "namespace": "com.demo",
    "fields": [
        {"name": "imgId",       "type": "string"},
        {"name": "title",       "type": "string"},
        {"name": "url",         "type": "string"},
        {"name": "uploadedBy",  "type": "string"},
        {"name": "uploadedAt",  "type": "long"},
    ]
})

print("Attempting to send a message with 'imgId' instead of 'imageId'...")
print("Schema Registry has BACKWARD compatibility set on this topic.")
print("=" * 55)

try:
    schema_registry_client = SchemaRegistryClient({"url": "http://localhost:8081"})
    avro_serializer = AvroSerializer(schema_registry_client, BREAKING_SCHEMA)
    producer = Producer({"bootstrap.servers": "localhost:9092"})

    message = {
        "imgId": "img-9999",
        "title": "Rogue message",
        "url": "https://cdn.example.com/rogue.jpg",
        "uploadedBy": "rogue-service",
        "uploadedAt": int(time.time() * 1000),
    }

    producer.produce(
        topic=TOPIC,
        value=avro_serializer(message, SerializationContext(TOPIC, MessageField.VALUE)),
    )
    producer.flush()
    print("ERROR: this should have been rejected.")

except Exception as e:
    print(f"✗  Rejected: {e}")
    print()
    print("The rogue field name never reached Kafka.")
    print("The consumer never had to deal with it.")
