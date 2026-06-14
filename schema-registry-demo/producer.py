"""
The fixed world.

Every team produces to the same schema. Schema Registry enforces it.
If your field is named wrong, it never reaches Kafka.
"""

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
import time
import random

TOPIC = "image-events"

schema_registry_client = SchemaRegistryClient({"url": "http://localhost:8081"})

with open("schemas/image-event.avsc") as f:
    schema_str = f.read()

avro_serializer = AvroSerializer(schema_registry_client, schema_str)
producer = Producer({"bootstrap.servers": "localhost:9092"})

messages = [
    {"imageId": f"img-{random.randint(1000,9999)}", "title": "Hero banner",     "url": "https://cdn.example.com/hero.jpg",      "uploadedBy": "team-alpha", "uploadedAt": int(time.time() * 1000)},
    {"imageId": f"img-{random.randint(1000,9999)}", "title": "Product close-up", "url": "https://cdn.example.com/closeup.jpg",   "uploadedBy": "team-beta",  "uploadedAt": int(time.time() * 1000)},
    {"imageId": f"img-{random.randint(1000,9999)}", "title": "Lifestyle shot",   "url": "https://cdn.example.com/lifestyle.jpg", "uploadedBy": "team-gamma", "uploadedAt": int(time.time() * 1000)},
]

print("Producing Avro messages — Schema Registry enforces the contract.")
print("=" * 55)

for msg in messages:
    producer.produce(
        topic=TOPIC,
        value=avro_serializer(msg, SerializationContext(TOPIC, MessageField.VALUE)),
    )
    producer.flush()
    print(f"✓  imageId={msg['imageId']}  uploadedBy={msg['uploadedBy']}")
    time.sleep(0.3)

print()
print("All messages conform to the schema. Run consumer.py.")
