"""
The real-world workaround every consumer team ended up writing.
Three services, three field names, one fragile if/else chain.
"""

from confluent_kafka import Consumer
import json

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "chaos-consumer",
    "auto.offset.reset": "earliest",
})
consumer.subscribe(["image-events-chaos"])

print("Reading messages — trying to extract imageId from every message.")
print("=" * 55)

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
            continue

        data = json.loads(msg.value().decode("utf-8"))

        # This is the workaround. Every consumer team wrote a version of this.
        image_id = (
            data.get("imageId") or
            data.get("imgId") or
            data.get("image_id") or
            "UNKNOWN"
        )

        if image_id == "UNKNOWN":
            print(f"⚠  Can't find image ID. Raw message: {data}")
        else:
            raw_key = next((k for k in data if "id" in k.lower()), "?")
            print(f"✓  imageId={image_id}  (came in as: '{raw_key}')")

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
