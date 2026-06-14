"""
The old world.

Three teams. One topic. Nobody agreed on field names.
Team Alpha called it imageId. Team Beta called it imgId. Team Gamma called it image_id.
All three were right, in their own codebase.
"""

from confluent_kafka import Producer
import json
import time
import random

TOPIC = "image-events-chaos"

producer = Producer({"bootstrap.servers": "localhost:9092"})


def team_alpha(image_id: str) -> dict:
    return {
        "imageId": image_id,
        "title": f"Product shot {image_id}",
        "url": f"https://cdn.example.com/{image_id}.jpg",
        "uploadedBy": "team-alpha",
        "uploadedAt": int(time.time() * 1000),
    }


def team_beta(image_id: str) -> dict:
    return {
        "imgId": image_id,
        "title": f"Product shot {image_id}",
        "URL": f"https://cdn.example.com/{image_id}.jpg",
        "user": "team-beta",
        "timestamp": int(time.time() * 1000),
    }


def team_gamma(image_id: str) -> dict:
    return {
        "image_id": image_id,
        "name": f"Product shot {image_id}",
        "link": f"https://cdn.example.com/{image_id}.jpg",
        "creator": "team-gamma",
        "created_at": int(time.time() * 1000),
    }


teams = [
    ("Team Alpha", team_alpha),
    ("Team Beta",  team_beta),
    ("Team Gamma", team_gamma),
]

print("Producing chaos — 3 teams, 3 formats, 1 topic.")
print("=" * 55)

for i in range(9):
    name, builder = teams[i % 3]
    image_id = f"img-{random.randint(1000, 9999)}"
    message = builder(image_id)
    print(f"[{name}] {json.dumps(message)}")
    producer.produce(TOPIC, value=json.dumps(message).encode("utf-8"))
    producer.flush()
    time.sleep(0.3)

print()
print("Done. Run chaos_consumer.py to see what the consumer deals with.")
