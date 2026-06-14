# Three teams. Three field names. One broken consumer.

Service A called it `imageId`. Service B called it `imgId`. Service C called it `image_id`.

All three were correct — in their own codebase. The consumer had to deal with all three.

---

## What actually happened

A client came to me with deserialization failures across their Kafka consumers. Every team owned a different service. Every service produced to the same topic. Nobody had agreed on a schema.

The consumer was doing this:

```python
image_id = (
    data.get("imageId") or
    data.get("imgId") or
    data.get("image_id") or
    "UNKNOWN"
)
```

Real code. In production.

And still getting `UNKNOWN`, because someone had added `ImageID` with a capital I.

---

## The fix

Confluent Schema Registry with Avro. One schema per topic. If your field is named wrong, the message never reaches Kafka.

```
Producer (Team A) ──→ Schema Registry check ──→ Kafka ──→ Consumer
Producer (Team B) ──→ Schema Registry check ──→ ✗ rejected
Producer (Team C) ──→ Schema Registry check ──→ ✗ rejected
```

The consumer stops guessing. It reads `imageId`. Always.

---

## Architecture

```
Kafka
  └── image-events-chaos   ← the old world (plain JSON, no contract)
  └── image-events         ← the fixed world (Avro, Schema Registry enforced)

Schema Registry
  └── image-events-value   ← registered schema, BACKWARD compatibility set
```

---

## Run it yourself

### Prerequisites

- Docker & Docker Compose
- Python 3.10+

### 1. Start the stack

```bash
./start.sh
```

Starts Kafka, Schema Registry, and Kafka UI. Registers the Avro schema. Sets BACKWARD compatibility.

### 2. Set up Python environment

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Demo sequence

**Step 1 — Show the chaos:**

```bash
python chaos_producer.py
```

Three services producing to the same topic. Three different field names.

**Step 2 — Show what the consumer deals with:**

```bash
python chaos_consumer.py
```

Watch it try to extract `imageId` from messages that call it something else.

**Step 3 — The fix:**

```bash
python producer.py
```

All three teams now produce Avro. Schema Registry enforces the contract at produce time.

**Step 4 — Clean consumer:**

```bash
python consumer.py
```

No `get("imageId") or get("imgId") or get("image_id")`. Just `event["imageId"]`.

**Step 5 — Try to break it:**

```bash
python break_schema.py
```

A service tries to send `imgId` instead of `imageId`. Schema Registry rejects it before it reaches Kafka.

---

## What breaks in production

- **Schema evolution** — adding a required field without a default breaks BACKWARD compatibility. Use optional fields with defaults for safe additions.
- **Compatibility mode matters** — BACKWARD means new consumers can read old messages. FORWARD means old consumers can read new messages. FULL means both. Pick the wrong one and you'll have a bad day.
- **Schema Registry as a single point of failure** — if it goes down, producers can't register new schemas. Cache schemas locally in production.
- **Confluent Cloud vs self-hosted** — managed Schema Registry works out of the box. Self-hosted needs monitoring. The registry itself has no built-in alerting.

---

## Files

```
├── docker-compose.yml       # Kafka + Schema Registry + Kafka UI
├── schemas/
│   └── image-event.avsc     # The agreed Avro schema
├── chaos_producer.py        # 3 teams, 3 formats, 1 topic
├── chaos_consumer.py        # The if/else workaround in action
├── producer.py              # Clean Avro producer
├── consumer.py              # Clean Avro consumer
├── break_schema.py          # What happens when imgId tries to sneak in
├── register-schema.sh       # Registers schema + sets BACKWARD compatibility
└── start.sh                 # One-command startup
```

---

**Serhat Kayikci** — [serhat.tech](https://serhat.tech) | [github.com/skayikci](https://github.com/skayikci) | [LinkedIn](https://linkedin.com/in/serhatkayikci)
