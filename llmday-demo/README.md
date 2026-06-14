# Stop Polling Your Database for AI Context. Use CDC.
### LLMday Hamburg — June 18, 2026

**Talk abstract:**  
Somewhere right now, a support chatbot is confidently telling a customer about a product
that was discontinued six months ago. The model is fine. The vector database is fine.
The batch job that feeds it just hasn't run yet.

This demo shows a working alternative: PostgreSQL → Debezium → Kafka → Gemini,
where every database change reaches your AI context in seconds — not hours.

---

## The Story

### Without CDC (the broken world)

A customer asks: *"Is Widget Pro available for purchase?"*

The chatbot queries its context store. The context was last refreshed at 2am.
At 9am, a product manager ran `UPDATE products SET status = 'discontinued'`.
The chatbot answers: **"Yes, Widget Pro is available!"**

The customer places an order. The order fails. Everyone is unhappy.
The model did nothing wrong. The data pipeline did.

### With CDC (this demo)

Debezium is listening to PostgreSQL's Write-Ahead Log (WAL).
The moment that `UPDATE` runs, a change event appears in Kafka.
The consumer picks it up, updates the context, and calls Gemini with fresh state.

The customer asks again: **"Is Widget Pro available?"**  
The chatbot answers: *"Widget Pro has been discontinued and is no longer available."*

Same model. Same architecture. Real-time data layer added.

---

## Architecture

```
PostgreSQL (WAL)
     │
     ▼
Debezium Connect          ← watches the WAL, emits change events
     │
     ▼
Kafka (demo.public.products topic)
     │
     ▼
Python Consumer           ← maintains in-memory product state
     │
     ▼
Gemini API                ← answers questions with up-to-date context
```

---

## Run It Yourself

### Prerequisites
- Docker & Docker Compose
- Python 3.10+
- A Gemini API key (free at [aistudio.google.com](https://aistudio.google.com))

### 1. Start the stack

```bash
docker compose up -d
```

This starts PostgreSQL, Zookeeper, Kafka, Debezium Connect, and Kafka UI.

### 2. Set up Python environment

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

Create a `.env` file:
```bash
cp .env.example .env
# Edit .env and add your GEMINI_API_KEY
```

### 3. Run the demo

```bash
./start.sh
```

`start.sh` brings up the Docker stack, waits for Debezium Connect to be ready, registers the connector, then starts the consumer.

### 4. Live demo sequence

The consumer is running and waiting for questions. Open a second terminal for the database changes.

**Ask about a product:**
```
> is widget pro available?
```
→ *"Yes, Widget Pro is currently available."*

**Discontinue it:**
```bash
./discontinue.sh "Widget Pro"
```

Watch the CDC event appear in the consumer terminal:
```
🔄 [CDC EVENT] Widget Pro: available → discontinued
```

**Ask again:**
```
> is widget pro available?
```
→ *"Widget Pro has been discontinued and is no longer available."*

**Restock it:**
```bash
./restock.sh "Widget Pro"
```

```
🔄 [CDC EVENT] Widget Pro: discontinued → available
```

**Ask one more time:**
```
> is widget pro available?
```
→ *"Yes, Widget Pro is back in stock and available for purchase."*

The model didn't change. The context did. That's the whole point.

**Or use psql directly if you prefer:**
```bash
docker exec -it llmday-demo-postgres-1 psql -U demo -d demo
```
```sql
UPDATE products SET status = 'discontinued' WHERE name = 'Widget Pro';
UPDATE products SET status = 'available'    WHERE name = 'Widget Pro';
```

---

## What Breaks in Production

Real issues from the field — the part most tutorials skip:

- **Connector going FAILED silently** — heartbeat topics help, monitoring is mandatory
- **Schema evolution** — a column added in PostgreSQL breaks the consumer if not handled
- **Spaced column names** — `message.key.columns` cannot handle spaces (e.g. `Employee Id`), a hard regex constraint in Debezium; only fix is source rename or a custom SMT
- **At-least-once delivery** — duplicate events are normal; your consumer must be idempotent
- **Initial snapshot on large tables** — Debezium snapshots the full table on first connect; on millions of rows use `snapshot.mode=schema_only`
- **Managed Kafka (GCP, AWS MSK)** — custom connector support varies; GCP Managed Kafka does not support Debezium plugins

---

## Files

```
├── docker-compose.yml       # Full stack: PostgreSQL + Kafka + Debezium + Kafka UI
├── init.sql                 # Products table + seed data (REPLICA IDENTITY FULL included)
├── register-connector.sh    # Registers Debezium PostgreSQL connector (waits for readiness)
├── consumer.py              # Kafka consumer + Gemini integration
├── start.sh                 # One-command startup: docker stack + consumer
├── discontinue.sh           # ./discontinue.sh "Product Name" — sets status to discontinued
├── restock.sh               # ./restock.sh "Product Name" — sets status back to available
├── requirements.txt         # Python dependencies
└── .env.example             # Environment variable template
```

---

## Contact

**Serhat Kayikci** — Senior Backend Engineer, Kafka & CDC Specialist  
[serhat.tech](https://serhat.tech) | [github.com/skayikci](https://github.com/skayikci) | [LinkedIn](https://linkedin.com/in/serhatkayikci)

*Building real-time data pipelines for AI? Let's talk.*