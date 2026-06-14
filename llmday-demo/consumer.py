"""
LLMday Hamburg — CDC-Powered AI Context Demo
PostgreSQL → Debezium → Kafka → Gemini

Run:
    python consumer.py
"""

import json
import threading
import time
import os
from confluent_kafka import Consumer, KafkaError
from google import genai
from google.genai import types
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_TOPIC     = "demo.public.products"
KAFKA_BOOTSTRAP = "localhost:9092"
GEMINI_MODEL    = "gemini-3.5-flash"

# ── Shared state (updated by background Kafka thread) ─────────────────────────
product_state = {}
state_lock    = threading.Lock()

_api_key = os.getenv("GEMINI_API_KEY")
if not _api_key:
    raise SystemExit("Error: GEMINI_API_KEY not set. Copy .env.example to .env and add your key.")

client = genai.Client(api_key=_api_key)


def get_snapshot() -> dict:
    with state_lock:
        return dict(product_state)


# ── Debezium event parser ─────────────────────────────────────────────────────

def parse_event(raw_value: str):
    """
    Parse a Debezium PostgreSQL change event.
    Returns (name, old_status, new_status, op) or (None, None, None, None).

    Debezium ops:
        r = read   (initial snapshot)
        c = create (INSERT)
        u = update (UPDATE)
        d = delete (DELETE) — 'after' is null, data is in 'before'
    """
    try:
        data    = json.loads(raw_value)
        payload = data.get("payload", data)

        op     = payload.get("op")
        after  = payload.get("after")
        before = payload.get("before")

        if after:
            return (
                after.get("name"),
                before.get("status") if before else None,
                after.get("status"),
                op,
            )
        elif op == "d" and before:
            # DELETE: after is null, read name/status from before
            return (before.get("name"), before.get("status"), None, op)

    except (json.JSONDecodeError, AttributeError, TypeError) as e:
        print(f"[parse_event] skipped malformed message: {e}")

    return None, None, None, None


# ── Kafka consumer thread ─────────────────────────────────────────────────────

def kafka_consumer_thread():
    def seek_to_beginning(consumer, partitions):
        """Always replay from offset 0 to rebuild product state from scratch."""
        for p in partitions:
            p.offset = 0
        consumer.assign(partitions)

    consumer = Consumer({
        "bootstrap.servers":  KAFKA_BOOTSTRAP,
        "group.id":           "llmday-demo",
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": False,   # no committed offsets — always replay
    })
    consumer.subscribe([KAFKA_TOPIC], on_assign=seek_to_beginning)

    print(f"[Kafka] Connected → topic: {KAFKA_TOPIC}\n")

    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"[Kafka Error] {msg.error()}")
            continue

        if msg.value() is None:
            continue  # tombstone message — skip

        name, old_status, new_status, op = parse_event(msg.value().decode("utf-8"))

        if not name:
            continue

        with state_lock:
            if op == "d":
                product_state.pop(name, None)
            elif new_status is not None:
                product_state[name] = new_status

        if op in ("c", "u"):
            change = f"{old_status} → {new_status}" if old_status is not None else new_status
            print(f"\n  🔄 [CDC EVENT] {name}: {change}")
            print(  "  Context updated. LLM will answer with fresh data.\n> ", end="", flush=True)
        elif op == "d":
            print(f"\n  🗑️  [CDC EVENT] {name} deleted from catalogue\n> ", end="", flush=True)


# ── Gemini call ───────────────────────────────────────────────────────────────

def ask(question: str) -> str:
    snapshot = get_snapshot()

    if not snapshot:
        return "Product catalogue is still loading — please try again in a moment."

    catalogue = "\n".join(f"  - {name}: {status}" for name, status in snapshot.items())

    system_prompt = f"""You are a helpful customer support assistant.
Answer questions based ONLY on the product catalogue below.
If a product is discontinued, say so clearly and do not suggest ordering it.
Keep answers short — two sentences maximum.

Current product catalogue (real-time via CDC):
{catalogue}"""

    response = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=question,
        config=types.GenerateContentConfig(
            system_instruction=system_prompt,
        ),
    )
    return response.text.strip()


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print()
    print("=" * 56)
    print("  LLMday Hamburg — CDC-Powered AI Context Demo")
    print("=" * 56)
    print("  Stack: PostgreSQL → Debezium → Kafka → Gemini")
    print("  Loading product catalogue from CDC snapshot...\n")

    t = threading.Thread(target=kafka_consumer_thread, daemon=True)
    t.start()

    time.sleep(5)

    snapshot = get_snapshot()

    if snapshot:
        items = list(snapshot.items())
        total = len(items)
        print(f"  Catalogue loaded — {total} products:")
        if total <= 6:
            for name, status in items:
                print(f"    {name:<30} {status}")
        else:
            for name, status in items[:3]:
                print(f"    {name:<30} {status}")
            print(f"    ... ({total - 6} more)")
            for name, status in items[-3:]:
                print(f"    {name:<30} {status}")
    else:
        print("  Still waiting for Kafka... is the connector running?")
        print("  Check: curl http://localhost:8083/connectors/postgres-connector/status")

    print("\n  Type a question and press Enter.")
    print("  Type 'state' to see current product state.")
    print("  Type 'quit' to exit.\n")

    while True:
        try:
            question = input("> ").strip()

            if not question:
                continue
            if question.lower() in ("quit", "exit", "q"):
                break
            if question.lower() == "state":
                with state_lock:
                    for name, status in product_state.items():
                        print(f"  {name}: {status}")
                print()
                continue

            answer = ask(question)
            print(f"\n  Chatbot: {answer}\n")

        except KeyboardInterrupt:
            break

    print("\nDemo ended.")


if __name__ == "__main__":
    main()
