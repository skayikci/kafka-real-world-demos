# kafka-real-world-demos

Kafka patterns that actually work in production — no toy examples, no hand-waving.

Each demo is self-contained: a real problem, a working stack, and the failure modes most tutorials skip.

---

## Demos

| Demo | What it shows |
|------|--------------|
| [llmday-demo](./llmday-demo/) | PostgreSQL → Debezium → Kafka → Gemini: real-time AI context via CDC |

---

## Philosophy

Most Kafka tutorials stop at "message produced, message consumed." These don't.

Every demo includes:
- A working Docker Compose stack you can run in minutes
- The real problem it solves (not just the happy path)
- The production gotchas — schema evolution, at-least-once delivery, connector failures

---

**Serhat Kayikci** — [serhat.tech](https://serhat.tech) | [github.com/skayikci](https://github.com/skayikci) | [LinkedIn](https://linkedin.com/in/skayikci)
