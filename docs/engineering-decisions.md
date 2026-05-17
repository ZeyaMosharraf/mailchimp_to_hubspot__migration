# Engineering Decisions

This document records key design decisions for the migration platform.

---

## 1) Checkpointing for Progress Durability

**Decision**  
Persist migration progress as an offset checkpoint after each processed batch.

**Context**  
Long-running API migrations are interruption-prone.

**Problem**  
Without durable progress state, interruption forces full rerun and increases migration risk.

**Chosen Solution**  
Store `offset` in `state/checkpoint.json` and resume from that boundary on restart.

**Tradeoffs**  
- Simple and low-overhead  
- Single-runner oriented  
- Coarse-grained replay boundary at page level

**Future Evolution**  
Move checkpoint state to transactional metadata storage for distributed workers.

---

## 2) Isolated REST Clients

**Decision**  
Keep Mailchimp and HubSpot API logic in dedicated client modules.

**Context**  
Source and sink APIs have different auth, contracts, and failure behavior.

**Problem**  
Inline API code in orchestration path increases coupling and makes change-risk high.

**Chosen Solution**  
Use `clients/mailchimp_client.py` and `clients/hubspot_client.py` as transport boundaries.

**Tradeoffs**  
- More modules to maintain  
- Clearer testability and failure isolation

**Future Evolution**  
Abstract client interfaces for additional CRM targets.

---

## 3) Batch Upsert into HubSpot

**Decision**  
Write contacts via HubSpot batch upsert endpoint.

**Context**  
Per-record writes amplify request volume and latency.

**Problem**  
Large migrations require controlled API call footprint.

**Chosen Solution**  
Submit grouped records in batch with `idProperty=email`.

**Tradeoffs**  
- Better throughput efficiency  
- Partial batch failure handling complexity

**Future Evolution**  
Add per-record result capture and replay queue for failed subset records.

---

## 4) File-Based State

**Decision**  
Use local JSON file as checkpoint persistence layer.

**Context**  
Current execution model is single-process migration runner.

**Problem**  
Need durable state without adding external infrastructure dependency.

**Chosen Solution**  
Persist offset in `state/checkpoint.json`.

**Tradeoffs**  
- Operationally simple  
- Not suitable for distributed coordination

**Future Evolution**  
Adopt Redis/PostgreSQL/DynamoDB for shared checkpoint and run metadata.

---

## 5) Synchronous Execution

**Decision**  
Use synchronous orchestration loop for extraction-transform-load flow.

**Context**  
Primary objective is deterministic reliability.

**Problem**  
Concurrent execution increases ordering and state-consistency complexity.

**Chosen Solution**  
Single-threaded control flow with explicit batch boundaries.

**Tradeoffs**  
- Predictable behavior and easier recovery  
- Lower peak throughput compared to async worker pools

**Future Evolution**  
Introduce partitioned async workers with centralized state and backpressure controls.

---

## 6) Isolated Transformation Layer

**Decision**  
Separate schema mapping logic from API client code.

**Context**  
Source and sink data contracts evolve independently.

**Problem**  
Coupled mapping + transport logic slows change velocity and increases defect risk.

**Chosen Solution**  
Encapsulate mapping in `transformers/contact_mapping.py`.

**Tradeoffs**  
- Additional module boundary  
- Stronger schema governance and testability

**Future Evolution**  
Move to declarative schema mapping + validation rules.

---

## 7) Retry and Backoff Envelopes

**Decision**  
Use retry-enabled sessions with status-class-based recovery behavior.

**Context**  
External APIs are not consistently available under migration loads.

**Problem**  
Transient errors should not force pipeline failure.

**Chosen Solution**  
Apply retries with backoff for 429 and 5xx classes; enforce request timeouts.

**Tradeoffs**  
- Longer completion time during degraded windows  
- Significantly improved completion reliability

**Future Evolution**  
Adaptive retry tuning with dynamic rate-limit feedback and circuit-breaking.

