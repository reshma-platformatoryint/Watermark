# Take-Home Assignment: Real-Time Order Processing with Kafka and Databricks

**Course module:** Streaming Data Engineering
**Platform:** Databricks Free Edition (serverless) + local Docker
**Estimated effort:** 12–16 hours
**Due:** _[instructor to fill in]_
**Submission:** Git repository link + report

---

## 1. What you are building

You run the data platform for **Spice Route Kitchens**, a quick-service restaurant chain with eight outlets across Indian cities. Orders arrive through four channels (dine-in, takeaway, delivery, drive-thru) and are paid by UPI, card, cash, or wallet.

Today the chain batch-loads yesterday's orders every morning. Operations wants live numbers: revenue per outlet as it happens, which items are spiking, and — most importantly — which orders were placed but never paid for.

```
┌──────────────────────────── YOUR LAPTOP ────────────────────────────┐
│                                                                     │
│   producer.py  ──►  Kafka (Docker, localhost:9092)                  │
│   • steady flow          topics: qsr.orders                         │
│   • late arrivals                qsr.payments                       │
│   • traffic bursts               │                                  │
│   • unpaid orders                ▼                                  │
│                          bridge_consumer.py                         │
│                          long-running daemon — start once:          │
│                          poll → batch → PUT → commit → repeat       │
│                          (no human touches a file, ever)            │
└──────────────────────────────────┬──────────────────────────────────┘
                                   │  HTTPS (Databricks Files API)
                                   ▼
┌────────────────────── DATABRICKS FREE EDITION ──────────────────────┐
│                                                                     │
│  /Volumes/.../landing/orders/    ──►  Auto Loader  ──►  bronze      │
│  /Volumes/.../landing/payments/       driven by a scheduled Job     │
│                                       (Section 7.5) — also hands-off│
│                                                          │          │
│                                                          ▼          │
│                                                 silver_order_lines  │
│                                                          │          │
│       ┌────────────────┬─────────────────┬───────────────┤          │
│       ▼                ▼                 ▼               ▼          │
│  watermarking   tumbling windows   sliding windows     joins        │
│                                                  (stream–static,    │
│                                                   stream–stream)    │
└─────────────────────────────────────────────────────────────────────┘
```

### 1.1 Why the data flows this way — read this first

The obvious design is to point `spark.readStream.format("kafka")` at your broker. **On Databricks Free Edition this cannot work**, for two independent reasons:

1. Free Edition gives you serverless compute only, and serverless **restricts outbound internet access to a limited set of trusted domains**. Your laptop's broker will never be on that list, and you cannot configure private networking to add it.
2. Your broker sits behind NAT with no public IP, so there is no route to it in the first place.

You could expose it with a tunnel — and on a paid workspace with classic compute, that is a legitimate approach. On Free Edition it is a dead end.

So we **invert the direction of travel**. Instead of Databricks reaching *out* to Kafka, a consumer on your laptop reaches *in* to Databricks over HTTPS. Outbound HTTPS from your machine is never blocked, and the Databricks REST API is reachable by definition.

This is not a workaround hack. Pushing from the edge into a managed platform is a common real pattern — the broker stays private, no inbound firewall rules are needed, and the credential lives in one place. But it has real trade-offs, and **naming them is part of your grade**:

| | Direct `format("kafka")` | Bridge consumer (this assignment) |
|---|---|---|
| Offsets managed by | Spark, in the checkpoint | You, in the consumer group |
| Delivery semantics | Exactly-once into Delta | At-least-once — **you must handle duplicates** |
| Latency floor | Sub-second possible | Your bridge's flush interval |
| Backpressure | Spark controls the read rate | You must implement it |
| Kafka skills exercised | Configuring a source | Consumer groups, poll loops, offset commits |

That "at-least-once" row matters later: the duplicates you deduplicate in Part 5 are not synthetic. They are a genuine consequence of the architecture you built.

### 1.2 Nothing in this pipeline is operated by hand

To be explicit, because the word "upload" can suggest otherwise:

| Stage | How it runs | Human involvement |
|---|---|---|
| `producer.py` | Long-running process | Start once |
| Kafka | Docker container | `docker compose up -d` once |
| `bridge_consumer.py` | Long-running daemon: poll → batch → PUT → commit → loop | Start once |
| Auto Loader → bronze/silver/gold | **Scheduled Databricks Job** (Section 7.4) | Set the schedule once |

There is no step where you drag a file into a browser. The bridge does exactly what a Kafka Connect sink worker does — consumes from a topic and writes to a destination — except you write it yourself, which is the point of Part 2.

The one thing you cannot have on Free Edition is an *always-on* streaming query, because serverless supports only `Trigger.AvailableNow()`. A scheduled Job closes that gap: each run processes everything that has arrived since the last run and exits. Latency becomes minutes rather than seconds, which is a real cost — quantify it in your report.

---

## 2. Learning objectives

By the end you should be able to:

1. Stand up a Kafka broker in Docker and explain listeners and advertised listeners.
2. Write a Kafka **consumer** with a consumer group, poll loop, and explicit offset commits — and reason about where the delivery guarantee breaks.
3. Explain why a cloud platform cannot reach a laptop broker, and evaluate the alternatives.
4. Ingest continuously arriving files with **Auto Loader** and explain what its checkpoint tracks.
5. Explain **event time vs processing time**, and set a watermark that reflects a real business tolerance.
6. Demonstrate, with evidence, the difference between a record that is **late but inside** the watermark and one that is **late and outside** it.
7. Deduplicate a stream and explain what bounds the dedup state.
8. Choose correctly between **tumbling** and **sliding** windows and defend the choice.
9. Implement **stream–static** and **stream–stream** joins, and explain why the second needs a watermark on both sides plus a time constraint.

You are assessed on **explanation as much as on code**. Working code with no analysis scores poorly.

---

## 3. Prerequisites

**Local:**
- Docker Desktop (or Docker Engine + Compose v2)
- Python 3.10+
- `pip install confluent-kafka databricks-sdk`


## 4. Part 1 — Kafka in Docker (10 points)

Because the broker now only ever serves clients on your own machine, the configuration is simple: no SASL, no tunnel, no external listener.

### 4.1 `docker-compose.yml`

```yaml
services:
  kafka:
    image: apache/kafka:3.9.0
    container_name: qsr-kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      CLUSTER_ID: 5L6g3nShT-eMCtK--X86sw

      KAFKA_LISTENERS: CONTROLLER://:9093,HOST://:9092
      KAFKA_ADVERTISED_LISTENERS: HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,HOST:PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_INTER_BROKER_LISTENER_NAME: HOST

      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
```

Environment-variable-to-config translation varies between images and versions. **If the broker will not start, read `docker compose logs kafka` — the error names the offending property.**

### 4.2 Create the topics

Topics do not auto-create, deliberately:

```bash
docker exec qsr-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create \
  --topic qsr.orders --partitions 3 --replication-factor 1

docker exec qsr-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create \
  --topic qsr.payments --partitions 3 --replication-factor 1
```

### 4.3 Report questions

1. `KAFKA_ADVERTISED_LISTENERS` is `localhost:9092`. Explain what a client does with that value — specifically, what happens *after* the bootstrap connection succeeds. Then explain why this exact setting would break if your consumer ran inside another Docker container.
2. You created 3 partitions. If you key messages by `order_id`, what ordering guarantee do you get? What do you *not* get?
3. Your consumer will use a consumer group. What happens if you run two instances of your bridge with the same group id? With different group ids?

---

## 5. Part 2 — The bridge consumer (20 points)

This is the most interesting engineering in the assignment. Read the whole section before writing code.

### 5.1 What it does

`bridge_consumer.py` is a **long-running daemon**, not a script you invoke per batch. You start it once:

```bash
python bridge_consumer.py --group-id qsr-bridge --batch-size 50 --flush-seconds 10
```

and leave it running for the whole session. It loops forever:

1. **Poll** Kafka for records from `qsr.orders` and `qsr.payments`.
2. **Buffer** them in memory, per topic.
3. When the buffer reaches `--batch-size` records **or** `--flush-seconds` elapses, serialise it as **newline-delimited JSON**.
4. **Upload** that batch as a single new file to the matching Unity Catalog Volume path.
5. **Commit** the Kafka offsets.
6. Go back to step 1.

It only stops when you stop it (or when `--crash-after-upload` fires deliberately).

### 5.2 Uploading to a Volume

Use the Databricks SDK:

```python
from databricks.sdk import WorkspaceClient
import io, os

w = WorkspaceClient(
    host=os.environ["DATABRICKS_HOST"],      # https://dbc-xxxx.cloud.databricks.com
    token=os.environ["DATABRICKS_TOKEN"],
)

def upload_batch(volume_dir: str, filename: str, ndjson: str):
    w.files.upload(
        f"{volume_dir}/{filename}",
        io.BytesIO(ndjson.encode("utf-8")),
        overwrite=False,
    )
```

Credentials come from environment variables or a `.env` file. **`.env` goes in `.gitignore`; commit a `.env.example` with keys and no values.** A token in a submitted repository loses marks and should be revoked immediately.

### 5.3 File naming rules — get these right

Auto Loader treats every new file in the directory as new data. Three hard rules follow:

- **One upload = one complete file.** Never append to a file already in the Volume. Auto Loader may read it before you finish, and it will not re-read it afterwards.
- **Filenames must be unique and never reused.** Use `{topic}-{utc_timestamp}-{uuid4_hex[:8]}.json`. This is why `overwrite=False` above — an overwrite is a silent data-loss bug.
- **Separate directories per topic**, so each gets its own Auto Loader stream and schema:

```text
/Volumes/<catalog>/qsr_stream/landing/orders/
/Volumes/<catalog>/qsr_stream/landing/payments/
```

### 5.4 Offsets and the duplicate problem

Commit offsets **after** the upload succeeds, never before. Disable auto-commit (`enable.auto.commit=False`) and commit explicitly.

Now reason about the failure window. Between step 4 (upload succeeded) and step 5 (commit succeeded), your process can crash. On restart it resumes from the last committed offset and re-uploads records already in the Volume — as a *new file*, because filenames are unique.

**This is at-least-once delivery, and it produces duplicates downstream.** That is not a flaw in your implementation; it is the guarantee this architecture provides. You will deduplicate them in Part 5.

Add a `--crash-after-upload` flag that exits the process between upload and commit, so you can trigger this deliberately.

### 5.5 Required flags

| Flag | Purpose |
|---|---|
| `--batch-size N` | Flush after N buffered records (default 50) |
| `--flush-seconds S` | Flush at least every S seconds (default 10) |
| `--group-id G` | Consumer group id |
| `--from-beginning` | Start at earliest offset for a new group |
| `--crash-after-upload` | Simulate the crash in 5.4 |

Log every flush: topic, record count, filename, offsets committed.

### 5.6 Connectivity test — run before writing pipeline code

Do not build for six hours and then discover uploads fail.

```python
from databricks.sdk import WorkspaceClient
import io, os

w = WorkspaceClient(host=os.environ["DATABRICKS_HOST"],
                    token=os.environ["DATABRICKS_TOKEN"])

VOL = "/Volumes/<catalog>/qsr_stream/landing/orders"
w.files.upload(f"{VOL}/_connectivity_test.json",
               io.BytesIO(b'{"test": true}\n'), overwrite=True)
print("✅ upload OK")
print([f.path for f in w.files.list_directory_contents(VOL)])
```

Create the catalog, schema, and Volume in Databricks first (Section 7.1). Delete the test file before running Auto Loader, or it will appear in your bronze table.

### 5.7 Report questions

1. Draw the failure window in 5.4 as a timeline and mark exactly where a crash produces duplicates. Would committing offsets *before* uploading fix it? What would that break instead?
2. Run with `--crash-after-upload`, restart, and show the duplicate records landing in bronze. Include the two filenames and the repeated `order_id`s.
3. Your bridge buffers in memory. What happens to buffered records if the process is killed before a flush? Which guarantee does that violate, and how would you fix it?
4. What is the end-to-end latency floor of this architecture, given your flush settings? Where exactly does the delay come from?

---

## 6. Part 3 — The event simulator (15 points)

Write `producer.py`, publishing to `localhost:9092`.

### 6.1 Message schemas

**Topic `qsr.orders`** — key: `order_id`

```json
{
  "order_id": "ORD-20260818-000412",
  "store_id": "STR-03",
  "channel": "DELIVERY",
  "payment_method": "UPI",
  "order_ts": "2026-08-18T10:04:23.117+05:30",
  "currency": "INR",
  "order_total": 545.00,
  "items": [
    {"item_id": "ITM-07", "qty": 2, "unit_price": 180.00},
    {"item_id": "ITM-11", "qty": 1, "unit_price": 185.00}
  ]
}
```

**Topic `qsr.payments`** — key: `order_id`

```json
{
  "payment_id": "PAY-8ac91f",
  "order_id": "ORD-20260818-000412",
  "payment_ts": "2026-08-18T10:06:02.400+05:30",
  "status": "SUCCESS",
  "amount": 545.00,
  "method": "UPI"
}
```

`store_id` and `item_id` values must exist in `stores.csv` and `menu_items.csv`.

### 6.2 Required flags

| Flag | Behaviour |
|---|---|
| `--rate N` | ~N orders/second (default 2) |
| `--late-pct P` | P% carry an `order_ts` backdated **20–90 seconds** |
| `--very-late-pct P` | P% carry an `order_ts` backdated **10–30 minutes** |
| `--dup-pct P` | P% re-sent verbatim a few seconds later (application-level duplicates) |
| `--burst` | Every ~3 minutes, a 20-second spike at 10× base rate |
| `--unpaid-pct P` | P% of orders get **no** payment event |
| `--payment-delay-max S` | Payments lag their order by 5–S seconds (default 120) |

**Critical:** backdating applies to `order_ts` **in the payload**, never to when you send the message. Late arrival means an *old event time arriving now*. If you delay the send instead, you lose all experimental control and cannot complete Part 5.

Note you now have **two independent duplicate sources**: application-level (`--dup-pct`) and infrastructure-level (bridge replay). Part 5 asks you to distinguish them.

### 6.3 `inject.py`

Sends exactly one order with a precise event-time offset:

```bash
python inject.py --order-id ORD-TEST-001 --store STR-03 --ts-offset -45s
python inject.py --order-id ORD-TEST-002 --store STR-03 --ts-offset -25m
```

Required for the deterministic watermark experiments. A random simulator cannot prove a specific claim.

---

## 7. Part 4 — Bronze ingestion with Auto Loader (10 points)

Notebook: `01_bronze_ingest.py`

### 7.1 Setup

```sql
CREATE CATALOG IF NOT EXISTS qsr_stream_cat;
CREATE SCHEMA IF NOT EXISTS qsr_stream_cat.qsr_stream;
CREATE VOLUME IF NOT EXISTS qsr_stream_cat.qsr_stream.landing;
```

Create `landing/orders/`, `landing/payments/`, `landing/_checkpoints/`, `landing/_schemas/`.

Load `stores.csv` and `menu_items.csv` as static Delta tables `dim_stores` and `dim_menu_items` — needed in Part 8.

### 7.2 Ingest

Read each landing directory with Auto Loader (`format("cloudFiles")`, `cloudFiles.format=json`), using an **explicit schema**, not inference.

Retain: the parsed payload fields, `_metadata.file_name`, an ingestion timestamp, and — named distinctly — the payload's `order_ts` as `event_time`.

Write `bronze_orders` and `bronze_payments`.

Then build `silver_order_lines`: explode `items` to one row per line, compute `line_amount = qty * unit_price`, derive `order_date`, and carry `event_time` through.

### 7.3 Serverless trigger constraint

Serverless supports **only** `Trigger.AvailableNow()`. `Trigger.ProcessingTime(...)` and `Trigger.Continuous(...)` raise `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`.

To simulate continuous processing, re-trigger against a fixed checkpoint. State and watermark persist in the checkpoint, so each pass resumes exactly where the last stopped and the watermark advances across iterations as it would in an always-on query. **This helper is used by every remaining part** — write it once, in a shared cell:

```python
import time

def run_pass(streaming_df, checkpoint, table, label, output_mode="append"):
    q = (streaming_df.writeStream
         .outputMode(output_mode)
         .option("checkpointLocation", checkpoint)
         .trigger(availableNow=True)
         .toTable(table))
    q.awaitTermination()

    p = q.lastProgress or {}
    wm = (p.get("eventTime") or {}).get("watermark", "—")
    dropped = sum(op.get("numRowsDroppedByWatermark", 0) or 0
                  for op in (p.get("stateOperators") or []))
    print(f"[{label}] batch={p.get('batchId')} rows={p.get('numInputRows')} "
          f"watermark={wm} droppedByWatermark={dropped}")
    return p


def demo_loop(streaming_df, checkpoint, table, label, passes=10, sleep_s=15):
    for i in range(1, passes + 1):
        print(f"--- {label} pass {i}/{passes} ---")
        run_pass(streaming_df, checkpoint, table, label)
        time.sleep(sleep_s)
```

Keep `passes` bounded. Never write `while True` — see the quota warning.

### 7.4 Automate the Databricks side with a scheduled Job

`demo_loop()` above needs you to run a cell. That is fine for developing and for the
watermark experiments in Part 5, where you *want* tight control over when each pass
happens. It is not acceptable as the steady-state pipeline.

Move it to a **scheduled Job** so the Databricks side runs unattended too:

1. Create `notebooks/05_scheduled_ingest.py`. It should call `run_pass()` **once** for
   each stream — bronze orders, bronze payments, silver — and then exit. No loop, no
   `time.sleep`. The Job's schedule is the loop.
2. Workflows → Create job → task type Notebook → point it at that notebook →
   compute: serverless.
3. Schedule: every **2 minutes** (Trigger type: Scheduled, cron `0 */2 * * * ?`).
4. Set **max concurrent runs = 1**. Without this, a slow run and the next scheduled run
   collide on the same checkpoint and the second fails.

Each run picks up whatever the bridge has landed since the last one. State and watermark
live in the checkpoint, so the watermark advances across runs exactly as it would in a
continuously running query.

Now the whole chain is hands-off: producer → Kafka → bridge → Volume → Job → Delta.

> **Quota warning.** A job firing every 2 minutes runs 720 times a day. This will consume
> your Free Edition quota quickly. **Pause the schedule the moment you stop working**
> (Workflows → your job → Pause). Treat an unpaused overnight schedule as a guaranteed
> lost day.

For Part 5 you will want to pause the Job and go back to `demo_loop()` in a notebook,
because those experiments depend on controlling exactly when a pass runs relative to when
you inject a record. Say which mode you used where in your report.

### 7.5 Report questions

1. You now have several timestamps: `order_ts` from the payload, the bridge's upload time, and the Auto Loader ingestion time. Which is event time? Which are processing time? Give one concrete case from your own run where `order_ts` and ingestion time differ by more than a minute, with actual values.
2. What does the Auto Loader checkpoint track, and how does that differ from what a Kafka consumer group tracks? Both prevent reprocessing — what does each treat as a "unit of progress"?
3. Why is inferring a schema from a stream a bad idea in production?
4. Trace one order end to end and measure the wall-clock delay at each hop: produced → consumed by bridge → flushed and uploaded → picked up by the scheduled Job → visible in silver. Which hop dominates? What is the smallest end-to-end latency this architecture could achieve, and what would you have to change to get there?

---

## 8. Part 5 — Watermarking (20 points)

The core of the assignment. Every task needs **evidence**, not just code.

Late drops are **silent** — no error, no warning, the row simply never appears. `numRowsDroppedByWatermark` from `run_pass()` is your only proof. Screenshot it.

### Task 5.1 — Establish the watermark
Apply `withWatermark("event_time", "2 minutes")` to the silver stream. Run producer and bridge steadily with no anomalies for ~5 minutes.

**Report:** the watermark value at five consecutive passes. Does it ever move backwards? State the rule that guarantees your answer.

### Task 5.2 — Application-level duplicates
Run with `--dup-pct 10`, then `dropDuplicates(["order_id"])` after the watermark.

**Report:** counts with and without dedup over the same window. What bounds the memory `dropDuplicates` uses here? What would happen if you removed the watermark?

### Task 5.3 — Infrastructure-level duplicates
Trigger a bridge replay with `--crash-after-upload`, restart the bridge, and let Auto Loader pick up both files.

**Report:** show the duplicate reaching bronze. Did your `dropDuplicates` catch it? **Then explain:** are these duplicates distinguishable from the `--dup-pct` ones once they reach silver? If a colleague asked "are we double-counting because of the app or because of the pipeline?", how would you answer from the data?

### Task 5.4 — Duplicates outside the watermark
Inject an order, wait until the watermark passes `order_ts + 2 minutes`, then re-send the identical order.

**Report:** was it caught? Explain in terms of **dedup state expiry**, not "the watermark filtered it." Those are different mechanisms and the distinction is the point.

### Task 5.5 — Late but inside the watermark
`python inject.py --order-id ORD-LATE-IN --ts-offset -45s` with a 2-minute tolerance.

**Report:** did it reach silver? Which window did it land in? Was `numRowsDroppedByWatermark` still zero?

### Task 5.6 — Late and outside the watermark
`python inject.py --order-id ORD-LATE-OUT --ts-offset -25m`

**Report:** this one must be *proven*.
- Show `numRowsDroppedByWatermark` incrementing.
- Show a query confirming `ORD-LATE-OUT` is absent from the output.
- The record still exists in Kafka, in the Volume, and in `bronze_orders`. Describe how you would recover it. Would deleting the checkpoint and reprocessing bring it back? Justify.

### Task 5.7 — Setting the threshold
You used 2 minutes because you were told to. Now argue it.

**Report:** delivery orders from the partner app can lag up to 8 minutes during network issues. Ops wants dashboards fresh within 1 minute. Recommend a watermark and justify it against both constraints. **Additionally:** your bridge adds its own delay before records ever reach Spark. Does that change your answer? Explain precisely why or why not.

---

## 9. Part 6 — Tumbling windows (10 points)

Notebook: `02_windows.py`

Build `gold_store_revenue_tumbling`: revenue, order count, and units sold per **1-minute** tumbling window per store, in **append** output mode.

**Report:**
1. Your first pass probably produced an empty result even though rows were flowing in. Explain why. (Expected behaviour, not a bug.)
2. Show the exact pass where the watermark crossed a window's end and that window's row appeared.
3. Sum `revenue` across all windows for one store and compare against a direct sum over `silver_order_lines` for the same period. Do they match exactly? Should they?

---

## 10. Part 7 — Sliding windows (10 points)

Build `gold_store_revenue_sliding`: same metrics over a **2-minute window sliding every 30 seconds**.

Run with `--burst` for at least 10 minutes to capture several spikes.

**Report:**
1. Take one specific `order_id` and show every window it appears in. How many? Derive that number from the window length and slide interval.
2. Sum revenue across all sliding windows and compare with the tumbling total. Explain the difference. Is it a bug?
3. Find a burst that **straddles a tumbling window boundary**. Show tumbling splitting it across two windows and understating the peak, while sliding captures it whole. This is the strongest argument for sliding windows — make it with your own numbers.
4. Compare state store size between the two queries. What are you paying for the sliding view?

---

## 11. Part 8 — Stream–static join (10 points)

Notebook: `03_joins.py`

Enrich `silver_order_lines` with `dim_stores` (`city`, `region`) and `dim_menu_items` (`item_name`, `category`). Write `gold_enriched_order_lines`.

**Report:**
1. Does this join need a watermark? Does it create state? Explain.
2. **Run this experiment:** with the stream running, insert a ninth outlet `STR-09` into `dim_stores`, then have your producer emit orders for it. What happens to those orders in the join output?
3. Based on what you observed, give two ways to make the dimension refresh, with the trade-offs of each.

Question 2 is the point of this section. A stream–static join loads the static side once, at query start. Teams find this out in production, months later, having silently joined against stale data.

---

## 12. Part 9 — Stream–stream join (15 points)

Business requirement: **find orders never paid for within 15 minutes.**

Join the orders stream to the payments stream on `order_id`, producing `gold_unpaid_orders`.

Requirements:
- Watermark **both** sides.
- Include an event-time range condition: payment at or after the order, and no more than 15 minutes after.
- Choose the join type that surfaces orders with no matching payment.
- Run with `--unpaid-pct 15`.

**Report:**
1. Which join type did you use, and why does the side the streaming data sits on determine what is legal here?
2. Try an outer join **without** a watermark on one side. Paste the exact error. Explain in your own words what Spark cannot decide without it.
3. **Watermark vs time constraint.** Define each in one sentence. Then, for your specific watermark values and range condition, state how long records from each side are retained in state and how you derived those numbers.
4. An unpaid order can only be identified after the 15-minute window expires. What does that mean for how fast ops can act? If they wanted alerts within 5 minutes, what would you change and what would you sacrifice?
5. Run for ~10 minutes and record state store size over time. Is it growing without bound? If so, what is misconfigured?

---

## 13. Part 10 — Written analysis (15 points)

`REPORT.md` with answers to every **Report** question, plus:

**Architecture review.** You inverted the data flow because Free Edition blocks the direct path. Now design it properly for production: where does the broker live, how does Databricks reach it, how are credentials managed? Would you keep the bridge? Under what circumstances is push-from-edge the *right* choice rather than a workaround?

**Scaling.** What in your current setup breaks first at 10,000 orders/second? Be specific — name the component and the mechanism.

**Failure analysis.** Pick the hardest failure you hit. Describe the symptom, what you thought was wrong, what was actually wrong, and how you found it. Honest debugging accounts earn more marks than a clean narrative with nothing in it.

---

## 14. Deliverables

```text
qsr-streaming-assignment/
├── README.md                 How to run YOUR code
├── REPORT.md                 Part 10 - all analysis
├── docker-compose.yml
├── .env.example              Keys only, no values
├── .gitignore                Must contain .env
├── producer.py
├── inject.py
├── bridge_consumer.py
├── requirements.txt
├── data/
│   ├── stores.csv
│   └── menu_items.csv
├── notebooks/
│   ├── 00_setup.py
│   ├── 01_bronze_ingest.py
│   ├── 02_windows.py
│   ├── 03_joins.py
│   ├── 04_verify.sql
│   └── 05_scheduled_ingest.py   Single-pass notebook driven by the Job
└── evidence/
    ├── bridge_flush_log.png
    ├── scheduled_job_runs.png    Job run history showing unattended passes
    ├── duplicate_from_crash.png
    ├── watermark_progression.png
    ├── late_outside_dropped.png
    ├── tumbling_vs_sliding_burst.png
    └── stream_stream_join_output.png
```

Notebooks as `.py` source format (File → Export → Source file), not `.ipynb` or `.html`.

---

## 15. Grading

| Part | Marks | What earns them |
|---|---:|---|
| 1 — Kafka in Docker | 10 | Broker runs; topics created; listener and partitioning questions answered |
| 2 — Bridge consumer | 20 | Correct poll/batch/upload/commit order; unique filenames; no committed credentials; duplicate window analysed and demonstrated |
| 3 — Simulator | 15 | All flags; late records backdate `order_ts` not send time; `inject.py` gives deterministic control |
| 4 — Bronze + automation | 15 | Explicit schema; timestamps distinguished; checkpoint-vs-consumer-group question answered; **scheduled Job runs the pipeline unattended, with run history as evidence** |
| 5 — Watermarking | 20 | All seven tasks with **evidence**; `numRowsDroppedByWatermark` captured; 5.3, 5.4 and 5.6 explained precisely |
| 6 — Tumbling | 10 | Correct windows; empty-first-pass explained; reconciliation checked |
| 7 — Sliding | 10 | Multi-window membership derived; burst-straddling-boundary shown with real numbers |
| 8 — Stream–static | 10 | Join correct; stale-dimension experiment actually run |
| 9 — Stream–stream | 15 | Both watermarks + time constraint; error reproduced; state retention derived |
| 10 — Analysis | 15 | Substantive answers; honest failure account |
| **Total** | **140** | |

**Marks are lost for:** a pipeline that only advances when a human runs a cell; committed tokens or `.env`; screenshots without explanation; claims asserted but not demonstrated (especially Task 5.6); `overwrite=True` on batch uploads; unbounded loops in notebooks; absolute paths that only work on your machine.

---

## 16. Common pitfalls

**"My uploads fail with 403 / 404."** Check the Volume exists and the path starts `/Volumes/<catalog>/<schema>/<volume>/`. Confirm the token belongs to the right workspace and has not expired.

**"Auto Loader isn't picking up new files."** Confirm files are actually landing (`w.files.list_directory_contents`). Check the checkpoint isn't left over from an earlier schema. Confirm you are not overwriting the same filename on every flush.

**"`processingTime` trigger throws an error."** Expected on serverless. Use `run_pass()` from 7.3.

**"My late record wasn't dropped."** Watermarks advance only *between* micro-batches. If a fresh query reads everything in one pass, the watermark starts at zero and nothing is ever late. The late record must arrive in a **later** pass against the **same checkpoint**.

**"Windows never appear in append mode."** They appear only once the watermark passes the window end. Push it forward by feeding later event times.

**"Stream–stream join returns nothing."** Check your time constraint is satisfiable — if payments lag up to 120s but the constraint allows 60s, nothing matches.

**"My scheduled Job runs fail with a checkpoint error."** Two runs are overlapping. Set max concurrent runs = 1.

**"The Job runs but processes nothing."** Check the bridge is actually running and files are landing in the Volume. A Job with no new files does no work — that is correct behaviour, not a failure.

**"My compute got shut off."** Quota. See the warning in Section 3. Nothing to do but wait for the reset.

**"Duplicates everywhere and I didn't ask for them."** Check whether your bridge commits offsets before uploading, or crashed mid-flush. This is the failure mode from 5.4 — investigate it rather than working around it.

---

## 17. Stretch goals (bonus, up to 10 marks)

1. **Closer to exactly-once.** Make the bridge idempotent so a replay produces no new duplicates downstream. Hint: deterministic filenames derived from offset ranges. Explain what guarantee you now have and what you still cannot promise.
2. **Backpressure.** What happens if Kafka produces faster than your bridge uploads? Demonstrate it, then implement something sensible.
3. **Schema Registry + Avro.** Add Confluent Schema Registry to the compose file, switch payloads to Avro, and discuss schema evolution versus the JSON approach.
4. **`flatMapGroupsWithState`.** Detect three or more failed payment attempts for one order within 10 minutes. Explain why windows and joins cannot express this.
5. **Late-arrival recovery.** Build a side path that captures watermark-dropped records and reconciles them into a corrected daily table.
6. **Compare architectures.** If you can access a paid trial workspace with classic compute, implement the same bronze layer with `spark.readStream.format("kafka")` and compare: latency, delivery semantics, operational complexity, code volume. This is the highest-value stretch goal.

---

## Appendix A — Reference data

Reuse `stores.csv` and `menu_items.csv` from the Auto Loader assignment: eight stores across Indian cities with city and region, twelve menu items with category and price. Your producer must only emit `store_id` and `item_id` values present in those files — except in Part 8 question 2, where introducing `STR-09` is the entire point of the experiment.
