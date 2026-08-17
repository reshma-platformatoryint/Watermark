# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Watermarking, Tumbling & Sliding Windows — Structured Streaming Lab
# MAGIC
# MAGIC Combines the deterministic controlled-batch approach (predictable counts,
# MAGIC labelled scenarios, without-vs-with contrast) with append-mode incremental
# MAGIC re-runs, so that late-data dropping is **actually observable** rather than
# MAGIC just labelled.
# MAGIC
# MAGIC **Structure**
# MAGIC - **Act 1** — Concept lab on files. Fully deterministic, no Kafka, no network risk.
# MAGIC   - 1A: No watermark → unbounded state, everything counted
# MAGIC   - 1B: Watermark + dropDuplicates → dedup works
# MAGIC   - 1C: Watermark + **tumbling** window, append mode → windows finalise, late row **dropped**
# MAGIC   - 1D: Watermark + **sliding** window on identical data → same event in two windows
# MAGIC - **Act 2** — Same windowing logic against a live Kafka topic (optional)
# MAGIC
# MAGIC **Key design choice:** every stateful query is re-run against the *same*
# MAGIC checkpoint after each new batch arrives. That's what lets the watermark
# MAGIC advance between runs — which is the only way a "late" row is ever late.
# MAGIC
# MAGIC > Serverless / Free Edition only supports `Trigger.AvailableNow()` —
# MAGIC > `processingTime` and `continuous` raise `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`.
# MAGIC > The run-batch-then-re-trigger pattern below is the supported equivalent.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS watermark_demo_cat;
# MAGIC CREATE SCHEMA IF NOT EXISTS watermark_demo_cat.watermark_demo;
# MAGIC CREATE VOLUME IF NOT EXISTS watermark_demo_cat.watermark_demo.source_vol;

# COMMAND ----------

BASE = "/Volumes/watermark_demo_cat/watermark_demo/source_vol"

WINDOW_DATA = f"{BASE}/orders_window"     # dataset for 1A / 1C / 1D
DEDUP_DATA = f"{BASE}/orders_dedup"       # dataset for 1B

CP = {
    "nowm":     f"{BASE}/cp_nowm",
    "dedup":    f"{BASE}/cp_dedup",
    "tumbling": f"{BASE}/cp_tumbling",
    "sliding":  f"{BASE}/cp_sliding",
    "kafka_t":  f"{BASE}/cp_kafka_tumbling",
    "kafka_s":  f"{BASE}/cp_kafka_sliding",
}
SCHEMA_LOC = {k: f"{BASE}/schema_{k}" for k in CP}

CAT = "watermark_demo_cat.watermark_demo"
T_TUMBLING = f"{CAT}.out_tumbling"
T_SLIDING = f"{CAT}.out_sliding"

WATERMARK_DELAY = "5 minutes"
WINDOW_LEN = "10 minutes"
SLIDE = "5 minutes"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Full reset
# MAGIC Run this whenever you want to re-demo from scratch. Everything downstream
# MAGIC assumes a clean slate.

# COMMAND ----------

for q in spark.streams.active:
    q.stop()

for p in [WINDOW_DATA, DEDUP_DATA] + list(CP.values()) + list(SCHEMA_LOC.values()):
    dbutils.fs.rm(p, True)

spark.sql(f"DROP TABLE IF EXISTS {T_TUMBLING}")
spark.sql(f"DROP TABLE IF EXISTS {T_SLIDING}")

dbutils.fs.mkdirs(WINDOW_DATA)
dbutils.fs.mkdirs(DEDUP_DATA)
print("Reset complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Helpers
# MAGIC `run_batch()` is the heart of the demo — it runs one `AvailableNow` pass
# MAGIC and prints the three numbers that tell the whole watermark story:
# MAGIC **rows in**, **current watermark**, and **rows dropped by watermark**.

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, window, count, sum as _sum, lit
from pyspark.sql.types import StructType, StringType, IntegerType


ORDER_SCHEMA = (
    StructType()
    .add("order_id", IntegerType())
    .add("event_time", StringType())
    .add("amount", IntegerType())
    .add("scenario", StringType())
)


def write_batch(rows, path, label):
    """Append a hand-crafted batch of events as a JSON file."""
    (spark.createDataFrame(rows, ["order_id", "event_time", "amount", "scenario"])
        .write.mode("append").json(path))
    print(f"📦 {label}: wrote {len(rows)} row(s)")
    for r in rows:
        print(f"     id={r[0]:<3} event_time={r[1]}  [{r[3]}]")


def run_batch(streaming_df, checkpoint, label, sink="table", table=None, query_name=None,
              output_mode="append"):
    """Run exactly one AvailableNow pass and report watermark diagnostics."""
    writer = (streaming_df.writeStream
              .outputMode(output_mode)
              .option("checkpointLocation", checkpoint)
              .trigger(availableNow=True))

    if sink == "table":
        q = writer.format("delta").toTable(table)
    else:
        q = writer.format("memory").queryName(query_name).start()

    q.awaitTermination()
    p = q.lastProgress or {}

    rows_in = p.get("numInputRows", 0)
    wm = (p.get("eventTime") or {}).get("watermark", "—")
    dropped = 0
    for op in p.get("stateOperators", []) or []:
        dropped += op.get("numRowsDroppedByWatermark", 0) or 0

    print(f"🔁 [{label}] batch={p.get('batchId')}  rowsIn={rows_in}  "
          f"watermark={wm}  droppedByWatermark={dropped}")
    if dropped:
        print(f"   ⚠️  {dropped} row(s) arrived older than the watermark and were discarded.")
    return p


def read_files(path, schema_loc, watermark=None):
    df = (spark.readStream.format("cloudFiles")
          .option("cloudFiles.format", "json")
          .option("cloudFiles.schemaLocation", schema_loc)
          .schema(ORDER_SCHEMA)
          .load(path)
          .withColumn("event_time", to_timestamp("event_time")))
    return df.withWatermark("event_time", watermark) if watermark else df

# COMMAND ----------

# MAGIC %md
# MAGIC # Act 1 — Concept lab (deterministic, file-based)
# MAGIC
# MAGIC ## The dataset
# MAGIC Three batches, delivered one at a time so the watermark can advance between them.
# MAGIC
# MAGIC | Batch | Rows | Purpose |
# MAGIC |---|---|---|
# MAGIC | 1 | 5 orders, 10:00 → 10:04 | Establish a baseline. Max event time 10:04 → watermark 09:59 |
# MAGIC | 2 | 1 order @ 10:20 | **Pushes the watermark forward** to 10:15. Closes the 10:00 windows |
# MAGIC | 3 | 1 order @ 10:18 (late, *inside*)<br>1 order @ 09:40 (late, *outside*) | The payoff — one accepted, one dropped, same batch |
# MAGIC
# MAGIC With `WATERMARK_DELAY = 5 minutes`, watermark = *(max event time seen)* − 5 min.

# COMMAND ----------

batch1 = [
    (1, "2025-04-01 10:00:00", 100, "normal"),
    (2, "2025-04-01 10:01:00", 200, "normal"),
    (3, "2025-04-01 10:02:00", 300, "normal"),
    (4, "2025-04-01 10:03:00", 400, "normal"),
    (5, "2025-04-01 10:04:00", 500, "normal"),
]

batch2 = [
    (6, "2025-04-01 10:20:00", 600, "pushes_watermark_forward"),
]

batch3 = [
    (7, "2025-04-01 10:18:00", 700, "late_INSIDE_watermark_accepted"),
    (8, "2025-04-01 09:40:00", 800, "late_OUTSIDE_watermark_dropped"),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1A — No watermark: state grows forever
# MAGIC Complete mode, no watermark, no window. Spark keeps every row's state
# MAGIC indefinitely because it has no basis for deciding anything is "done."
# MAGIC Every row that ever arrives — however old — is counted.

# COMMAND ----------

write_batch(batch1, WINDOW_DATA, "Batch 1")

nowm_df = read_files(WINDOW_DATA, SCHEMA_LOC["nowm"]).groupBy().count()
run_batch(nowm_df, CP["nowm"], "NO-WATERMARK", sink="memory",
          query_name="no_wm", output_mode="complete")

display(spark.sql("SELECT * FROM no_wm"))

# COMMAND ----------

write_batch(batch2, WINDOW_DATA, "Batch 2")
run_batch(nowm_df, CP["nowm"], "NO-WATERMARK", sink="memory",
          query_name="no_wm", output_mode="complete")
display(spark.sql("SELECT * FROM no_wm"))

# COMMAND ----------

write_batch(batch3, WINDOW_DATA, "Batch 3")
run_batch(nowm_df, CP["nowm"], "NO-WATERMARK", sink="memory",
          query_name="no_wm", output_mode="complete")
display(spark.sql("SELECT * FROM no_wm"))



# COMMAND ----------

# MAGIC %md
# MAGIC **Expected: 5 → 6 → 8.** The 09:40 row from Batch 3 — nearly 40 minutes
# MAGIC stale — is counted like any other. Nothing is ever considered too late.
# MAGIC In a long-running job this is exactly how state blows up.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1B — Watermark + deduplication
# MAGIC A separate dataset with deliberate duplicates. `dropDuplicates(["order_id"])`
# MAGIC needs a watermark so Spark knows when it can forget an id it has already seen.

# COMMAND ----------

dedup_batch1 = [
    (1, "2025-04-01 10:00:00", 100, "normal"),
    (2, "2025-04-01 10:01:00", 200, "normal"),
    (2, "2025-04-01 10:01:00", 200, "DUPLICATE_of_id_2"),
    (3, "2025-04-01 10:02:00", 300, "normal"),
]
dedup_batch2 = [
    (3, "2025-04-01 10:02:00", 300, "DUPLICATE_arriving_later"),
    (4, "2025-04-01 10:06:00", 400, "normal"),
]

write_batch(dedup_batch1, DEDUP_DATA, "Dedup batch 1")

dedup_df = (read_files(DEDUP_DATA, SCHEMA_LOC["dedup"], watermark=WATERMARK_DELAY)
            .dropDuplicates(["order_id"])
            .groupBy().count())

run_batch(dedup_df, CP["dedup"], "DEDUP", sink="memory",
          query_name="dedup_out", output_mode="complete")
display(spark.sql("SELECT * FROM dedup_out"))

# COMMAND ----------

write_batch(dedup_batch2, DEDUP_DATA, "Dedup batch 2")
run_batch(dedup_df, CP["dedup"], "DEDUP", sink="memory",
          query_name="dedup_out", output_mode="complete")
display(spark.sql("SELECT * FROM dedup_out"))



# COMMAND ----------

# MAGIC %md
# MAGIC **Expected: 3 → 4.** Six physical rows in, four distinct orders out.
# MAGIC The duplicate that arrived in a *later* batch is still caught, because
# MAGIC the watermark hasn't yet expired id 3 from the dedup state store.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1C — Tumbling window, append mode
# MAGIC `window(event_time, "10 minutes")` — fixed, non-overlapping. Every event
# MAGIC lands in exactly one window.
# MAGIC
# MAGIC **Append mode is the point here.** A window row is written only once the
# MAGIC watermark has passed the window's end — i.e. once Spark is confident the
# MAGIC window is final. This is why Run 1 produces *no output at all*.
# MAGIC
# MAGIC The data files already contain all three batches (written above), so this
# MAGIC query needs its own checkpoint and its own batch-by-batch narrative. To
# MAGIC replay it cleanly, we re-create the source directory and re-feed the batches.

# COMMAND ----------

# fresh source dir so this query sees the batches arrive one at a time
WINDOW_DATA2 = f"{BASE}/orders_window_seq"
dbutils.fs.rm(WINDOW_DATA2, True)
dbutils.fs.mkdirs(WINDOW_DATA2)

tumbling_df = (read_files(WINDOW_DATA2, SCHEMA_LOC["tumbling"], watermark=WATERMARK_DELAY)
               .groupBy(window(col("event_time"), WINDOW_LEN))
               .agg(count("*").alias("order_count"), _sum("amount").alias("total_amount")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run 1 — Batch 1 arrives (max event 10:04 → watermark 09:59)

# COMMAND ----------

write_batch(batch1, WINDOW_DATA2, "Batch 1")
run_batch(tumbling_df, CP["tumbling"], "TUMBLING", table=T_TUMBLING)
display(spark.sql(f"SELECT window.start, window.end, order_count, total_amount FROM {T_TUMBLING} ORDER BY window.start"))



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **Expected: empty table.** The `[10:00, 10:10)` window hasn't closed —
# MAGIC watermark is only at 09:59, so more events could still arrive for it.
# MAGIC This "nothing happened yet" moment is the concept, not a bug.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run 2 — Batch 2 arrives (event @ 10:20 → watermark jumps to 10:15)

# COMMAND ----------

write_batch(batch2, WINDOW_DATA2, "Batch 2")
run_batch(tumbling_df, CP["tumbling"], "TUMBLING", table=T_TUMBLING)
display(spark.sql(f"SELECT window.start, window.end, order_count, total_amount FROM {T_TUMBLING} ORDER BY window.start"))



# COMMAND ----------

# MAGIC %md
# MAGIC **Expected: one row — `[10:00, 10:10)`, count 5, total 1500.**
# MAGIC A single event at 10:20 pushed the watermark past 10:10, which finalised
# MAGIC and emitted the earlier window. The `[10:20, 10:30)` window is still open.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run 3 — Batch 3: one late row accepted, one dropped

# COMMAND ----------

write_batch(batch3, WINDOW_DATA2, "Batch 3")
p = run_batch(tumbling_df, CP["tumbling"], "TUMBLING", table=T_TUMBLING)
display(spark.sql(f"SELECT window.start, window.end, order_count, total_amount FROM {T_TUMBLING} ORDER BY window.start"))



# COMMAND ----------

# MAGIC %md
# MAGIC **Expected: `droppedByWatermark = 1`.**
# MAGIC - id 7 @ **10:18** is later than the 10:15 watermark → accepted, joins `[10:10, 10:20)`
# MAGIC - id 8 @ **09:40** is far older than the watermark → **silently discarded**
# MAGIC
# MAGIC That counter is the smoking gun. Note it is *silent* by default — no error,
# MAGIC no warning, the row just never appears. Which is precisely why you monitor it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1D — Sliding window, identical data
# MAGIC `window(event_time, "10 minutes", "5 minutes")` — same 10-minute length,
# MAGIC but a new window opens every 5 minutes, so windows **overlap by half**.
# MAGIC
# MAGIC Consequence: **each event is counted in two windows, not one.** Same input,
# MAGIC roughly double the output rows. Useful for rolling metrics ("orders in the
# MAGIC last 10 minutes, refreshed every 5") where tumbling would be too coarse.

# COMMAND ----------

WINDOW_DATA3 = f"{BASE}/orders_window_seq_sliding"
dbutils.fs.rm(WINDOW_DATA3, True)
dbutils.fs.mkdirs(WINDOW_DATA3)

sliding_df = (read_files(WINDOW_DATA3, SCHEMA_LOC["sliding"], watermark=WATERMARK_DELAY)
              .groupBy(window(col("event_time"), WINDOW_LEN, SLIDE))
              .agg(count("*").alias("order_count"), _sum("amount").alias("total_amount")))

# Additional batches to demonstrate sliding window overlaps
batch4 = [
    (9, "2025-04-01 10:05:00", 900, "overlaps_2_windows"),
    (10, "2025-04-01 10:08:00", 1000, "overlaps_2_windows"),
]

batch5 = [
    (11, "2025-04-01 10:12:00", 1100, "overlaps_2_windows"),
    (12, "2025-04-01 10:14:00", 1200, "overlaps_2_windows"),
]

batch6 = [
    (13, "2025-04-01 10:25:00", 1300, "pushes_watermark_to_10:20"),
]

def explain_sliding_windows(event_time_str):
    """Show which sliding windows an event belongs to."""
    from datetime import datetime, timedelta
    event_time = datetime.strptime(event_time_str, "%Y-%m-%d %H:%M:%S")
    # For 10-min windows with 5-min slide, each event belongs to 2 windows
    # Window starts: ..., 09:50, 09:55, 10:00, 10:05, 10:10, 10:15, 10:20, ...
    windows = []
    for offset in [0, 5]:  # event can be in current and previous window
        minute = event_time.minute
        # Find the window start (either :00, :05, :10, :15, :20, etc.)
        window_start_min = (minute // 5) * 5 - offset
        window_start = event_time.replace(minute=0, second=0) + timedelta(minutes=window_start_min)
        window_end = window_start + timedelta(minutes=10)
        if window_start <= event_time < window_end:
            windows.append(f"[{window_start.strftime('%H:%M')}, {window_end.strftime('%H:%M')})")
    return windows

print("\n" + "="*70)
print("SLIDING WINDOW DEMO: 10-minute windows, 5-minute slide")
print("Each event appears in 2 OVERLAPPING windows")
print("="*70 + "\n")

for label, b in [("Batch 1", batch1), ("Batch 2", batch2), ("Batch 3", batch3), 
                 ("Batch 4", batch4), ("Batch 5", batch5), ("Batch 6", batch6)]:
    write_batch(b, WINDOW_DATA3, label)
    
    # Show which windows each event belongs to
    print("  → Events in this batch belong to these OVERLAPPING windows:")
    for row in b:
        windows = explain_sliding_windows(row[1])
        print(f"     id={row[0]:<3} @ {row[1].split()[1]:8} → windows: {', '.join(windows)}")
    
    run_batch(sliding_df, CP["sliding"], "SLIDING", table=T_SLIDING)
    print()

print("\n" + "="*70)
print("FINAL RESULT: Notice how events appear in MULTIPLE windows")
print("="*70)
display(spark.sql(f"SELECT window.start, window.end, order_count, total_amount FROM {T_SLIDING} ORDER BY window.start"))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Insights from Sliding Window Demo
# MAGIC
# MAGIC **Why Sliding Windows?**
# MAGIC Sliding windows give you **overlapping time buckets** — perfect for metrics like "orders in the last 10 minutes, refreshed every 5 minutes."
# MAGIC
# MAGIC **The Math:**
# MAGIC * Window length: 10 minutes
# MAGIC * Slide interval: 5 minutes  
# MAGIC * Result: **Each event appears in 2 windows** (current + previous)
# MAGIC
# MAGIC **Example from the demo:**
# MAGIC * Event at `10:05` belongs to:
# MAGIC   * `[10:00, 10:10)` ✓
# MAGIC   * `[10:05, 10:15)` ✓
# MAGIC
# MAGIC * Event at `10:12` belongs to:
# MAGIC   * `[10:05, 10:15)` ✓
# MAGIC   * `[10:10, 10:20)` ✓
# MAGIC
# MAGIC **Dropped Rows Counter:**
# MAGIC * Tumbling: 1 physical event = 1 window assignment → `droppedByWatermark = 1`
# MAGIC * Sliding: 1 physical event = 2 window assignments → `droppedByWatermark = 2`
# MAGIC
# MAGIC **Use Cases:**
# MAGIC * Real-time dashboards showing "last N minutes" metrics
# MAGIC * Detecting spikes/patterns that tumbling windows might miss
# MAGIC * Smoother trend analysis with overlapping aggregations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Side-by-side: tumbling vs sliding
# MAGIC The money shot for the demo — one query, the whole distinction.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'TUMBLING' AS window_type, window.start, window.end, order_count, total_amount
# MAGIC FROM watermark_demo_cat.watermark_demo.out_tumbling
# MAGIC UNION ALL
# MAGIC SELECT 'SLIDING'  AS window_type, window.start, window.end, order_count, total_amount
# MAGIC FROM watermark_demo_cat.watermark_demo.out_sliding
# MAGIC ORDER BY window_type, start;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Act 1 recap
# MAGIC
# MAGIC | Concept | What you saw |
# MAGIC |---|---|
# MAGIC | No watermark | Count climbs forever; 40-minute-stale row still counted; state never freed |
# MAGIC | Watermark + dedup | Duplicates removed even across batches, while ids remain in state |
# MAGIC | Watermark threshold | `max(event_time) − 5 min`, recomputed each trigger, only moves forward |
# MAGIC | Append mode | Windows emit *only* after the watermark passes their end — hence the empty first run |
# MAGIC | Late inside threshold | Accepted, folded into its window |
# MAGIC | Late outside threshold | Dropped silently; visible only via `numRowsDroppedByWatermark` |
# MAGIC | Tumbling | One event → one window |
# MAGIC | Sliding | One event → multiple overlapping windows |

# COMMAND ----------

# MAGIC %md
# MAGIC # Act 2 — Same logic on a live Kafka topic (optional)
# MAGIC
# MAGIC Identical windowing code, real source. Run the connectivity check first —
# MAGIC Free Edition restricts outbound access to an allowlist of trusted domains,
# MAGIC so an external Kafka broker may simply be unreachable. If it is, Act 1
# MAGIC stands on its own perfectly well as the teaching artifact.

# COMMAND ----------

BOOTSTRAP_SERVERS = "pkc-921jm.us-east-2.aws.confluent.cloud:9092"
TOPIC = "sample_data_orders"
# Prefer secrets over hardcoding:
# API_KEY = dbutils.secrets.get("confluent", "api-key")
# API_SECRET = dbutils.secrets.get("confluent", "api-secret")
API_KEY = "VDIVN76P5MMCDMBC"
API_SECRET = "cfltYaDtCM8XZpKqqdi/KtuqK9FtKJ/NCOh168x7j9DE8D4CA92YbTZRr011wx2w"

import socket
_h, _p = BOOTSTRAP_SERVERS.split(":")
try:
    with socket.create_connection((_h, int(_p)), timeout=8):
        print(f"✅ Reached {_h}:{_p}")
except Exception as e:
    print(f"❌ Cannot reach {_h}:{_p} — {e}")
    print("   Likely the Free Edition outbound allowlist. Act 1 covers the concepts without Kafka.")

# COMMAND ----------

from pyspark.sql.functions import from_json
from pyspark.sql.types import DoubleType, LongType

kafka_schema = (StructType()
                .add("order_id", StringType())
                .add("customer", StringType())
                .add("amount", DoubleType())
                .add("order_ts", LongType()))

kafka_parsed = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option(
        "kafka.sasl.jaas.config",
        f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{API_KEY}" password="{API_SECRET}";')
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .load()
    .select(from_json(col("value").cast("string"), kafka_schema).alias("d")).select("d.*")
    .withColumn("event_time", to_timestamp(col("order_ts") / 1000))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Simulating a continuous stream
# MAGIC Re-trigger `AvailableNow` on a loop against a fixed checkpoint. State and
# MAGIC watermark persist in the checkpoint, so each pass resumes exactly where the
# MAGIC last one stopped — the watermark climbs across iterations just as it would
# MAGIC in an always-on query.
# MAGIC
# MAGIC Keep your producer running in a terminal while this loop executes.

# COMMAND ----------

import time

K_TUMBLING = f"{CAT}.kafka_tumbling"
K_SLIDING = f"{CAT}.kafka_sliding"

k_tumbling = (kafka_parsed.withWatermark("event_time", "30 seconds")
              .groupBy(window(col("event_time"), "1 minute"), col("customer"))
              .agg(count("*").alias("order_count"), _sum("amount").alias("total_amount")))

k_sliding = (kafka_parsed.withWatermark("event_time", "30 seconds")
             .groupBy(window(col("event_time"), "1 minute", "30 seconds"), col("customer"))
             .agg(count("*").alias("order_count"), _sum("amount").alias("total_amount")))

for i in range(1, 7):
    print(f"\n─── pass {i}/6 ───")
    run_batch(k_tumbling, CP["kafka_t"], "KAFKA-TUMBLING", table=K_TUMBLING)
    run_batch(k_sliding, CP["kafka_s"], "KAFKA-SLIDING", table=K_SLIDING)
    time.sleep(15)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Beyond the notebook
# MAGIC For something unattended rather than you re-triggering by hand: wrap the
# MAGIC same `AvailableNow` queries in a **Databricks Job on a schedule** (each run
# MAGIC = one incremental pass), or use **Lakeflow Spark Declarative Pipelines in
# MAGIC continuous mode**. Both are the supported serverless routes to an always-on
# MAGIC stream — with minute-level rather than sub-second latency.

# COMMAND ----------

for q in spark.streams.active:
    q.stop()
print("All streams stopped.")