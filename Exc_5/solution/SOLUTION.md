# Exercise 5: Solution

> **Honesty note.** The Stage A numbers below are derived from the code
> (`plant.py`, `baseline_detector.py`, `model.py`). Replace the "expected"
> values with what you measure on your machine, and paste your own scoreboard
> output in Stage C. The explanations are what is graded.

---

## Stage A: Today's system

### A1. 20 machines (200 readings/s)

**Are the missing readings in the table?** Yes. `count(*)` of the run equals
what the plant sent. Expected: `detector read + detector missing ≈ count(*)`.
The rows exist; the detector never read them.

**Why not?** The detector assumes *"ids become visible in increasing order"*.
That is false:

- A `BIGSERIAL` id is taken from a sequence **when the row is inserted**.
  Sequences are not transactional.
- A row becomes **visible to other sessions only at COMMIT**.
- `PostgresSink` sleeps `0–120 ms` *inside* the transaction, before the commit
  (the "gateway work + network"). Four gateways commit in an unpredictable order.

Timeline from the task:

| time | event |
|---|---|
| t0 | gateway 1 inserts rows 100–114, not committed (about 100 ms to go) |
| t0 | gateway 2 inserts rows 115–129 and commits at once |
| t0+ε | **detector polls**: sees only 115–129 (100–114 are invisible), processes them, remembers `last_id = 129` |
| t0+100 ms | gateway 1 commits: 100–114 are now visible |
| next poll | `WHERE id > 129`: **100–114 are skipped forever** |

The model then sees `seq` jump for those machines and increments `missing`.
The detector "keeps up" (backlog 0) because speed is not the problem; the
cursor logic is wrong. A high-water mark on a sequence is only safe if no
transaction is in flight below it.

Possible fixes in PostgreSQL: a per-machine cursor (`WHERE machine_id = … AND seq > last_seq[m]`,
`seq` has no gaps per machine), re-reading an overlap window and de-duplicating,
or only reading ids below the oldest in-flight transaction (`pg_snapshot_xmin`).
All add complexity and none removes the polling load.

### A2. Full plant (600 readings/s)

`model.score()` costs 3 ms, so **one detector process manages at most
1000/3 ≈ 333 readings/s**, less than 600/s. Expected while the plant runs:

- **backlog** grows linearly by about 600 − 333 ≈ **270 rows/s**; after 60 s about 16,000 rows.
- **delay** (age of the newest row being scored) grows by about 270/333 ≈ **0.8 s per second**; after 60 s roughly 45–50 s.
  The detector is permanently behind, and the lag grows without bound.
- **missing** does not explode the way it does in A1. When the detector is far behind the head
  of the table, all rows it reads are long committed, so the commit race disappears. It reappears
  only when the detector is at the head of the table (start, and when the backlog drains after the plant stops).
  Check your own output.
- After the plant stops, the detector only drains the backlog: about 16,000 / 333 ≈ 50 s.

Consequence: a fault that lasts 45 s would be reported after it is repaired.
R3 is badly missed, and one process can never serve 100,000 readings/s.

---

## Stage B: Decision record

**Context.** 60 machines × 10 Hz = 600 readings/s now; 2,000 × 50 Hz = 100,000 readings/s later.
The model is stateful per machine and costs 3 ms per reading.

### Options

| | Option 1: PostgreSQL, several pollers | Option 2: gateways → Kafka |
|---|---|---|
| **R1 scale** | At risk. One database must take 100k inserts/s (≈ 8.6 billion rows/day, index and WAL included) and serve every poller. Pollers are assigned to machines by hand; no automatic failover. | Meets. A partitioned log scales by adding partitions and brokers; 100k × ~150 B ≈ 15 MB/s is small for Kafka. The compute (the model) is the real cost, not Kafka. |
| **R2 complete and ordered** | At risk. The id/commit race of A1 remains. It can be solved with per-machine `seq` cursors, but that is on us, and a machine moving between pollers is hard. | Meets. Key = `machine_id` → one partition → append order = `seq` order; the idempotent producer keeps order across retries; one consumer per partition. |
| **R3 fresh (p95 < 5 s)** | At risk. Latency = poll interval + query time; more pollers = more load on the same table. | Meets. Push-style consumption, milliseconds; latency is determined by the model's 3 ms per reading (queueing if under-provisioned). |
| **R4 independent consumers** | **Fails.** Every new reader (archive, dashboard) adds polling queries to PostgreSQL, which is exactly what R4 forbids. | Meets. Every consumer group has its own offsets; reading deletes nothing and does not touch the others or PostgreSQL. A new group can replay history. |

**Own idea considered:** PostgreSQL `LISTEN/NOTIFY` or logical replication. This improves latency
but not R1/R4 (the load stays on one database), so it was dropped.

### Decision

**Option 2.** Gateways write every reading to the Kafka topic `readings`, key `machine_id`;
detector instances form one consumer group; anomalies go to the topic `anomalies`.
PostgreSQL remains the system of record for the machine registry (low volume, relational, transactional).

### Numbers

Capacity of one instance: 1 / 3 ms = **333 readings/s**.

| | readings/s | busy instance-seconds per second | instances at ≈ 70 % load | partitions |
|---|---|---|---|---|
| Lab | 600 | 600 × 0.003 = 1.8 | 2 is the minimum (90 % busy, no headroom) → **3** | **6** (room to grow to 6 instances) |
| Group | 100,000 | 100,000 × 0.003 = 300 | 300 is the minimum → **≈ 430** | **≈ 1,024** |

Why more partitions than instances: the number of partitions is the ceiling for parallelism, and it is
hard to change later (changing it changes which partition a key maps to, so ordering is broken during
the change). At 50 Hz, **one instance can serve only about 6 machines** (333 / 50 ≈ 6.7), and hashing
2,000 keys over ~430 partitions is uneven: some partitions get 10+ machines (> 500 readings/s) which one
consumer cannot score. With ~1,024 partitions each instance owns ~2–3 partitions and the skew averages out.
At this scale one would also make the model cheaper (batching/vectorising, GPU) instead of
buying 430 cores for `sleep`-level inference.

### Tipping point

A small office with one temperature reading per minute is ≈ 0.017 readings/s. A single polling
process and a single table handle that without effort, and Kafka would be pure operational overhead.
Polling a database stops being a good idea when one or more of these holds:

- **rate:** beyond roughly 10²–10³ events/s per consumer (here: already at 333/s per detector because of the model cost), or beyond what one database comfortably ingests while also serving reads (order of 10³–10⁴ inserts/s);
- **fan-out:** more than one independent consumer (R4), because each one multiplies the polling load;
- **ordering/completeness** requirements across concurrent writers (the race of A1);
- **latency** below the practical polling interval, or **replay** needs (new consumer, bug fix).

Roughly, that is 4–5 orders of magnitude above the office, and the lab (600/s, three consumers wanted) is already past it.

---

## Stage C: Build

### Commands

```bash
# 1. topics (BEFORE the plant: a topic created by the first write has 1 partition)
docker exec -it kafka kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic readings  --partitions 6 --replication-factor 1
docker exec -it kafka kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic anomalies --partitions 3 --replication-factor 1

# 2. three detector instances, one terminal each, started BEFORE the plant
python3 detector_service.py --name det-1
python3 detector_service.py --name det-2
python3 detector_service.py --name det-3

# 3. the plant (new terminal), then after ~3 minutes:
python3 plant.py --sink kafka
python3 scoreboard.py
```

For a clean repeat: stop everything, `python3 scoreboard.py --reset`, delete topics `readings` and
`anomalies` (`kafka-topics.sh --delete`), and recreate them.

### Decisions

| Decision | Choice | Reason |
|---|---|---|
| Key of `readings` | `machine_id` | Same key → same partition → same order (Part 1, step 2). The model needs every reading of a machine in `seq` order, so all of a machine's readings must reach one instance. Without a key, readings would be spread over instances: missing + out-of-order. |
| Partitions of `readings` | 6 | ≥ the 3 instances needed, with headroom; see numbers above. With 1 partition (auto-created) only one instance would work and would be overloaded (600/s > 333/s). |
| Producer settings | idempotence, `acks=all`, `linger.ms=10`, lz4, `murmur2_random` | Idempotence: a retry can neither duplicate nor reorder a reading. Batching: the plant must not wait for each message (`flush()` per reading would make it fall behind). |
| Consumer group | one group `detector` for all instances | Within a group each partition goes to exactly one instance (they share the work). A second group would score everything twice. Archive and dashboard get their own groups (R4). |
| `auto.offset.reset` | `latest` | A live detector wants the present. `earliest` would replay old runs (up to 7 days of retention) before reaching the current one: late alerts and wasted compute. The model needs no history, it learns each machine from the first 100 readings of the current run. |
| Commit | offset stored after scoring, auto-commit every 5 s, synchronous commit on revoke | At-least-once: a crash re-delivers instead of losing readings. |
| Rebalance | `cooperative-sticky`, and `model.forget()` for the machines of every revoked/lost partition | See the hint: if a partition leaves an instance and later returns, the model would still remember an old `next_seq` and baseline → it reports `missing`/`out-of-order` and scores against a stale window. Forgetting makes the machine restart cleanly (new warm-up). |
| Key of `anomalies` | `machine_id` | Events of one machine stay in order in one partition (a technician following one machine sees its history in sequence). The topic is created with 3 partitions explicitly. |

### Limitations (worth saying in the presentation)

- **Delivery is at-least-once**, so after a crash an anomaly could appear twice; consumers should de-duplicate by `anomaly_id` (the scoreboard counts these).
- **State is lost on a rebalance.** After a move, the machine needs 100 readings of warm-up plus a 50-reading window (≈ 15 s at 10 Hz) before it can alert again, and if the machine is already faulty at that moment, the new baseline is learned from faulty data. A real system would checkpoint the model state (state store with a changelog topic, or externalise it) so the new owner can restore it.
- Scale-out is capped by the partition count (6 in the lab). Increasing it later changes the key → partition mapping.
- A single broker with replication factor 1 is fine for the lab only. Production needs ≥ 3 brokers, RF 3, `min.insync.replicas=2`.

### Scoreboard output

```
<paste the output of python3 scoreboard.py here>
```

**If something fails, the usual causes are:**

| Symptom | Cause |
|---|---|
| R2 FAIL, `missing` and `out-of-order` > 0 | no key (readings spread over instances), or the model state was not forgotten when a partition moved (instances joined/left during the run) |
| R3 FAIL, lag grows in `kafka-consumer-groups.sh --describe` | too few partitions or instances (1 partition → one instance at 180 % load), or an instance's partitions hold more machines than it can score |
| plant: `WARNING: behind schedule` | the sink waits per message (`flush()` per reading) |
| detection delay huge, many events from old runs | `earliest`, or a group with an old committed offset: reset with `--reset-offsets --to-latest` |
