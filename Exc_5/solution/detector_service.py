"""The anomaly detector as a Kafka service. YOUR CODE (Part 2, Stage C2).

Contract with the maintenance team: every reading goes through model.score();
every reading with score >= model.threshold becomes ONE anomaly event,
model.anomaly_event(reading, score), as JSON, published to the topic
`anomalies`.

    python3 detector_service.py --name det-1
    python3 detector_service.py --name det-2          # one more instance
"""
import argparse
import json
import time

from confluent_kafka import Consumer, KafkaException, Producer

from model import BearingModel

ap = argparse.ArgumentParser()
ap.add_argument("--name", default="det-1", help="instance name (shows up in the scoreboard)")
ap.add_argument("--in-topic", default="readings", help="the topic the gateways write to")
ap.add_argument("--group", default="detector", help="all instances share ONE group: they split the partitions")
ap.add_argument("--out-topic", default="anomalies")
ap.add_argument("--bootstrap", default="localhost:9092")
args = ap.parse_args()

model = BearingModel(instance=args.name)
owner = {}        # machine_id -> partition it was last read from (to know what to forget on a rebalance)

consumer = Consumer({
    "bootstrap.servers": args.bootstrap,
    "group.id": args.group,
    "client.id": args.name,
    # A live detector only cares about NOW. "earliest" would make a new group chew through
    # the readings of all earlier plant runs (up to 7 days of retention): hours of useless
    # work, and the alerts for the current run would come late (R3). The model needs no
    # history: it learns each machine from its first 100 readings of the current run.
    # (Only applies while the group has no committed offset; afterwards it continues there.)
    "auto.offset.reset": "latest",
    # Commit the offset of a reading only AFTER it was scored (at-least-once): a crash
    # re-delivers readings instead of losing them.
    "enable.auto.commit": True,
    "enable.auto.offset.store": False,
    # On a rebalance only the partitions that really move are interrupted.
    "partition.assignment.strategy": "cooperative-sticky",
    "session.timeout.ms": 10000,
})
producer = Producer({
    "bootstrap.servers": args.bootstrap,
    "enable.idempotence": True,        # a retry cannot duplicate or reorder an anomaly event
    "acks": "all",
    "linger.ms": 5,                    # tiny batching delay, far below the 5 s budget
    "compression.type": "lz4",
})

failed = 0


def delivered(err, msg):
    global failed
    if err is not None:
        failed += 1
        print(f"[{args.name}] anomaly event not delivered: {err}", flush=True)


def forget_partitions(partitions):
    """The model keeps per-machine state (baseline, window, next_seq). If a partition leaves
    this instance, that state goes stale: when the partition comes back, the model would see
    a gap (counted as `missing`) or old readings (`out_of_order`), and a baseline learned
    long ago. So drop the state of the machines of every partition we lose; the model then
    starts that machine afresh (new warm-up) when we get it back."""
    gone = {p.partition for p in partitions}
    for mid in [m for m, p in owner.items() if p in gone]:
        model.forget(mid)
        del owner[mid]


def on_assign(c, partitions):
    print(f"[{args.name}] assigned:  {sorted(p.partition for p in partitions)}", flush=True)


def on_revoke(c, partitions):
    print(f"[{args.name}] revoked:   {sorted(p.partition for p in partitions)}", flush=True)
    try:
        c.commit(asynchronous=False)   # hand over exactly where we stopped
    except KafkaException:
        pass                           # nothing stored yet
    forget_partitions(partitions)


def on_lost(c, partitions):            # we were kicked out of the group (e.g. stalled): no commit possible
    print(f"[{args.name}] LOST:      {sorted(p.partition for p in partitions)}", flush=True)
    forget_partitions(partitions)


consumer.subscribe([args.in_topic], on_assign=on_assign, on_revoke=on_revoke, on_lost=on_lost)
print(f"[{args.name}] {args.in_topic} -> {args.out_topic} (group {args.group}, Ctrl+C to stop)", flush=True)

readings = anomalies = 0
report_at = time.time() + 5
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"[{args.name}] {msg.error()}")
            continue
        reading = json.loads(msg.value())
        owner[reading["machine_id"]] = msg.partition()
        score = model.score(reading)
        readings += 1
        if score is not None and score >= model.threshold:
            anomalies += 1
            event = model.anomaly_event(reading, score)
            while True:
                try:
                    # key = machine_id: all events of one machine stay in one partition, in order,
                    # so whoever reads `anomalies` (maintenance team, one technician per machine)
                    # sees the history of a machine in sequence.
                    producer.produce(args.out_topic, key=event["machine_id"].encode(),
                                     value=json.dumps(event).encode(), on_delivery=delivered)
                    break
                except BufferError:
                    producer.poll(0.5)
        producer.poll(0)
        consumer.store_offsets(msg)    # scored (and event handed to the producer): now it may be committed
        if time.time() >= report_at:
            print(f"[{args.name}] {readings:>9,} readings  {anomalies:>6,} anomalies  "
                  f"missing {model.missing}  out-of-order {model.out_of_order}  "
                  f"machines {len(model.machines())}", flush=True)
            report_at += 5
except KeyboardInterrupt:
    pass
finally:
    producer.flush(10)
    consumer.close()                   # commits the stored offsets and leaves the group cleanly
    print(f"[{args.name}] stopped after {readings:,} readings, {anomalies:,} anomalies"
          + (f", {failed} events NOT delivered" if failed else ""))
