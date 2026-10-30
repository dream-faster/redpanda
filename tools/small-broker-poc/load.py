"""Produce to and consume from topics t0000..t(N-1) at once.

usage: load.py N SECONDS MB_PER_SEC [CONSUMERS]
"""

import os
import sys
import threading
import time

from confluent_kafka import Consumer, Producer


def main() -> None:
    n, secs, mbps = int(sys.argv[1]), int(sys.argv[2]), float(sys.argv[3])
    nconsumers = int(sys.argv[4]) if len(sys.argv) > 4 else 2
    bootstrap = os.environ.get("BROKERS", "rp0:9092,rp1:9092,rp2:9092")
    topics = [f"t{i:04d}" for i in range(n)]
    stop = time.time() + secs
    consumed = [0]

    def consume() -> None:
        c = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": "load",
                "auto.offset.reset": "earliest",
                "fetch.wait.max.ms": 100,
            }
        )
        c.subscribe(topics)
        while time.time() < stop + 5:
            for m in c.consume(1000, 0.5):
                if not m.error():
                    consumed[0] += 1
        c.close()

    threads = [threading.Thread(target=consume) for _ in range(nconsumers)]
    for t in threads:
        t.start()
    p = Producer(
        {
            "bootstrap.servers": bootstrap,
            "acks": "all",
            "linger.ms": 5,
            "queue.buffering.max.messages": 1_000_000,
        }
    )
    val = b"x" * 1000
    sent, t0 = 0, time.time()
    while time.time() < stop:
        target = (time.time() - t0) * mbps * 1000
        while sent < target:
            try:
                p.produce(topics[sent % n], val)
                sent += 1
            except BufferError:
                p.poll(0.05)
        p.poll(0.01)
    p.flush(30)
    for t in threads:
        t.join()
    print(f"produced={sent} consumed={consumed[0]} rate={sent / secs / 1000:.1f}MB/s")


if __name__ == "__main__":
    main()
