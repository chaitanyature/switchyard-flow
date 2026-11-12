"""Execution is deliberately outside the transaction that acquires a lease."""

import threading

from .engine import LostLease


def execute(engine, lease, handlers, ttl=30.0):
    stop = threading.Event()
    errors = []

    def renew():
        while not stop.wait(ttl / 3):
            try:
                engine.heartbeat(lease, ttl)
            except Exception as exc:
                errors.append(exc)
                return

    heartbeat = threading.Thread(target=renew, daemon=True)
    heartbeat.start()
    try:
        try:
            value = handlers[lease.kind](lease)
        except Exception as exc:
            engine.fail(lease, f"{type(exc).__name__}: {exc}")
        else:
            if errors:
                raise errors[0]
            engine.finish(lease, value)
    except LostLease:
        return False
    finally:
        stop.set()
        heartbeat.join()
    return True


def histogram(lease):
    buckets = {}
    for word in lease.payload.lower().split():
        buckets[word] = buckets.get(word, 0) + 1
    return buckets


def merge(lease):
    total = {}
    for histogram in lease.inputs.values():
        for word, count in histogram.items():
            total[word] = total.get(word, 0) + count
    return total


HANDLERS = {
    "histogram": histogram,
    "merge": merge,
    "top": lambda lease: sorted(
        merge(lease).items(), key=lambda item: (-item[1], item[0])
    )[: lease.payload],
}
