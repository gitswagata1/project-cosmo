"""Blockchain Anchoring Daemon — batches interval hash fingerprints and commits
them periodically to an immutable append-only ledger.

Architecture:
  - Consumes fingerprints from the extraction and mixing-pool streams.
  - Batches fingerprints into Merkle-tree-structured commitments.
  - Writes commit records to a local append-only ledger (JSON Lines).
  - In production, the commit hash would be anchored to a public blockchain
    (Ethereum, Bitcoin via Openimestamps, or a permissioned Hyperledger).

Each ledger entry contains:
  - batch_id: monotonic batch counter
  - merkle_root: SHA3-256 root of the fingerprint batch
  - fingerprints: list of SHA3-512 hex fingerprints in the batch
  - prev_hash: hash of the previous ledger entry (chain integrity)
  - timestamp: wall-clock time of the commit
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import secrets
import signal
import time
from pathlib import Path

import redis.asyncio as aioredis
from prometheus_client import Counter, Gauge, start_http_server

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger("cosmo.anchoring")

REDIS_URL = os.environ["REDIS_URL"]
EXTRACTION_STREAM = os.getenv("EXTRACTION_STREAM", "cosmo:extracted_entropy")
CONDITIONED_STREAM = os.getenv("CONDITIONED_STREAM", "cosmo:conditioned_entropy")
CONSUMER_GROUP = "anchoring-daemon"
CONSUMER_NAME = os.getenv("HOSTNAME", f"anchor-{secrets.token_hex(4)}")
METRICS_PORT = int(os.getenv("METRICS_PORT", "9093"))

BATCH_INTERVAL_S = float(os.getenv("BATCH_INTERVAL_S", "30.0"))
MIN_BATCH_SIZE = int(os.getenv("MIN_BATCH_SIZE", "1"))
LEDGER_PATH = Path(os.getenv("LEDGER_PATH", "/data/ledger.jsonl"))

batches_committed = Counter("cosmo_anchor_batches_committed_total", "Ledger batches committed")
fingerprints_anchored = Counter("cosmo_anchor_fingerprints_total", "Fingerprints anchored")
ledger_size = Gauge("cosmo_anchor_ledger_entries", "Total ledger entries")


def sha3_256(data: bytes) -> bytes:
    return hashlib.sha3_256(data).digest()


def compute_merkle_root(leaves: list[bytes]) -> bytes:
    """Compute SHA3-256 Merkle root from a list of leaf hashes."""
    if not leaves:
        return sha3_256(b"empty")
    layer = [sha3_256(leaf) for leaf in leaves]
    while len(layer) > 1:
        next_layer = []
        for i in range(0, len(layer), 2):
            left = layer[i]
            right = layer[i + 1] if i + 1 < len(layer) else left
            next_layer.append(sha3_256(left + right))
        layer = next_layer
    return layer[0]


class AppendOnlyLedger:
    """File-backed append-only ledger with hash chaining."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._batch_id = 0
        self._prev_hash = sha3_256(b"genesis")
        self._recover()

    def _recover(self) -> None:
        if not self._path.exists():
            return
        with open(self._path, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                entry = json.loads(line)
                self._batch_id = entry["batch_id"] + 1
                self._prev_hash = bytes.fromhex(entry["entry_hash"])
        logger.info("Recovered ledger at batch_id=%d", self._batch_id)
        ledger_size.set(self._batch_id)

    def commit(self, fingerprints: list[bytes]) -> dict:
        merkle_root = compute_merkle_root(fingerprints)
        fp_hex = [fp.hex() for fp in fingerprints]

        entry = {
            "batch_id": self._batch_id,
            "merkle_root": merkle_root.hex(),
            "fingerprints": fp_hex,
            "prev_hash": self._prev_hash.hex(),
            "timestamp": time.time(),
            "timestamp_ns": time.time_ns(),
            "n_fingerprints": len(fingerprints),
        }

        entry_bytes = json.dumps(entry, sort_keys=True).encode()
        entry_hash = sha3_256(entry_bytes)
        entry["entry_hash"] = entry_hash.hex()

        with open(self._path, "a") as f:
            f.write(json.dumps(entry, sort_keys=True) + "\n")

        self._prev_hash = entry_hash
        self._batch_id += 1
        ledger_size.set(self._batch_id)

        return entry


class AnchoringDaemon:
    def __init__(self) -> None:
        self._redis: aioredis.Redis | None = None
        self._ledger = AppendOnlyLedger(LEDGER_PATH)
        self._pending_fingerprints: list[bytes] = []
        self._running = True

    async def connect(self) -> None:
        self._redis = aioredis.from_url(REDIS_URL, decode_responses=False)
        await self._redis.ping()

        for stream in (EXTRACTION_STREAM, CONDITIONED_STREAM):
            try:
                await self._redis.xgroup_create(stream, CONSUMER_GROUP, id="0", mkstream=True)
            except aioredis.ResponseError as e:
                if "BUSYGROUP" not in str(e):
                    raise
        logger.info("Anchoring daemon connected to Redis")

    async def close(self) -> None:
        self._running = False
        if self._pending_fingerprints:
            self._commit_batch()
        if self._redis:
            await self._redis.aclose()

    async def _consume_fingerprints(self) -> None:
        streams = {
            EXTRACTION_STREAM: ">",
            CONDITIONED_STREAM: ">",
        }
        while self._running:
            try:
                messages = await self._redis.xreadgroup(
                    CONSUMER_GROUP,
                    CONSUMER_NAME,
                    streams,
                    count=200,
                    block=1000,
                )
                if not messages:
                    continue
                for stream_name, entries in messages:
                    msg_ids = []
                    for msg_id, fields in entries:
                        fp = fields.get(b"fingerprint", b"")
                        if fp:
                            self._pending_fingerprints.append(fp)
                        msg_ids.append(msg_id)
                    if msg_ids:
                        stream_key = stream_name if isinstance(stream_name, str) else stream_name.decode()
                        await self._redis.xack(stream_key, CONSUMER_GROUP, *msg_ids)
            except Exception:
                logger.exception("Error consuming fingerprints")
                await asyncio.sleep(1)

    def _commit_batch(self) -> None:
        if len(self._pending_fingerprints) < MIN_BATCH_SIZE:
            return
        batch = self._pending_fingerprints[:]
        self._pending_fingerprints.clear()

        entry = self._ledger.commit(batch)
        batches_committed.inc()
        fingerprints_anchored.inc(len(batch))

        logger.info(
            "Committed batch %d: %d fingerprints, merkle_root=%s",
            entry["batch_id"],
            entry["n_fingerprints"],
            entry["merkle_root"][:16] + "…",
        )

    async def _periodic_commit(self) -> None:
        while self._running:
            await asyncio.sleep(BATCH_INTERVAL_S)
            try:
                self._commit_batch()
            except Exception:
                logger.exception("Error committing batch")

    async def run(self) -> None:
        await asyncio.gather(self._consume_fingerprints(), self._periodic_commit())


async def main() -> None:
    start_http_server(METRICS_PORT)
    logger.info("Prometheus metrics on :%d", METRICS_PORT)

    daemon = AnchoringDaemon()
    await daemon.connect()

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    task = asyncio.create_task(daemon.run())

    await stop.wait()
    logger.info("Shutting down anchoring daemon…")
    await daemon.close()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    asyncio.run(main())
