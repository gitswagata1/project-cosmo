"""Hybrid Mixing Pool — combines low-rate physical entropy with high-rate system entropy.

Architecture:
  - Consumes extracted entropy blocks from Redis Streams (physical source).
  - Mixes with OS CSPRNG entropy (system source) via independent XOR lanes.
  - Maintains a rolling entropy pool conditioned through HKDF-SHA3-256.
  - Serves conditioned entropy to downstream consumers via a separate stream.

The mixing follows the "entropy cannot decrease" principle: XOR of independent
sources preserves the entropy of the stronger source, and HKDF conditioning
ensures uniform distribution of the output.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import secrets
import signal
import time

import redis.asyncio as aioredis
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives import hashes
from prometheus_client import Counter, Gauge, start_http_server

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger("cosmo.mixing_pool")

REDIS_URL = os.environ["REDIS_URL"]
INPUT_STREAM = os.getenv("INPUT_STREAM", "cosmo:extracted_entropy")
OUTPUT_STREAM = os.getenv("OUTPUT_STREAM", "cosmo:conditioned_entropy")
CONSUMER_GROUP = "mixing-pool"
CONSUMER_NAME = os.getenv("HOSTNAME", f"mixer-{secrets.token_hex(4)}")
METRICS_PORT = int(os.getenv("METRICS_PORT", "9092"))

POOL_SIZE_BYTES = int(os.getenv("POOL_SIZE_BYTES", "4096"))
OUTPUT_BLOCK_BYTES = int(os.getenv("OUTPUT_BLOCK_BYTES", "32"))
MIX_INTERVAL_S = float(os.getenv("MIX_INTERVAL_S", "1.0"))

blocks_mixed = Counter("cosmo_pool_blocks_mixed_total", "Blocks ingested into pool")
blocks_emitted = Counter("cosmo_pool_blocks_emitted_total", "Conditioned blocks emitted")
pool_generation = Counter("cosmo_pool_generation_total", "Pool re-conditioning cycles")
pool_physical_bytes = Gauge("cosmo_pool_physical_bytes_ingested", "Physical entropy bytes in current cycle")


class EntropyPool:
    """Thread-safe entropy pool with dual-source mixing and HKDF conditioning."""

    def __init__(self, size: int = POOL_SIZE_BYTES) -> None:
        self._size = size
        self._pool = bytearray(secrets.token_bytes(size))
        self._physical_accum = bytearray()
        self._mix_count = 0

    def feed_physical(self, data: bytes) -> None:
        self._physical_accum.extend(data)

    def _xor_into_pool(self, data: bytes) -> None:
        for i, b in enumerate(data):
            self._pool[i % self._size] ^= b

    def mix_and_condition(self) -> bytes:
        system_entropy = secrets.token_bytes(self._size)
        self._xor_into_pool(system_entropy)

        if self._physical_accum:
            self._xor_into_pool(bytes(self._physical_accum))
            physical_len = len(self._physical_accum)
            self._physical_accum.clear()
        else:
            physical_len = 0

        pool_physical_bytes.set(physical_len)

        salt = hashlib.sha3_256(
            self._mix_count.to_bytes(8, "big") + time.time_ns().to_bytes(8, "big")
        ).digest()

        hkdf = HKDF(
            algorithm=hashes.SHA256(),
            length=OUTPUT_BLOCK_BYTES,
            salt=salt,
            info=b"cosmo-mixing-pool-v1",
        )
        conditioned = hkdf.derive(bytes(self._pool))

        self._pool = bytearray(secrets.token_bytes(self._size))
        self._xor_into_pool(conditioned)
        self._mix_count += 1
        pool_generation.inc()

        return conditioned


class MixingPoolWorker:
    def __init__(self) -> None:
        self._redis: aioredis.Redis | None = None
        self._pool = EntropyPool()
        self._running = True

    async def connect(self) -> None:
        self._redis = aioredis.from_url(REDIS_URL, decode_responses=False)
        await self._redis.ping()
        try:
            await self._redis.xgroup_create(INPUT_STREAM, CONSUMER_GROUP, id="0", mkstream=True)
        except aioredis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise
        logger.info("Mixing pool connected to Redis")

    async def close(self) -> None:
        self._running = False
        if self._redis:
            await self._redis.aclose()

    async def _consume_physical(self) -> None:
        while self._running:
            try:
                messages = await self._redis.xreadgroup(
                    CONSUMER_GROUP,
                    CONSUMER_NAME,
                    {INPUT_STREAM: ">"},
                    count=100,
                    block=500,
                )
                if not messages:
                    continue
                for stream_name, entries in messages:
                    msg_ids = []
                    for msg_id, fields in entries:
                        entropy_data = fields.get(b"entropy", b"")
                        if entropy_data:
                            self._pool.feed_physical(entropy_data)
                            blocks_mixed.inc()
                        msg_ids.append(msg_id)
                    if msg_ids:
                        await self._redis.xack(INPUT_STREAM, CONSUMER_GROUP, *msg_ids)
            except Exception:
                logger.exception("Error consuming physical entropy")
                await asyncio.sleep(1)

    async def _periodic_mix(self) -> None:
        while self._running:
            await asyncio.sleep(MIX_INTERVAL_S)
            try:
                conditioned = self._pool.mix_and_condition()
                fingerprint = hashlib.sha3_512(conditioned).digest()

                await self._redis.xadd(
                    OUTPUT_STREAM,
                    {
                        "entropy": conditioned,
                        "fingerprint": fingerprint,
                        "generation": str(self._pool._mix_count).encode(),
                        "timestamp": str(time.time_ns()).encode(),
                    },
                    maxlen=50_000,
                )
                blocks_emitted.inc()
                logger.debug("Emitted conditioned block gen=%d", self._pool._mix_count)
            except Exception:
                logger.exception("Error in periodic mix")

    async def run(self) -> None:
        await asyncio.gather(self._consume_physical(), self._periodic_mix())


async def main() -> None:
    start_http_server(METRICS_PORT)
    logger.info("Prometheus metrics on :%d", METRICS_PORT)

    worker = MixingPoolWorker()
    await worker.connect()

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    task = asyncio.create_task(worker.run())

    await stop.wait()
    logger.info("Shutting down mixing pool…")
    await worker.close()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    asyncio.run(main())
