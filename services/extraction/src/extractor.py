"""Entropy Extraction Engine — production-grade worker.

Consumes raw sensor timestamps from Redis Streams, computes inter-arrival
delta times, estimates min-entropy per NIST SP 800-90B, and applies
Leftover-Hash-Lemma-based extraction via SHAKE256 XOF.

Key concepts:
  - Inter-arrival delta Δt_i = t_{i+1} - t_i (nanoseconds)
  - Min-entropy H_∞ = -log2(max_i p_i) over quantized delta bins
  - Q-factor = H_∞ / log2(|alphabet|), quality ratio ∈ (0, 1]
  - K_eff = n * H_∞ - 2*log2(1/ε), extractable secure bits (LHL bound)
  - Extraction: SHAKE256(seed ‖ deltas) truncated to K_eff bits
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import math
import os
import secrets
import signal
import struct
import time
from dataclasses import dataclass

import numpy as np
import redis.asyncio as aioredis
from prometheus_client import Counter, Gauge, Histogram, start_http_server
from scipy import stats as sp_stats

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger("cosmo.extraction")

REDIS_URL = os.environ.get("REDIS_URL", "")
INPUT_STREAM = os.getenv("INPUT_STREAM", "cosmo:raw_timestamps")
OUTPUT_STREAM = os.getenv("OUTPUT_STREAM", "cosmo:extracted_entropy")
CONSUMER_GROUP = "extraction-workers"
CONSUMER_NAME = os.getenv("HOSTNAME", f"worker-{secrets.token_hex(4)}")
METRICS_PORT = int(os.getenv("METRICS_PORT", "9091"))

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1024"))
BIN_WIDTH_NS = int(os.getenv("BIN_WIDTH_NS", "1000"))
SECURITY_PARAM_BITS = int(os.getenv("SECURITY_PARAM_BITS", "128"))
MIN_ENTROPY_THRESHOLD = float(os.getenv("MIN_ENTROPY_THRESHOLD", "0.5"))
EXTRACTION_EPSILON = 2.0 ** (-SECURITY_PARAM_BITS)

blocks_extracted = Counter("cosmo_entropy_blocks_extracted_total", "Entropy blocks produced")
blocks_rejected = Counter("cosmo_entropy_blocks_rejected_total", "Blocks failing min-entropy check")
current_min_entropy = Gauge("cosmo_current_min_entropy_bits", "Latest H_∞ estimate")
current_q_factor = Gauge("cosmo_current_q_factor", "Latest Q-factor")
current_k_eff = Gauge("cosmo_current_k_eff_bits", "Latest K_eff")
extraction_latency = Histogram("cosmo_extraction_latency_seconds", "Per-batch extraction time")


@dataclass(frozen=True)
class EntropyEstimate:
    min_entropy_per_sample: float
    q_factor: float
    k_eff: int
    n_samples: int
    alphabet_size: int


def compute_inter_arrival_deltas(timestamps_ns: np.ndarray) -> np.ndarray:
    """Compute Δt_i = t_{i+1} - t_i in nanoseconds."""
    sorted_ts = np.sort(timestamps_ns)
    deltas = np.diff(sorted_ts)
    return deltas[deltas > 0]


def quantize_deltas(deltas_ns: np.ndarray, bin_width: int = BIN_WIDTH_NS) -> np.ndarray:
    """Quantize deltas into discrete bins for symbol frequency analysis."""
    return (deltas_ns // bin_width).astype(np.int64)


def estimate_min_entropy(symbols: np.ndarray) -> EntropyEstimate:
    """Estimate min-entropy H_∞ per NIST SP 800-90B MCV (Most Common Value) estimator.

    H_∞ = -log2(p_max) where p_max is the maximum observed symbol frequency,
    upper-bounded by a confidence interval.
    """
    n = len(symbols)
    unique, counts = np.unique(symbols, return_counts=True)
    alphabet_size = len(unique)

    p_max = counts.max() / n
    z_alpha = 2.576  # 99% confidence
    p_upper = min(1.0, p_max + z_alpha * math.sqrt(p_max * (1 - p_max) / n))

    h_inf = -math.log2(p_upper) if p_upper < 1.0 else 0.0
    log2_alpha = math.log2(alphabet_size) if alphabet_size > 1 else 1.0
    q_factor = h_inf / log2_alpha if log2_alpha > 0 else 0.0
    k_eff = max(0, int(n * h_inf - 2 * math.log2(1.0 / EXTRACTION_EPSILON)))

    return EntropyEstimate(
        min_entropy_per_sample=h_inf,
        q_factor=q_factor,
        k_eff=k_eff,
        n_samples=n,
        alphabet_size=alphabet_size,
    )


def run_nist_90b_sanity_checks(deltas_ns: np.ndarray) -> list[str]:
    """Lightweight NIST SP 800-90B sanity checks on raw deltas.

    Not a full compliance suite — flags obvious non-randomness.
    """
    warnings: list[str] = []
    n = len(deltas_ns)
    if n < 128:
        warnings.append(f"insufficient samples ({n} < 128)")
        return warnings

    median = np.median(deltas_ns)
    binary = (deltas_ns >= median).astype(int)
    runs = 1 + np.sum(binary[1:] != binary[:-1])
    n1 = int(np.sum(binary))
    n0 = n - n1
    if n0 > 0 and n1 > 0:
        expected = 1.0 + (2.0 * n0 * n1) / n
        var_denom = n * n * (n - 1)
        variance = (2.0 * n0 * n1 * (2.0 * n0 * n1 - n)) / var_denom if var_denom > 0 else 0
        if variance > 0:
            z = (runs - expected) / math.sqrt(variance)
            p_runs = 2.0 * (1.0 - sp_stats.norm.cdf(abs(z)))
            if p_runs < 0.01:
                warnings.append(f"runs test p={p_runs:.4f} < 0.01 — possible autocorrelation")

    symbols = quantize_deltas(deltas_ns)
    unique, counts = np.unique(symbols, return_counts=True)
    if len(unique) < 4:
        warnings.append(f"alphabet too small ({len(unique)} symbols)")

    freq = counts / counts.sum()
    if freq.max() > 0.5:
        warnings.append(f"dominant symbol frequency {freq.max():.3f} > 0.5")

    return warnings


def extract_entropy_lhl(
    deltas_ns: np.ndarray,
    estimate: EntropyEstimate,
    seed: bytes | None = None,
) -> bytes:
    """Apply Leftover Hash Lemma extraction using SHAKE256 as the universal hash.

    seed ‖ raw_deltas → SHAKE256 XOF → truncated to ⌊K_eff / 8⌋ bytes.
    """
    if seed is None:
        seed = secrets.token_bytes(32)

    output_bytes = max(1, estimate.k_eff // 8)

    raw = deltas_ns.astype(">i8").tobytes()
    shake = hashlib.shake_256(seed + raw)
    return shake.digest(output_bytes)


def sha3_512_fingerprint(data: bytes) -> bytes:
    """SHA3-512 fingerprint for blockchain anchoring."""
    return hashlib.sha3_512(data).digest()


class ExtractionWorker:
    def __init__(self) -> None:
        self._redis: aioredis.Redis | None = None
        self._running = True
        self._seed = secrets.token_bytes(32)

    async def connect(self) -> None:
        self._redis = aioredis.from_url(REDIS_URL, decode_responses=False)
        await self._redis.ping()
        try:
            await self._redis.xgroup_create(INPUT_STREAM, CONSUMER_GROUP, id="0", mkstream=True)
        except aioredis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise
        logger.info("Connected to Redis, consumer group ready")

    async def close(self) -> None:
        self._running = False
        if self._redis:
            await self._redis.aclose()

    @extraction_latency.time()
    async def process_batch(self) -> bool:
        messages = await self._redis.xreadgroup(
            CONSUMER_GROUP,
            CONSUMER_NAME,
            {INPUT_STREAM: ">"},
            count=BATCH_SIZE,
            block=2000,
        )

        if not messages:
            return False

        timestamps_ns = []
        msg_ids = []
        for stream_name, entries in messages:
            for msg_id, fields in entries:
                try:
                    ts = int(fields[b"timestamp_ns"])
                    timestamps_ns.append(ts)
                    msg_ids.append(msg_id)
                except (KeyError, ValueError):
                    continue

        if len(timestamps_ns) < 2:
            if msg_ids:
                await self._redis.xack(INPUT_STREAM, CONSUMER_GROUP, *msg_ids)
            return False

        ts_array = np.array(timestamps_ns, dtype=np.int64)
        deltas = compute_inter_arrival_deltas(ts_array)

        if len(deltas) == 0:
            await self._redis.xack(INPUT_STREAM, CONSUMER_GROUP, *msg_ids)
            return False

        warnings = run_nist_90b_sanity_checks(deltas)
        for w in warnings:
            logger.warning("90B check: %s", w)

        symbols = quantize_deltas(deltas)
        estimate = estimate_min_entropy(symbols)

        current_min_entropy.set(estimate.min_entropy_per_sample)
        current_q_factor.set(estimate.q_factor)
        current_k_eff.set(estimate.k_eff)

        logger.info(
            "Batch n=%d H_∞=%.3f Q=%.3f K_eff=%d alphabet=%d",
            estimate.n_samples,
            estimate.min_entropy_per_sample,
            estimate.q_factor,
            estimate.k_eff,
            estimate.alphabet_size,
        )

        if estimate.min_entropy_per_sample < MIN_ENTROPY_THRESHOLD:
            blocks_rejected.inc()
            logger.warning("Min-entropy %.3f below threshold %.3f — discarding batch",
                           estimate.min_entropy_per_sample, MIN_ENTROPY_THRESHOLD)
            await self._redis.xack(INPUT_STREAM, CONSUMER_GROUP, *msg_ids)
            return False

        extracted = extract_entropy_lhl(deltas, estimate, seed=self._seed)
        fingerprint = sha3_512_fingerprint(deltas.astype(">i8").tobytes())

        await self._redis.xadd(
            OUTPUT_STREAM,
            {
                "entropy": extracted,
                "k_eff": str(estimate.k_eff).encode(),
                "h_inf": f"{estimate.min_entropy_per_sample:.6f}".encode(),
                "q_factor": f"{estimate.q_factor:.6f}".encode(),
                "n_samples": str(estimate.n_samples).encode(),
                "fingerprint": fingerprint,
                "timestamp": str(time.time_ns()).encode(),
            },
            maxlen=100_000,
        )

        self._seed = secrets.token_bytes(32)

        await self._redis.xack(INPUT_STREAM, CONSUMER_GROUP, *msg_ids)
        blocks_extracted.inc()
        return True

    async def run(self) -> None:
        while self._running:
            try:
                await self.process_batch()
            except Exception:
                logger.exception("Batch processing error")
                await asyncio.sleep(1)


async def main() -> None:
    start_http_server(METRICS_PORT)
    logger.info("Prometheus metrics on :%d", METRICS_PORT)

    worker = ExtractionWorker()
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
    logger.info("Shutting down extraction worker…")
    await worker.close()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    asyncio.run(main())
