"""Ingestion Service — real-time async ingestion of sensor event timestamps.

Exposes gRPC and MQTT endpoints simulating muon detector interval capture.
Validates, deduplicates, and publishes raw timestamps to Redis Streams for
downstream entropy extraction.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import struct
import time
from concurrent import futures
from dataclasses import dataclass, field
from typing import AsyncIterator

import grpc
import paho.mqtt.client as mqtt
import redis.asyncio as aioredis
from prometheus_client import Counter, Histogram, start_http_server
from pydantic import BaseModel, field_validator

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger("cosmo.ingestion")

REDIS_URL = os.environ["REDIS_URL"]
MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "cosmo/muon/events")
GRPC_PORT = int(os.getenv("GRPC_PORT", "50051"))
METRICS_PORT = int(os.getenv("METRICS_PORT", "9090"))
STREAM_KEY = "cosmo:raw_timestamps"
MAX_TIMESTAMP_DRIFT_S = float(os.getenv("MAX_TIMESTAMP_DRIFT_S", "5.0"))
DEDUP_WINDOW_NS = int(os.getenv("DEDUP_WINDOW_NS", "100"))

events_ingested = Counter("cosmo_events_ingested_total", "Total events ingested", ["source"])
events_rejected = Counter("cosmo_events_rejected_total", "Rejected events", ["reason"])
ingest_latency = Histogram("cosmo_ingest_latency_seconds", "Ingestion latency")


class SensorEvent(BaseModel):
    sensor_id: str
    timestamp_ns: int
    sequence: int

    @field_validator("timestamp_ns")
    @classmethod
    def validate_timestamp(cls, v: int) -> int:
        now_ns = time.time_ns()
        drift_ns = int(MAX_TIMESTAMP_DRIFT_S * 1e9)
        if abs(v - now_ns) > drift_ns:
            raise ValueError(f"timestamp drift exceeds {MAX_TIMESTAMP_DRIFT_S}s")
        if v <= 0:
            raise ValueError("timestamp must be positive")
        return v

    @field_validator("sensor_id")
    @classmethod
    def validate_sensor_id(cls, v: str) -> str:
        if not v or len(v) > 64 or not v.isalnum():
            raise ValueError("sensor_id must be 1-64 alphanumeric chars")
        return v


@dataclass
class DeduplicationFilter:
    _seen: dict[str, int] = field(default_factory=dict)
    _window_ns: int = DEDUP_WINDOW_NS

    def is_duplicate(self, sensor_id: str, timestamp_ns: int) -> bool:
        key = f"{sensor_id}:{timestamp_ns // self._window_ns}"
        if key in self._seen:
            return True
        self._seen[key] = timestamp_ns
        self._prune(timestamp_ns)
        return False

    def _prune(self, now_ns: int) -> None:
        cutoff = now_ns - int(10e9)
        stale = [k for k, v in self._seen.items() if v < cutoff]
        for k in stale:
            del self._seen[k]


class IngestionPipeline:
    def __init__(self) -> None:
        self._redis: aioredis.Redis | None = None
        self._dedup = DeduplicationFilter()
        self._running = True

    async def connect(self) -> None:
        self._redis = aioredis.from_url(REDIS_URL, decode_responses=False)
        await self._redis.ping()
        logger.info("Connected to Redis at %s", REDIS_URL)

    async def close(self) -> None:
        self._running = False
        if self._redis:
            await self._redis.aclose()

    @ingest_latency.time()
    async def ingest(self, event: SensorEvent, source: str) -> bool:
        if self._dedup.is_duplicate(event.sensor_id, event.timestamp_ns):
            events_rejected.labels(reason="duplicate").inc()
            return False

        payload = struct.pack(">Q", event.timestamp_ns) + event.sensor_id.encode("ascii")
        await self._redis.xadd(
            STREAM_KEY,
            {
                "sensor_id": event.sensor_id.encode(),
                "timestamp_ns": str(event.timestamp_ns).encode(),
                "sequence": str(event.sequence).encode(),
                "payload": payload,
            },
            maxlen=1_000_000,
        )
        events_ingested.labels(source=source).inc()
        return True


pipeline = IngestionPipeline()


# --- MQTT listener -----------------------------------------------------------

def _build_mqtt_client() -> mqtt.Client:
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id="cosmo-ingestion",
        protocol=mqtt.MQTTv5,
    )

    def on_connect(client: mqtt.Client, userdata, flags, rc, properties=None):
        logger.info("MQTT connected (rc=%s)", rc)
        client.subscribe(MQTT_TOPIC, qos=1)

    def on_message(client: mqtt.Client, userdata, msg: mqtt.MQTTMessage):
        try:
            data = json.loads(msg.payload)
            event = SensorEvent(**data)
            loop = asyncio.get_event_loop()
            asyncio.run_coroutine_threadsafe(pipeline.ingest(event, source="mqtt"), loop)
        except Exception:
            events_rejected.labels(reason="invalid_mqtt").inc()
            logger.debug("Rejected MQTT message", exc_info=True)

    client.on_connect = on_connect
    client.on_message = on_message
    return client


# --- gRPC service (inline proto to avoid build step for demo) -----------------

from grpc import aio as grpc_aio

_INGEST_METHOD = "/cosmo.Ingestion/Ingest"
_HEALTH_METHOD = "/grpc.health.v1.Health/Check"


class IngestionServicer(grpc_aio.GenericRpcHandler):
    """Handles unary Ingest RPCs with manual deserialization."""

    def service(self, handler_call_details):
        method = handler_call_details.method
        if method == _INGEST_METHOD:
            return grpc.unary_unary_rpc_method_handler(self._handle_ingest)
        if method == _HEALTH_METHOD:
            return grpc.unary_unary_rpc_method_handler(self._handle_health)
        return None

    async def _handle_ingest(self, request_bytes: bytes, context):
        try:
            data = json.loads(request_bytes)
            event = SensorEvent(**data)
            ok = await pipeline.ingest(event, source="grpc")
            return json.dumps({"accepted": ok}).encode()
        except Exception as exc:
            events_rejected.labels(reason="invalid_grpc").inc()
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(exc))
            return b"{}"

    async def _handle_health(self, request_bytes: bytes, context):
        return json.dumps({"status": "SERVING"}).encode()


async def serve_grpc() -> grpc_aio.Server:
    server = grpc_aio.server(futures.ThreadPoolExecutor(max_workers=4))
    server.add_generic_rpc_handlers([IngestionServicer()])
    listen_addr = f"[::]:{GRPC_PORT}"
    server.add_insecure_port(listen_addr)
    await server.start()
    logger.info("gRPC server listening on %s", listen_addr)
    return server


# --- Entrypoint ---------------------------------------------------------------

async def main() -> None:
    start_http_server(METRICS_PORT)
    logger.info("Prometheus metrics on :%d", METRICS_PORT)

    await pipeline.connect()
    grpc_server = await serve_grpc()

    mqtt_client = _build_mqtt_client()
    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
        mqtt_client.loop_start()
        logger.info("MQTT connected to %s:%d", MQTT_BROKER, MQTT_PORT)
    except Exception:
        logger.warning("MQTT broker unavailable — gRPC-only mode", exc_info=True)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    await stop.wait()
    logger.info("Shutting down…")
    mqtt_client.loop_stop()
    mqtt_client.disconnect()
    await grpc_server.stop(grace=5)
    await pipeline.close()


if __name__ == "__main__":
    asyncio.run(main())
