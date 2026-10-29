FROM python:3.12-slim AS base

RUN groupadd -r cosmo && useradd -r -g cosmo -s /usr/sbin/nologin cosmo
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

USER cosmo

EXPOSE 50051 9090

HEALTHCHECK --interval=15s --timeout=3s --retries=3 \
    CMD python -c "import grpc; ch=grpc.insecure_channel('localhost:50051'); grpc.channel_ready_future(ch).result(timeout=2)" || exit 1

ENTRYPOINT ["python", "-u", "src/main.py"]
