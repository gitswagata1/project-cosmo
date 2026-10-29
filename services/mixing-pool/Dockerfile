FROM python:3.12-slim AS base

RUN groupadd -r cosmo && useradd -r -g cosmo -s /usr/sbin/nologin cosmo
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

USER cosmo

EXPOSE 9092

HEALTHCHECK --interval=15s --timeout=3s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:9092/metrics')" || exit 1

ENTRYPOINT ["python", "-u", "src/pool.py"]
