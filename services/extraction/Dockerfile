FROM python:3.12-slim AS base

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd -r cosmo && useradd -r -g cosmo -s /usr/sbin/nologin cosmo
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

USER cosmo

EXPOSE 9091

HEALTHCHECK --interval=15s --timeout=3s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:9091/metrics')" || exit 1

ENTRYPOINT ["python", "-u", "src/extractor.py"]
