<p align="center">
  <img src="https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python">
  <img src="https://img.shields.io/badge/NIST_SP_800--90B-Compliant-00897B?style=for-the-badge" alt="NIST">
  <img src="https://img.shields.io/badge/SHA3--512-Cryptographic-7C3AED?style=for-the-badge" alt="SHA3">
  <img src="https://img.shields.io/badge/Tests-41_Passing-34D399?style=for-the-badge" alt="Tests">
  <img src="https://img.shields.io/badge/License-MIT-blue?style=for-the-badge" alt="License">
</p>

<h1 align="center">
  <br>
  PROJECT COSMO
  <br>
</h1>

<h3 align="center">Cosmic-Ray Entropy Harvesting &amp; Blockchain Anchoring Pipeline</h3>

<p align="center">
  A cryptographically rigorous entropy harvesting system that captures cosmic-ray muon detector timing data, extracts verified randomness using the <strong>Leftover Hash Lemma</strong>, conditions it through a hybrid mixing pool, and anchors the output on a hash-chained Merkle ledger — with a real-time interactive dashboard.
</p>

<p align="center">
  <a href="https://gitswagata1.github.io/project-cosmo/"><strong>Live Dashboard</strong></a> · 
  <a href="#quick-start"><strong>Quick Start</strong></a> · 
  <a href="#architecture"><strong>Architecture</strong></a> · 
  <a href="#cryptographic-pipeline"><strong>Crypto Pipeline</strong></a>
</p>

---

## Overview

Project COSMO implements a five-stage entropy pipeline modeled after NIST SP 800-90B recommendations for non-deterministic random number generation. It simulates cosmic-ray muon detector events, validates and deduplicates the raw data, extracts cryptographically strong randomness, conditions it through a dual-source mixing pool, and commits the output to an append-only blockchain ledger.

The project includes a fully interactive **web dashboard** that visualizes the entire pipeline in real time — from muon capture to Merkle tree anchoring — with animated gauges, histograms, heatmaps, and a comprehensive statistical quality report.

### Key Features

- **NIST SP 800-90B** min-entropy estimation using the Most Common Value (MCV) estimator
- **Leftover Hash Lemma** extraction via SHAKE256 XOF for information-theoretic security
- **HKDF-SHA3-256** conditioning in a hybrid physical + CSPRNG mixing pool
- **SHA3-512** fingerprinting with Merkle tree batching and hash-chained ledger
- **41 automated tests** covering entropy verification, statistical uniformity, and mixing pool properties
- **Interactive dashboard** with light/dark themes, animated particle effects, and live pipeline visualization
- **Microservices architecture** with Docker Compose and full Kubernetes manifests

---

## Live Dashboard

> **[https://gitswagata1.github.io/project-cosmo/](https://gitswagata1.github.io/project-cosmo/)**

The dashboard runs entirely in-browser using the Web Crypto API. Click **RUN PIPELINE** to watch:

| Stage | What You See |
|-------|-------------|
| **Muon Detectors** | Animated event counter, sensor progress bars |
| **Ingestion** | Deduplication stats, rejection rate |
| **Extraction** | Arc gauges for H∞, Q-Factor, K_eff; animated histogram |
| **Mixing Pool** | Byte heatmap, conditioned output hex display |
| **Anchoring** | Merkle tree SVG with animated hash paths |
| **Quality Report** | 6 statistical tests with live pass/fail badges |

---

## Architecture

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│    MUON      │────▸│  INGESTION   │────▸│  EXTRACTION  │────▸│   MIXING     │────▸│  ANCHORING   │
│  DETECTORS   │     │   PIPELINE   │     │    ENGINE    │     │    POOL      │     │   DAEMON     │
│              │     │              │     │              │     │              │     │              │
│ 4 sensors    │     │ Validation   │     │ NIST 90B     │     │ XOR mixing   │     │ Merkle tree  │
│ Exponential  │     │ Dedup filter │     │ MCV entropy  │     │ Physical +   │     │ SHA3-256     │
│ intervals    │     │ 100ns window │     │ LHL-SHAKE256 │     │ CSPRNG       │     │ Hash-chained │
│              │     │              │     │ SHA3-512 fp  │     │ HKDF-SHA3    │     │ JSON ledger  │
└──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
```

### Microservices (Docker Compose)

| Service | Transport | Purpose |
|---------|-----------|---------|
| **Redis** | — | Event stream bus (Redis Streams) |
| **Ingestion** | gRPC + MQTT | Validates, deduplicates, and ingests sensor events |
| **Extraction** | Redis Streams | Entropy estimation and LHL extraction |
| **Mixing Pool** | Redis Streams | Dual-source XOR mixing + HKDF conditioning |
| **Anchoring** | Redis Streams | Merkle tree construction + ledger commits |

---

## Cryptographic Pipeline

### Stage 3: Entropy Extraction (core algorithm)

The extraction engine implements the **Leftover Hash Lemma** for information-theoretic randomness extraction:

```
1. Compute inter-arrival deltas:  Δᵢ = tᵢ₊₁ − tᵢ
2. Quantize into symbols:         sᵢ = ⌊Δᵢ / bin_width⌋
3. Estimate min-entropy (MCV):    H∞ = −log₂(p̂_max + z·σ)
4. Compute extractable bits:      K_eff = max(0, ⌊n·H∞ − 2·log₂(1/ε)⌋)
5. Extract via SHAKE256 XOF:      output = SHAKE256(seed ‖ data, K_eff/8)
6. Fingerprint:                   fp = SHA3-512(raw_deltas)
```

Where `ε = 2⁻¹²⁸` provides 128-bit statistical distance security.

### Stage 4: Hybrid Mixing Pool

```
pool  = os.urandom(4096)
pool ^= physical_entropy        # XOR with extracted bytes
pool ^= os.urandom(4096)        # XOR with fresh CSPRNG
output = HKDF-SHA3-256(pool)    # Condition to 32 bytes
```

### Stage 5: Blockchain Anchoring

```
leaf₁ = SHA3-256(fingerprint₁)
leaf₂ = SHA3-256(fingerprint₂)
root  = SHA3-256(leaf₁ ‖ leaf₂)

entry_hash = SHA3-256(JSON(batch_id, root, prev_hash, ...))
```

---

## Quick Start

### Web Dashboard (no dependencies)

```bash
python serve.py
# Opens http://127.0.0.1:8080 in your browser
```

### CLI Demo

```bash
pip install numpy scipy cryptography
python demo.py
```

Runs the full 5-stage pipeline in-process with colored terminal output, progress bars, and a statistical quality report.

### Run Tests

```bash
pip install numpy scipy cryptography pytest redis prometheus-client pydantic
cd tests && python -m pytest -v
```

All **41 tests** cover:
- Min-entropy estimation accuracy
- LHL extraction output length
- NIST SP 800-90B sanity checks
- Chi-squared byte distribution
- Kolmogorov-Smirnov uniformity
- Serial correlation
- Runs test for independence
- Monobit frequency
- Shannon & min-entropy rates
- Extraction idempotency
- Mixing pool XOR properties

### Docker Compose (full microservices)

```bash
docker-compose up --build
```

---

## Project Structure

```
project-cosmo/
├── web/
│   └── index.html              # Interactive dashboard (1175 lines)
├── services/
│   ├── ingestion/src/main.py   # gRPC + MQTT ingestion service
│   ├── extraction/src/          
│   │   └── extractor.py        # Core entropy extraction engine
│   ├── mixing-pool/src/pool.py # Hybrid XOR + HKDF mixing
│   └── anchoring/src/daemon.py # Merkle tree + ledger daemon
├── tests/
│   ├── test_entropy_verification.py   # 25 extraction tests
│   └── test_statistical_uniformity.py # 16 uniformity tests
├── k8s/                        # Full Kubernetes manifests
│   ├── namespace.yaml
│   ├── config.yaml
│   ├── network-policy.yaml
│   ├── redis.yaml
│   └── {service}/deployment.yaml + service.yaml
├── demo.py                     # Standalone CLI demo
├── serve.py                    # Dashboard HTTP server
├── docker-compose.yml          # 5-service orchestration
└── .github/workflows/pages.yml # GitHub Pages deployment
```

---

## Statistical Quality Tests

The pipeline gates key derivation on a full statistical battery:

| Test | Criterion | Threshold |
|------|-----------|-----------|
| Chi-squared | Byte distribution uniformity | p > 0.001 |
| Kolmogorov-Smirnov | CDF against Uniform(0,1) | p > 0.001 |
| Monobit | Bit frequency balance | p > 0.01, proportion ∈ [0.4, 0.6] |
| Runs | Independence of consecutive bits | \|z\| < 3.0 |
| Serial Correlation | Adjacent byte independence | \|r\| < 0.1 |
| Shannon Entropy | Information density | H > 6.0 bits/byte |
| Min-Entropy | Worst-case randomness | H∞ > 4.0 bits/byte |

---

## Tech Stack

| Layer | Technologies |
|-------|-------------|
| **Crypto** | SHA3-512, SHA3-256, SHAKE256 XOF, HKDF-SHA3-256 |
| **Backend** | Python 3.10+, NumPy, SciPy, cryptography, gRPC, MQTT |
| **Streaming** | Redis Streams |
| **Orchestration** | Docker Compose, Kubernetes |
| **Frontend** | Vanilla HTML/CSS/JS, Canvas API, Web Crypto API, SVG |
| **Monitoring** | Prometheus metrics (per-service) |
| **CI/CD** | GitHub Actions → GitHub Pages |

---

## Standards Compliance

- **NIST SP 800-90B** — Entropy source validation and min-entropy estimation
- **NIST SP 800-90C** — Conditioning component design (HKDF)
- **Leftover Hash Lemma** — Information-theoretic extraction guarantee with ε = 2⁻¹²⁸

---

<p align="center">
  <strong>Swagata Banerjee</strong> · VIT Vellore · BCSE324L · Foundations of Blockchain Technology
</p>
