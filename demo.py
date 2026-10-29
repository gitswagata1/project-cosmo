"""
Project COSMO — Live Demo
Cosmic-ray entropy harvesting pipeline with blockchain anchoring.

Runs the full pipeline in-process (no Docker/Redis needed):
  Muon Sensor Simulation -> Ingestion -> Extraction -> Mixing Pool -> Blockchain Anchoring
"""

import hashlib
import io
import json
import math
import os
import secrets
import sys
import time
from collections import Counter
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import numpy as np
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives import hashes

sys.path.insert(0, str(Path(__file__).parent / "services" / "extraction" / "src"))
from extractor import (
    compute_inter_arrival_deltas,
    quantize_deltas,
    estimate_min_entropy,
    extract_entropy_lhl,
    run_nist_90b_sanity_checks,
    sha3_512_fingerprint,
)

# ── Terminal colors ──────────────────────────────────────────────────────────

class C:
    HEADER  = "\033[95m"
    BLUE    = "\033[94m"
    CYAN    = "\033[96m"
    GREEN   = "\033[92m"
    YELLOW  = "\033[93m"
    RED     = "\033[91m"
    BOLD    = "\033[1m"
    DIM     = "\033[2m"
    RESET   = "\033[0m"


def banner():
    print(f"""
{C.BOLD}{C.CYAN}╔══════════════════════════════════════════════════════════════════╗
║                                                                  ║
║   ██████╗ ██████╗ ███████╗███╗   ███╗ ██████╗                    ║
║  ██╔════╝██╔═══██╗██╔════╝████╗ ████║██╔═══██╗                   ║
║  ██║     ██║   ██║███████╗██╔████╔██║██║   ██║                   ║
║  ██║     ██║   ██║╚════██║██║╚██╔╝██║██║   ██║                   ║
║  ╚██████╗╚██████╔╝███████║██║ ╚═╝ ██║╚██████╔╝                   ║
║   ╚═════╝ ╚═════╝ ╚══════╝╚═╝     ╚═╝ ╚═════╝                   ║
║                                                                  ║
║   Cosmic-Ray Entropy Harvesting & Blockchain Anchoring Pipeline  ║
║                                                                  ║
╚══════════════════════════════════════════════════════════════════╝{C.RESET}
""")


def section(title, icon=""):
    width = 64
    print(f"\n{C.BOLD}{C.BLUE}{'─' * width}{C.RESET}")
    print(f"{C.BOLD}{C.BLUE}  {icon}  {title}{C.RESET}")
    print(f"{C.BOLD}{C.BLUE}{'─' * width}{C.RESET}\n")


def step(msg):
    print(f"  {C.DIM}>{C.RESET} {msg}")


def ok(msg):
    print(f"  {C.GREEN}[OK]{C.RESET} {msg}")


def warn(msg):
    print(f"  {C.YELLOW}[!!]{C.RESET} {msg}")


def metric(label, value, unit=""):
    print(f"  {C.DIM}{label:<30}{C.RESET}{C.BOLD}{value}{C.RESET} {C.DIM}{unit}{C.RESET}")


def hex_block(data: bytes, width=32):
    hex_str = data.hex()
    for i in range(0, len(hex_str), width):
        line = hex_str[i:i+width]
        spaced = " ".join(line[j:j+2] for j in range(0, len(line), 2))
        print(f"  {C.DIM}{spaced}{C.RESET}")


def progress_bar(current, total, width=40, label=""):
    filled = int(width * current / total)
    bar = f"{'█' * filled}{'░' * (width - filled)}"
    pct = current / total * 100
    print(f"\r  {label} [{C.CYAN}{bar}{C.RESET}] {pct:5.1f}%", end="", flush=True)


# ── Stage 1: Muon Sensor Simulation ─────────────────────────────────────────

def simulate_muon_events(n_sensors=4, events_per_sensor=1024, seed=42):
    section("STAGE 1: Muon Detector Simulation", ">>")
    step(f"Initializing {n_sensors} virtual muon detectors...")
    time.sleep(0.3)

    rng = np.random.default_rng(seed)
    all_events = []

    for i in range(n_sensors):
        sensor_id = f"muon{i+1:02d}"
        base_ns = time.time_ns()
        intervals = rng.exponential(scale=8000, size=events_per_sensor).astype(np.int64)
        intervals = np.clip(intervals, 1, None)
        timestamps = base_ns + np.cumsum(intervals)

        for seq, ts in enumerate(timestamps):
            all_events.append({
                "sensor_id": sensor_id,
                "timestamp_ns": int(ts),
                "sequence": seq,
            })
        ok(f"Sensor {C.CYAN}{sensor_id}{C.RESET}: {events_per_sensor} events captured  "
           f"{C.DIM}(mean interval: {intervals.mean():.0f} ns){C.RESET}")

    all_events.sort(key=lambda e: e["timestamp_ns"])

    print()
    metric("Total events", f"{len(all_events):,}")
    metric("Time span", f"{(all_events[-1]['timestamp_ns'] - all_events[0]['timestamp_ns']) / 1e6:.2f}", "ms")
    metric("Sensors active", n_sensors)

    return all_events


# ── Stage 2: Ingestion ──────────────────────────────────────────────────────

def run_ingestion(events):
    section("STAGE 2: Ingestion Pipeline", ">>")
    step("Validating and deduplicating sensor events...")
    time.sleep(0.2)

    seen = set()
    accepted = []
    duplicates = 0
    window_ns = 100

    for i, ev in enumerate(events):
        key = f"{ev['sensor_id']}:{ev['timestamp_ns'] // window_ns}"
        if key in seen:
            duplicates += 1
            continue
        seen.add(key)
        accepted.append(ev)
        if (i + 1) % 500 == 0:
            progress_bar(i + 1, len(events), label="Ingesting")

    progress_bar(len(events), len(events), label="Ingesting")
    print()
    print()

    ok(f"Ingested {C.BOLD}{len(accepted):,}{C.RESET} events")
    if duplicates:
        warn(f"Filtered {duplicates} duplicates (dedup window: {window_ns} ns)")

    timestamps_ns = np.array([e["timestamp_ns"] for e in accepted], dtype=np.int64)

    metric("Accepted events", f"{len(accepted):,}")
    metric("Rejection rate", f"{duplicates / len(events) * 100:.2f}", "%")

    return timestamps_ns


# ── Stage 3: Entropy Extraction ─────────────────────────────────────────────

def run_extraction(timestamps_ns, batch_size=1024):
    section("STAGE 3: Entropy Extraction Engine", ">>")
    step("Computing inter-arrival deltas...")
    time.sleep(0.2)

    deltas = compute_inter_arrival_deltas(timestamps_ns)
    ok(f"Computed {len(deltas):,} deltas from {len(timestamps_ns):,} timestamps")

    print()
    step("Running NIST SP 800-90B sanity checks...")
    warnings = run_nist_90b_sanity_checks(deltas)
    if warnings:
        for w in warnings:
            warn(f"90B: {w}")
    else:
        ok("All NIST SP 800-90B checks passed")

    print()
    step("Quantizing deltas and estimating min-entropy...")
    symbols = quantize_deltas(deltas, bin_width=1000)
    estimate = estimate_min_entropy(symbols)

    print()
    print(f"  {C.BOLD}{C.GREEN}┌─────────────────────────────────────────────┐{C.RESET}")
    print(f"  {C.BOLD}{C.GREEN}│  Entropy Estimation Results                 │{C.RESET}")
    print(f"  {C.BOLD}{C.GREEN}├─────────────────────────────────────────────┤{C.RESET}")
    print(f"  {C.BOLD}{C.GREEN}│{C.RESET}  Min-entropy H_inf   : {C.BOLD}{estimate.min_entropy_per_sample:.4f}{C.RESET} bits/sample  {C.BOLD}{C.GREEN}│{C.RESET}")
    print(f"  {C.BOLD}{C.GREEN}│{C.RESET}  Quality factor Q    : {C.BOLD}{estimate.q_factor:.4f}{C.RESET}              {C.BOLD}{C.GREEN}│{C.RESET}")
    print(f"  {C.BOLD}{C.GREEN}│{C.RESET}  Extractable bits K  : {C.BOLD}{estimate.k_eff:,}{C.RESET}               {' ' * max(0, 5 - len(str(estimate.k_eff)))}{C.BOLD}{C.GREEN}│{C.RESET}")
    print(f"  {C.BOLD}{C.GREEN}│{C.RESET}  Samples             : {C.BOLD}{estimate.n_samples:,}{C.RESET}               {' ' * max(0, 5 - len(str(estimate.n_samples)))}{C.BOLD}{C.GREEN}│{C.RESET}")
    print(f"  {C.BOLD}{C.GREEN}│{C.RESET}  Alphabet size       : {C.BOLD}{estimate.alphabet_size:,}{C.RESET}               {' ' * max(0, 5 - len(str(estimate.alphabet_size)))}{C.BOLD}{C.GREEN}│{C.RESET}")
    print(f"  {C.BOLD}{C.GREEN}└─────────────────────────────────────────────┘{C.RESET}")

    print()
    step("Applying Leftover Hash Lemma extraction (SHAKE256 XOF)...")
    seed = secrets.token_bytes(32)
    extracted = extract_entropy_lhl(deltas, estimate, seed=seed)
    ok(f"Extracted {C.BOLD}{len(extracted)}{C.RESET} bytes ({len(extracted) * 8} bits) of entropy")

    print()
    step("Computing SHA3-512 fingerprint for anchoring...")
    fingerprint = sha3_512_fingerprint(deltas.astype(">i8").tobytes())
    ok(f"Fingerprint ({len(fingerprint)} bytes):")
    print(f"  {C.DIM}{fingerprint.hex()[:64]}{C.RESET}")
    print(f"  {C.DIM}{fingerprint.hex()[64:]}{C.RESET}")

    return extracted, fingerprint, estimate


# ── Stage 4: Mixing Pool ────────────────────────────────────────────────────

def run_mixing_pool(extracted_entropy):
    section("STAGE 4: Hybrid Mixing Pool", ">>")

    pool_size = 4096
    step(f"Initializing {pool_size}-byte entropy pool with OS CSPRNG...")
    pool = bytearray(secrets.token_bytes(pool_size))
    time.sleep(0.2)

    step("Feeding physical entropy into pool...")
    for i, b in enumerate(extracted_entropy):
        pool[i % pool_size] ^= b
    ok(f"Mixed {len(extracted_entropy)} bytes of physical entropy via XOR")

    step("Mixing with fresh OS CSPRNG entropy...")
    system_entropy = secrets.token_bytes(pool_size)
    for i, b in enumerate(system_entropy):
        pool[i % pool_size] ^= b
    ok(f"Mixed {pool_size} bytes of system entropy via XOR")

    print()
    step("Conditioning pool through HKDF-SHA3-256...")
    salt = hashlib.sha3_256(
        int(0).to_bytes(8, "big") + time.time_ns().to_bytes(8, "big")
    ).digest()

    hkdf = HKDF(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        info=b"cosmo-mixing-pool-v1",
    )
    conditioned = hkdf.derive(bytes(pool))
    ok(f"Conditioned output: {len(conditioned)} bytes")

    print()
    print(f"  {C.BOLD}Conditioned entropy block:{C.RESET}")
    hex_block(conditioned)

    freq = Counter(conditioned)
    n = len(conditioned)
    h = -sum((c / n) * math.log2(c / n) for c in freq.values())
    print()
    metric("Shannon entropy", f"{h:.2f}", "bits/byte")

    return conditioned


# ── Stage 5: Blockchain Anchoring ────────────────────────────────────────────

def run_anchoring(fingerprints, ledger_path=None):
    section("STAGE 5: Blockchain Anchoring", ">>")

    if ledger_path is None:
        ledger_path = Path(__file__).parent / "demo_ledger.jsonl"

    def sha3_256(data: bytes) -> bytes:
        return hashlib.sha3_256(data).digest()

    def merkle_root(leaves):
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

    step(f"Batching {len(fingerprints)} fingerprints into Merkle tree...")
    time.sleep(0.2)

    root = merkle_root(fingerprints)

    depth = math.ceil(math.log2(max(len(fingerprints), 1)))
    ok(f"Merkle tree constructed (depth={depth}, leaves={len(fingerprints)})")

    print()
    print(f"  {C.BOLD}{C.YELLOW}┌─────────────────────────────────────────────┐{C.RESET}")
    print(f"  {C.BOLD}{C.YELLOW}│  Merkle Root (anchored on-chain)            │{C.RESET}")
    print(f"  {C.BOLD}{C.YELLOW}├─────────────────────────────────────────────┤{C.RESET}")
    print(f"  {C.BOLD}{C.YELLOW}│{C.RESET}  {root.hex()[:44]}  {C.BOLD}{C.YELLOW}│{C.RESET}")
    print(f"  {C.BOLD}{C.YELLOW}│{C.RESET}  {root.hex()[44:]:<44}  {C.BOLD}{C.YELLOW}│{C.RESET}")
    print(f"  {C.BOLD}{C.YELLOW}└─────────────────────────────────────────────┘{C.RESET}")

    print()
    step("Writing to append-only ledger...")

    genesis_hash = sha3_256(b"genesis")
    entry = {
        "batch_id": 0,
        "merkle_root": root.hex(),
        "fingerprints": [fp.hex() for fp in fingerprints],
        "prev_hash": genesis_hash.hex(),
        "timestamp": time.time(),
        "timestamp_ns": time.time_ns(),
        "n_fingerprints": len(fingerprints),
    }
    entry_bytes = json.dumps(entry, sort_keys=True).encode()
    entry_hash = sha3_256(entry_bytes)
    entry["entry_hash"] = entry_hash.hex()

    with open(ledger_path, "w") as f:
        f.write(json.dumps(entry, sort_keys=True) + "\n")

    ok(f"Ledger entry written to {C.DIM}{ledger_path}{C.RESET}")

    print()
    metric("Batch ID", 0)
    metric("Fingerprints anchored", len(fingerprints))
    metric("Entry hash", entry_hash.hex()[:32] + "...")
    metric("Chain link (prev_hash)", genesis_hash.hex()[:32] + "...")

    print()
    step("Verifying ledger integrity...")
    time.sleep(0.2)

    with open(ledger_path, "r") as f:
        stored = json.loads(f.readline().strip())

    stored_hash = stored.pop("entry_hash")
    recomputed = sha3_256(json.dumps(stored, sort_keys=True).encode()).hex()
    if recomputed == stored_hash:
        ok(f"Ledger integrity verified {C.GREEN}(hash chain valid){C.RESET}")
    else:
        warn("Ledger integrity check FAILED")

    return entry


# ── Stage 6: Output Quality Report ──────────────────────────────────────────

def quality_report(extracted_entropy):
    section("QUALITY REPORT: Statistical Uniformity", ">>")

    if len(extracted_entropy) < 32:
        warn("Extracted block too short for full statistical analysis")
        return

    data = np.frombuffer(extracted_entropy, dtype=np.uint8)

    byte_counts = np.bincount(data, minlength=256)
    nonzero = byte_counts[byte_counts > 0]

    from scipy import stats as sp_stats

    if len(nonzero) >= 10:
        observed = byte_counts[byte_counts > 0]
        expected = np.full_like(observed, dtype=float, fill_value=observed.sum() / len(observed))
        chi2, p_chi2 = sp_stats.chisquare(observed, f_exp=expected)
        status = f"{C.GREEN}PASS{C.RESET}" if p_chi2 > 0.01 else f"{C.RED}FAIL{C.RESET}"
        metric("Chi-squared test", f"p={p_chi2:.4f}  [{status}]")
    else:
        metric("Chi-squared test", "SKIP (too few bins)")

    bits = np.unpackbits(data)
    n_bits = len(bits)
    ones = int(np.sum(bits))
    proportion = ones / n_bits
    s_obs = abs(2 * ones - n_bits) / math.sqrt(n_bits)
    p_mono = math.erfc(s_obs / math.sqrt(2))
    status = f"{C.GREEN}PASS{C.RESET}" if p_mono > 0.01 else f"{C.RED}FAIL{C.RESET}"
    metric("Monobit test", f"p={p_mono:.4f}  [{status}]")
    metric("Bit proportion (1s)", f"{proportion:.4f}  (ideal: 0.5000)")

    freq = Counter(extracted_entropy)
    n = len(extracted_entropy)
    h_shannon = -sum((c / n) * math.log2(c / n) for c in freq.values())
    p_max = max(freq.values()) / n
    h_min = -math.log2(p_max) if p_max < 1.0 else 0.0
    metric("Shannon entropy", f"{h_shannon:.3f}", "bits/byte (ideal: 8.0)")
    metric("Min-entropy", f"{h_min:.3f}", "bits/byte")

    if len(data) >= 128:
        centered = data.astype(np.float64) - data.mean()
        autocorr = np.correlate(centered, centered, mode="full")
        c0 = autocorr[len(data) - 1]
        if c0 > 0:
            lag1 = autocorr[len(data)] / c0
            status = f"{C.GREEN}PASS{C.RESET}" if abs(lag1) < 0.1 else f"{C.RED}FAIL{C.RESET}"
            metric("Serial correlation", f"|r|={abs(lag1):.4f}  [{status}]")


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    banner()

    print(f"  {C.DIM}Pipeline: Sensor -> Ingestion -> Extraction -> Mixing -> Anchoring{C.RESET}")
    print(f"  {C.DIM}Mode: In-process demo (no Docker/Redis required){C.RESET}")
    print(f"  {C.DIM}Security: NIST SP 800-90B entropy validation + SHA3 + HKDF{C.RESET}")
    print()

    # Stage 1
    events = simulate_muon_events(n_sensors=4, events_per_sensor=1024)

    # Stage 2
    timestamps_ns = run_ingestion(events)

    # Stage 3
    extracted, fingerprint, estimate = run_extraction(timestamps_ns)

    # Stage 4
    conditioned = run_mixing_pool(extracted)

    # Stage 5
    fingerprints = [fingerprint, hashlib.sha3_512(conditioned).digest()]
    ledger_entry = run_anchoring(fingerprints)

    # Stage 6
    quality_report(extracted)

    # Final summary
    section("DEMO COMPLETE", ">>")
    print(f"  {C.BOLD}{C.GREEN}Full pipeline executed successfully.{C.RESET}")
    print()
    print(f"  {C.DIM}Events ingested   :{C.RESET}  {len(events):,}")
    print(f"  {C.DIM}Entropy extracted  :{C.RESET}  {len(extracted)} bytes ({len(extracted)*8} bits)")
    print(f"  {C.DIM}Min-entropy H_inf  :{C.RESET}  {estimate.min_entropy_per_sample:.4f} bits/sample")
    print(f"  {C.DIM}Merkle root        :{C.RESET}  {ledger_entry['merkle_root'][:32]}...")
    print(f"  {C.DIM}Ledger entries     :{C.RESET}  1 (hash-chained)")
    print()
    print(f"  {C.DIM}Ledger file: demo_ledger.jsonl{C.RESET}")
    print()


if __name__ == "__main__":
    main()
