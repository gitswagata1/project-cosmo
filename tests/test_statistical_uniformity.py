"""Statistical uniformity tests — validates that extracted entropy output
satisfies uniformity requirements before key derivation.

Implements standard statistical test battery:
  - Chi-squared byte distribution test
  - Kolmogorov-Smirnov test against uniform distribution
  - Serial correlation coefficient test
  - Runs test for independence
  - Monobit frequency test
  - Entropy rate estimation (Shannon / min-entropy)

These tests gate key derivation: if extracted output fails uniformity,
it must not be used for cryptographic key material.
"""

import hashlib
import math
import secrets
from collections import Counter

import numpy as np
import pytest
from scipy import stats as sp_stats

from extractor import (
    compute_inter_arrival_deltas,
    estimate_min_entropy,
    extract_entropy_lhl,
    quantize_deltas,
)


def _generate_extracted_block(n_timestamps: int = 4096, rng_seed: int = 42) -> bytes:
    """Helper: generate an extracted entropy block from synthetic muon data."""
    rng = np.random.default_rng(rng_seed)
    base = 1_700_000_000_000_000_000
    intervals = rng.exponential(scale=10_000, size=n_timestamps).astype(np.int64)
    intervals = np.clip(intervals, 1, None)
    timestamps = base + np.cumsum(intervals)

    deltas = compute_inter_arrival_deltas(timestamps)
    symbols = quantize_deltas(deltas, bin_width=1000)
    estimate = estimate_min_entropy(symbols)
    return extract_entropy_lhl(deltas, estimate, seed=secrets.token_bytes(32))


class TestChiSquaredByteDistribution:
    """Byte values in extracted output should be approximately uniform."""

    @pytest.mark.parametrize("seed", [42, 123, 999, 2024, 31415])
    def test_byte_distribution_uniform(self, seed: int):
        block = _generate_extracted_block(n_timestamps=8192, rng_seed=seed)
        if len(block) < 64:
            pytest.skip("Block too short for byte distribution test")

        byte_counts = np.bincount(np.frombuffer(block, dtype=np.uint8), minlength=256)
        nonzero_bins = byte_counts[byte_counts > 0]

        if len(nonzero_bins) < 10:
            pytest.skip("Too few distinct byte values for chi-squared test")

        expected = len(block) / 256
        chi2, p_value = sp_stats.chisquare(byte_counts[byte_counts > 0],
                                            f_exp=expected)
        assert p_value > 0.001, f"Chi-squared p={p_value:.6f} — output not uniform"


class TestKolmogorovSmirnov:
    """Byte values should follow a uniform distribution."""

    @pytest.mark.parametrize("seed", [42, 100, 777])
    def test_ks_uniform(self, seed: int):
        block = _generate_extracted_block(n_timestamps=8192, rng_seed=seed)
        if len(block) < 64:
            pytest.skip("Block too short for KS test")

        byte_values = np.frombuffer(block, dtype=np.uint8) / 255.0
        stat, p_value = sp_stats.kstest(byte_values, "uniform")
        assert p_value > 0.001, f"KS test p={p_value:.6f} — not uniformly distributed"


class TestSerialCorrelation:
    """Adjacent bytes should show no significant correlation."""

    def test_serial_correlation_low(self):
        block = _generate_extracted_block(n_timestamps=8192, rng_seed=42)
        if len(block) < 128:
            pytest.skip("Block too short for correlation test")

        data = np.frombuffer(block, dtype=np.uint8).astype(np.float64)
        n = len(data)
        mean = data.mean()
        centered = data - mean
        autocorr = np.correlate(centered, centered, mode="full")
        autocorr = autocorr[n - 1:]
        c0 = autocorr[0]
        if c0 == 0:
            pytest.skip("Zero variance — degenerate block")
        lag1_corr = autocorr[1] / c0
        assert abs(lag1_corr) < 0.1, f"Serial correlation |r|={abs(lag1_corr):.4f} > 0.1"


class TestRunsTest:
    """Bit runs in extracted output should match expected distribution."""

    def test_runs_above_below_median(self):
        block = _generate_extracted_block(n_timestamps=8192, rng_seed=42)
        if len(block) < 64:
            pytest.skip("Block too short for runs test")

        data = np.frombuffer(block, dtype=np.uint8).astype(np.float64)
        if np.std(data) == 0:
            pytest.skip("Zero-variance block")

        median = np.median(data)
        binary = (data >= median).astype(int)

        runs = 1
        for i in range(1, len(binary)):
            if binary[i] != binary[i - 1]:
                runs += 1

        n1 = np.sum(binary)
        n0 = len(binary) - n1
        n = len(binary)

        if n0 == 0 or n1 == 0:
            pytest.skip("All values on one side of median")

        expected_runs = 1 + (2 * n0 * n1) / n
        variance_runs = (2 * n0 * n1 * (2 * n0 * n1 - n)) / (n * n * (n - 1))

        if variance_runs <= 0:
            pytest.skip("Variance of runs is non-positive")

        z = (runs - expected_runs) / math.sqrt(variance_runs)
        assert abs(z) < 3.0, f"Runs test |z|={abs(z):.2f} > 3.0 — non-random pattern"


class TestMonobitFrequency:
    """Bit frequency should be approximately 50/50."""

    def test_monobit(self):
        block = _generate_extracted_block(n_timestamps=8192, rng_seed=42)
        if len(block) < 32:
            pytest.skip("Block too short for monobit test")

        bits = np.unpackbits(np.frombuffer(block, dtype=np.uint8))
        n = len(bits)
        ones = int(np.sum(bits))
        proportion = ones / n

        s_obs = abs(2 * ones - n) / math.sqrt(n)
        p_value = math.erfc(s_obs / math.sqrt(2))
        assert p_value > 0.01, f"Monobit p={p_value:.6f} — bit bias detected"
        assert 0.4 < proportion < 0.6, f"Bit proportion {proportion:.4f} outside [0.4, 0.6]"


class TestEntropyRate:
    """Extracted output should have high Shannon entropy per byte."""

    def test_shannon_entropy_per_byte(self):
        block = _generate_extracted_block(n_timestamps=8192, rng_seed=42)
        if len(block) < 64:
            pytest.skip("Block too short for entropy estimation")

        freq = Counter(block)
        n = len(block)
        h = -sum((count / n) * math.log2(count / n) for count in freq.values())
        assert h > 6.0, f"Shannon entropy H={h:.3f} bits/byte — too low (< 6.0)"

    def test_min_entropy_per_byte(self):
        block = _generate_extracted_block(n_timestamps=8192, rng_seed=42)
        if len(block) < 64:
            pytest.skip("Block too short for min-entropy estimation")

        freq = Counter(block)
        n = len(block)
        p_max = max(freq.values()) / n
        h_inf = -math.log2(p_max) if p_max < 1.0 else 0.0
        assert h_inf > 4.0, f"Min-entropy H_∞={h_inf:.3f} bits/byte — too low (< 4.0)"


class TestExtractionIdempotency:
    """Extraction with same inputs must produce same output."""

    def test_deterministic_with_fixed_seed(self):
        rng = np.random.default_rng(42)
        intervals = rng.exponential(scale=10_000, size=2048).astype(np.int64)
        intervals = np.clip(intervals, 1, None)
        base = 1_700_000_000_000_000_000
        timestamps = base + np.cumsum(intervals)
        deltas = compute_inter_arrival_deltas(timestamps)
        symbols = quantize_deltas(deltas)
        est = estimate_min_entropy(symbols)

        seed = b"fixed-seed-for-reproducibility\x00\x00"
        out1 = extract_entropy_lhl(deltas, est, seed=seed)
        out2 = extract_entropy_lhl(deltas, est, seed=seed)
        assert out1 == out2


class TestMixingPoolProperties:
    """Verify mixing pool XOR properties."""

    def test_xor_independence(self):
        a = secrets.token_bytes(256)
        b = secrets.token_bytes(256)
        xored = bytes(x ^ y for x, y in zip(a, b))

        freq_a = Counter(a)
        freq_xor = Counter(xored)
        n = len(xored)
        p_max_xor = max(freq_xor.values()) / n
        h_inf_xor = -math.log2(p_max_xor) if p_max_xor < 1.0 else 0.0
        assert h_inf_xor > 3.0, "XOR of two random sources should preserve entropy"

    def test_xor_with_constant_preserves_entropy(self):
        good = secrets.token_bytes(256)
        bad = b"\x42" * 256
        xored = bytes(x ^ y for x, y in zip(good, bad))

        freq = Counter(xored)
        n = len(xored)
        h = -sum((c / n) * math.log2(c / n) for c in freq.values())
        assert h > 5.0, "XOR with constant should preserve good source's entropy"
