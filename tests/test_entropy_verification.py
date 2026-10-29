"""Entropy verification tests — validates min-entropy bounds, LHL extraction
correctness, and NIST SP 800-90B sanity checks before key derivation.

Tests use synthetic timestamp streams with known statistical properties
to verify that the extraction pipeline produces correct entropy estimates
and that extracted output satisfies the Leftover Hash Lemma bounds.
"""

import hashlib
import math
import secrets

import numpy as np
import pytest

from extractor import (
    EntropyEstimate,
    compute_inter_arrival_deltas,
    estimate_min_entropy,
    extract_entropy_lhl,
    quantize_deltas,
    run_nist_90b_sanity_checks,
    sha3_512_fingerprint,
)


class TestInterArrivalDeltas:
    def test_basic_monotonic_sequence(self):
        ts = np.array([100, 200, 350, 500, 700], dtype=np.int64)
        deltas = compute_inter_arrival_deltas(ts)
        np.testing.assert_array_equal(deltas, [100, 150, 150, 200])

    def test_unsorted_input_is_sorted(self):
        ts = np.array([500, 100, 350, 200, 700], dtype=np.int64)
        deltas = compute_inter_arrival_deltas(ts)
        np.testing.assert_array_equal(deltas, [100, 150, 150, 200])

    def test_duplicate_timestamps_filtered(self):
        ts = np.array([100, 100, 200, 200, 300], dtype=np.int64)
        deltas = compute_inter_arrival_deltas(ts)
        assert len(deltas) == 2
        np.testing.assert_array_equal(deltas, [100, 100])

    def test_single_timestamp_returns_empty(self):
        ts = np.array([100], dtype=np.int64)
        deltas = compute_inter_arrival_deltas(ts)
        assert len(deltas) == 0

    def test_large_nanosecond_values(self):
        base = 1_700_000_000_000_000_000
        ts = np.array([base, base + 1000, base + 3000, base + 6000], dtype=np.int64)
        deltas = compute_inter_arrival_deltas(ts)
        np.testing.assert_array_equal(deltas, [1000, 2000, 3000])


class TestQuantization:
    def test_quantize_bins_correctly(self):
        deltas = np.array([500, 1500, 2500, 3500], dtype=np.int64)
        bins = quantize_deltas(deltas, bin_width=1000)
        np.testing.assert_array_equal(bins, [0, 1, 2, 3])

    def test_quantize_groups_similar_values(self):
        deltas = np.array([100, 150, 200, 950, 1050, 1100], dtype=np.int64)
        bins = quantize_deltas(deltas, bin_width=1000)
        np.testing.assert_array_equal(bins, [0, 0, 0, 0, 1, 1])


class TestMinEntropyEstimation:
    def test_uniform_distribution_high_entropy(self):
        rng = np.random.default_rng(42)
        symbols = rng.integers(0, 256, size=10_000)
        est = estimate_min_entropy(symbols)
        assert est.min_entropy_per_sample > 7.0
        assert est.q_factor > 0.85
        assert est.alphabet_size == 256

    def test_constant_sequence_zero_entropy(self):
        symbols = np.zeros(1000, dtype=np.int64)
        est = estimate_min_entropy(symbols)
        assert est.min_entropy_per_sample == 0.0
        assert est.q_factor == 0.0

    def test_binary_biased_source(self):
        rng = np.random.default_rng(42)
        symbols = rng.choice([0, 1], size=10_000, p=[0.9, 0.1])
        est = estimate_min_entropy(symbols)
        assert est.min_entropy_per_sample < 0.2
        assert est.k_eff < est.n_samples * 0.2

    def test_k_eff_respects_lhl_bound(self):
        rng = np.random.default_rng(42)
        symbols = rng.integers(0, 16, size=4096)
        est = estimate_min_entropy(symbols)
        theoretical_max = est.n_samples * est.min_entropy_per_sample
        assert est.k_eff <= theoretical_max

    def test_confidence_interval_conservative(self):
        rng = np.random.default_rng(42)
        symbols = rng.integers(0, 8, size=500)
        est = estimate_min_entropy(symbols)
        _, counts = np.unique(symbols, return_counts=True)
        raw_p_max = counts.max() / len(symbols)
        raw_h_inf = -math.log2(raw_p_max)
        assert est.min_entropy_per_sample <= raw_h_inf


class TestNIST90BSanityChecks:
    def test_good_random_source_no_warnings(self):
        rng = np.random.default_rng(42)
        deltas = rng.exponential(scale=5000, size=1000).astype(np.int64)
        deltas = deltas[deltas > 0]
        warnings = run_nist_90b_sanity_checks(deltas)
        assert len(warnings) == 0

    def test_insufficient_samples_flagged(self):
        deltas = np.array([100, 200, 300], dtype=np.int64)
        warnings = run_nist_90b_sanity_checks(deltas)
        assert any("insufficient" in w for w in warnings)

    def test_constant_deltas_flagged(self):
        deltas = np.full(500, 1000, dtype=np.int64)
        warnings = run_nist_90b_sanity_checks(deltas)
        assert len(warnings) > 0

    def test_highly_correlated_flagged(self):
        deltas = np.tile([100, 200], 250).astype(np.int64)
        warnings = run_nist_90b_sanity_checks(deltas)
        assert any("dominant" in w or "autocorrelation" in w or "alphabet" in w for w in warnings)


class TestLHLExtraction:
    def test_output_length_matches_k_eff(self):
        rng = np.random.default_rng(42)
        deltas = rng.exponential(scale=5000, size=2048).astype(np.int64)
        deltas = deltas[deltas > 0]
        symbols = quantize_deltas(deltas)
        est = estimate_min_entropy(symbols)
        extracted = extract_entropy_lhl(deltas, est)
        assert len(extracted) == max(1, est.k_eff // 8)

    def test_different_seeds_produce_different_output(self):
        rng = np.random.default_rng(42)
        deltas = rng.exponential(scale=5000, size=1024).astype(np.int64)
        deltas = deltas[deltas > 0]
        symbols = quantize_deltas(deltas)
        est = estimate_min_entropy(symbols)
        out1 = extract_entropy_lhl(deltas, est, seed=b"seed_a" + b"\x00" * 26)
        out2 = extract_entropy_lhl(deltas, est, seed=b"seed_b" + b"\x00" * 26)
        assert out1 != out2

    def test_same_seed_is_deterministic(self):
        rng = np.random.default_rng(42)
        deltas = rng.exponential(scale=5000, size=1024).astype(np.int64)
        deltas = deltas[deltas > 0]
        symbols = quantize_deltas(deltas)
        est = estimate_min_entropy(symbols)
        seed = secrets.token_bytes(32)
        out1 = extract_entropy_lhl(deltas, est, seed=seed)
        out2 = extract_entropy_lhl(deltas, est, seed=seed)
        assert out1 == out2

    def test_extraction_uses_shake256(self):
        rng = np.random.default_rng(42)
        deltas = rng.exponential(scale=5000, size=512).astype(np.int64)
        deltas = deltas[deltas > 0]
        symbols = quantize_deltas(deltas)
        est = estimate_min_entropy(symbols)
        seed = b"\x00" * 32
        extracted = extract_entropy_lhl(deltas, est, seed=seed)

        raw = deltas.astype(">i8").tobytes()
        expected = hashlib.shake_256(seed + raw).digest(max(1, est.k_eff // 8))
        assert extracted == expected


class TestSHA3Fingerprint:
    def test_fingerprint_is_64_bytes(self):
        fp = sha3_512_fingerprint(b"test data")
        assert len(fp) == 64

    def test_fingerprint_deterministic(self):
        data = b"cosmo entropy block"
        assert sha3_512_fingerprint(data) == sha3_512_fingerprint(data)

    def test_fingerprint_different_for_different_input(self):
        assert sha3_512_fingerprint(b"a") != sha3_512_fingerprint(b"b")


class TestEndToEndExtraction:
    """Integration test: timestamp ingestion → delta computation → entropy
    estimation → LHL extraction → fingerprint generation."""

    def test_full_pipeline(self):
        rng = np.random.default_rng(seed=12345)
        base_ns = 1_700_000_000_000_000_000
        intervals = rng.exponential(scale=10_000, size=2048).astype(np.int64)
        intervals = np.clip(intervals, 1, None)
        timestamps = base_ns + np.cumsum(intervals)

        deltas = compute_inter_arrival_deltas(timestamps)
        assert len(deltas) == len(timestamps) - 1
        assert np.all(deltas > 0)

        warnings = run_nist_90b_sanity_checks(deltas)

        symbols = quantize_deltas(deltas, bin_width=1000)
        estimate = estimate_min_entropy(symbols)

        assert estimate.min_entropy_per_sample > 0
        assert estimate.n_samples == len(symbols)
        assert 0 <= estimate.q_factor <= 1.0
        assert estimate.k_eff >= 0

        extracted = extract_entropy_lhl(deltas, estimate)
        assert len(extracted) > 0

        fingerprint = sha3_512_fingerprint(deltas.astype(">i8").tobytes())
        assert len(fingerprint) == 64

    def test_pipeline_rejects_degenerate_source(self):
        timestamps = np.arange(1000, 2000, dtype=np.int64)
        deltas = compute_inter_arrival_deltas(timestamps)
        symbols = quantize_deltas(deltas)
        estimate = estimate_min_entropy(symbols)
        assert estimate.min_entropy_per_sample == 0.0
        assert estimate.k_eff == 0
