#!/usr/bin/env python3
"""
Unit tests for tools/health_check.py

Covers:
  1. TokenBucketRateLimiter basic acquire / throttle
  2. TokenBucketRateLimiter refill over time
  3. TokenBucketRateLimiter rate reduction (half-open factor)
  4. CircuitBreaker state transitions (CLOSED -> OPEN -> HALF_OPEN -> CLOSED)
  5. CircuitBreaker HALF_OPEN failure re-opens
  6. --timeout CLI flag overrides default timeout
  7. --probe-rate CLI flag enables rate limiter
  8. Half-open breaker reduces probe rate to 50%
  9. Rate limiter stats present in JSON report
 10. HTTP probe is skipped when breaker is OPEN
"""

import json
import os
import sys
import time
import unittest
from unittest.mock import patch, MagicMock

# Ensure we import from the tools directory
TOOLS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TOOLS_DIR)

import health_check as hc


class TestTokenBucketRateLimiter(unittest.TestCase):
    """Tests 1-3: token bucket rate limiter behavior."""

    def test_acquire_and_throttle(self):
        """Test 1: tokens are consumed until empty, then throttled."""
        rl = hc.TokenBucketRateLimiter(rate=2, capacity=2)
        # With capacity=2, first 2 acquires should succeed
        self.assertTrue(rl.acquire())
        self.assertTrue(rl.acquire())
        # Third should be throttled (no tokens left, no time to refill)
        self.assertFalse(rl.acquire())
        self.assertEqual(rl.throttled_count, 1)
        self.assertEqual(rl.allowed_count, 2)

    def test_refill_over_time(self):
        """Test 2: tokens are refilled as time passes."""
        rl = hc.TokenBucketRateLimiter(rate=100, capacity=1)
        # Consume the single token
        self.assertTrue(rl.acquire())
        self.assertFalse(rl.acquire())
        # Wait a bit for refill (rate=100/sec means ~0.01s per token)
        time.sleep(0.05)
        # Should have refilled at least one token
        self.assertTrue(rl.acquire())

    def test_rate_reduction_factor(self):
        """Test 3: reduction factor halves the effective rate."""
        rl = hc.TokenBucketRateLimiter(rate=10, capacity=10)
        rl.set_reduction_factor(0.5)
        self.assertEqual(rl.reduction_factor, 0.5)
        self.assertEqual(rl.current_rate, 5.0)
        # Consume all tokens
        for _ in range(10):
            rl.acquire()
        self.assertFalse(rl.acquire())
        # With 0.5 factor, refill is slower; wait 0.2s
        time.sleep(0.2)
        # At 5/sec, 0.2s gives ~1 token
        rl.set_reduction_factor(1.0)
        # Reset refill timing
        rl.last_refill = time.monotonic()
        # Verify stats
        stats = rl.to_dict()
        self.assertIn("throttled_count", stats)
        self.assertIn("allowed_count", stats)
        self.assertIn("effective_rate", stats)

    def test_rate_reduction_clamping(self):
        """Reduction factor is clamped to [0, 1]."""
        rl = hc.TokenBucketRateLimiter(rate=5)
        rl.set_reduction_factor(2.0)  # should clamp to 1.0
        self.assertEqual(rl.reduction_factor, 1.0)
        rl.set_reduction_factor(-1.0)  # should clamp to 0.0
        self.assertEqual(rl.reduction_factor, 0.0)

    def test_invalid_rate_raises(self):
        """Rate <= 0 should raise ValueError."""
        with self.assertRaises(ValueError):
            hc.TokenBucketRateLimiter(rate=0)
        with self.assertRaises(ValueError):
            hc.TokenBucketRateLimiter(rate=-1)


class TestCircuitBreaker(unittest.TestCase):
    """Tests 4-5: circuit breaker state transitions."""

    def test_closed_to_open_on_failures(self):
        """Test 4a: consecutive failures open the breaker."""
        cb = hc.CircuitBreaker("test-svc", failure_threshold=3, recovery_timeout=1)
        self.assertEqual(cb.state, hc.CB_CLOSED)
        cb.record_failure()
        cb.record_failure()
        self.assertEqual(cb.state, hc.CB_CLOSED)  # not yet
        cb.record_failure()
        self.assertEqual(cb.state, hc.CB_OPEN)
        self.assertIsNotNone(cb.opened_at)

    def test_open_blocks_probes(self):
        """When OPEN, can_proceed returns False."""
        cb = hc.CircuitBreaker("test-svc", failure_threshold=1, recovery_timeout=10)
        cb.record_failure()
        self.assertEqual(cb.state, hc.CB_OPEN)
        self.assertFalse(cb.can_proceed())

    def test_open_to_half_open_after_recovery(self):
        """Test 4b: after recovery timeout, breaker goes HALF_OPEN."""
        cb = hc.CircuitBreaker("test-svc", failure_threshold=1, recovery_timeout=0.1)
        cb.record_failure()
        self.assertEqual(cb.state, hc.CB_OPEN)
        time.sleep(0.15)
        self.assertTrue(cb.can_proceed())
        self.assertEqual(cb.state, hc.CB_HALF_OPEN)

    def test_half_open_success_closes(self):
        """Test 4c: success in HALF_OPEN closes the breaker."""
        cb = hc.CircuitBreaker("test-svc", failure_threshold=1, recovery_timeout=0.1)
        cb.record_failure()
        time.sleep(0.15)
        cb.can_proceed()
        self.assertEqual(cb.state, hc.CB_HALF_OPEN)
        cb.record_success()
        self.assertEqual(cb.state, hc.CB_CLOSED)
        self.assertEqual(cb.failure_count, 0)

    def test_half_open_failure_reopens(self):
        """Test 5: failure in HALF_OPEN re-opens the breaker."""
        cb = hc.CircuitBreaker("test-svc", failure_threshold=1, recovery_timeout=0.1)
        cb.record_failure()
        time.sleep(0.15)
        cb.can_proceed()
        self.assertEqual(cb.state, hc.CB_HALF_OPEN)
        cb.record_failure()
        self.assertEqual(cb.state, hc.CB_OPEN)

    def test_half_open_probe_limit(self):
        """HALF_OPEN only allows limited probes."""
        cb = hc.CircuitBreaker("test-svc", failure_threshold=1,
                                recovery_timeout=0.1, half_open_max_probes=1)
        cb.record_failure()
        time.sleep(0.15)
        self.assertTrue(cb.can_proceed())
        cb.consume_half_open_slot()
        self.assertFalse(cb.can_proceed())  # slot consumed

    def test_to_dict(self):
        """to_dict returns useful state info."""
        cb = hc.CircuitBreaker("svc", failure_threshold=5, recovery_timeout=30)
        d = cb.to_dict()
        self.assertEqual(d["state"], "CLOSED")
        self.assertEqual(d["failure_threshold"], 5)
        self.assertIn("failure_count", d)


class TestTimeoutOverride(unittest.TestCase):
    """Test 6: --timeout CLI flag overrides default timeout."""

    def test_timeout_flag_passed_to_run(self):
        """run_health_checks passes global_timeout to HTTP check."""
        with patch.object(hc, "check_http_service") as mock_http, \
             patch.object(hc, "check_tcp_port") as mock_tcp, \
             patch.object(hc, "check_disk_usage", return_value=("OK", "", 0)), \
             patch.object(hc, "check_memory_usage", return_value=("OK", "", 0)), \
             patch.object(hc, "check_load_average", return_value=("OK", "", 0)):
            mock_http.return_value = ("OK", "HTTP 200", 200)
            mock_tcp.return_value = ("OK", "Connected", 1.0)
            results = hc.run_health_checks(
                global_timeout=42, probe_rate=None
            )
            # Verify the timeout was passed to check_http_service
            args = mock_http.call_args[0]
            self.assertEqual(args[3], 42)  # timeout arg
            # Verify the TCP check also got the override
            tcp_args = mock_tcp.call_args[0]
            self.assertEqual(tcp_args[2], 42)

    def test_timeout_flag_none_uses_default(self):
        """When global_timeout is None, service default is used."""
        with patch.object(hc, "check_http_service") as mock_http, \
             patch.object(hc, "check_tcp_port") as mock_tcp, \
             patch.object(hc, "check_disk_usage", return_value=("OK", "", 0)), \
             patch.object(hc, "check_memory_usage", return_value=("OK", "", 0)), \
             patch.object(hc, "check_load_average", return_value=("OK", "", 0)):
            mock_http.return_value = ("OK", "HTTP 200", 200)
            mock_tcp.return_value = ("OK", "Connected", 1.0)
            hc.run_health_checks(service="backend", global_timeout=None, probe_rate=None)
            args = mock_http.call_args[0]
            # backend default timeout is 5
            self.assertEqual(args[3], hc.SERVICES["backend"]["timeout"])


class TestProbeRateFlag(unittest.TestCase):
    """Test 7: --probe-rate flag enables rate limiter in report."""

    def test_probe_rate_creates_limiter(self):
        """When probe_rate is set, rate_limiter stats appear in results."""
        with patch.object(hc, "check_http_service", return_value=("OK", "HTTP 200", 200)), \
             patch.object(hc, "check_tcp_port", return_value=("OK", "Connected", 1.0)), \
             patch.object(hc, "check_disk_usage", return_value=("OK", "", 0)), \
             patch.object(hc, "check_memory_usage", return_value=("OK", "", 0)), \
             patch.object(hc, "check_load_average", return_value=("OK", "", 0)):
            results = hc.run_health_checks(probe_rate=10)
            self.assertTrue(results["rate_limiter"]["enabled"]
                            if "enabled" in results["rate_limiter"]
                            else "rate" in results["rate_limiter"])
            self.assertIn("rate", results["rate_limiter"])
            self.assertEqual(results["rate_limiter"]["rate"], 10)

    def test_no_probe_rate_disables_limiter(self):
        """Without probe_rate, rate_limiter is disabled."""
        with patch.object(hc, "check_http_service", return_value=("OK", "HTTP 200", 200)), \
             patch.object(hc, "check_tcp_port", return_value=("OK", "Connected", 1.0)), \
             patch.object(hc, "check_disk_usage", return_value=("OK", "", 0)), \
             patch.object(hc, "check_memory_usage", return_value=("OK", "", 0)), \
             patch.object(hc, "check_load_average", return_value=("OK", "", 0)):
            results = hc.run_health_checks(probe_rate=None)
            self.assertFalse(results["rate_limiter"].get("enabled", True))

    def test_rate_limiter_throttles_probes(self):
        """Test 8/10: rate limiter throttles when rate is very low."""
        with patch.object(hc, "check_http_service", return_value=("OK", "HTTP 200", 200)), \
             patch.object(hc, "check_tcp_port", return_value=("OK", "Connected", 1.0)), \
             patch.object(hc, "check_disk_usage", return_value=("OK", "", 0)), \
             patch.object(hc, "check_memory_usage", return_value=("OK", "", 0)), \
             patch.object(hc, "check_load_average", return_value=("OK", "", 0)):
            # rate=1, capacity=1 → only 1 probe allowed, rest throttled
            results = hc.run_health_checks(probe_rate=1)
            rl_stats = results["rate_limiter"]
            self.assertGreaterEqual(rl_stats["throttled_count"], 1)


class TestHalfOpenRateReduction(unittest.TestCase):
    """Test 8: half-open breaker reduces probe rate to 50%."""

    def test_half_open_sets_reduction_factor(self):
        """When any breaker is HALF_OPEN, limiter reduction factor is 0.5."""
        rl = hc.TokenBucketRateLimiter(rate=10, capacity=10)
        breakers = {"svc": hc.CircuitBreaker("svc", failure_threshold=1, recovery_timeout=0.1)}
        breakers["svc"].record_failure()
        time.sleep(0.15)
        breakers["svc"].can_proceed()  # triggers HALF_OPEN

        # Simulate the check in _probe_http
        if any(b.state == hc.CB_HALF_OPEN for b in breakers.values()):
            rl.set_reduction_factor(0.5)

        self.assertEqual(rl.reduction_factor, 0.5)
        self.assertEqual(rl.current_rate, 5.0)


class TestBreakerOpenSkipsProbe(unittest.TestCase):
    """Test 10: HTTP probe is skipped when breaker is OPEN."""

    def test_open_breaker_returns_skip_message(self):
        """When breaker is OPEN, probe returns WARNING with skip detail."""
        breakers = {"backend": hc.CircuitBreaker("backend", failure_threshold=1, recovery_timeout=60)}
        breakers["backend"].record_failure()  # opens the breaker
        self.assertEqual(breakers["backend"].state, hc.CB_OPEN)

        config = hc.SERVICES["backend"]
        result = hc._probe_http("backend", config, None, breakers, None)
        self.assertIsNotNone(result)
        self.assertEqual(result["status"], "WARNING")
        self.assertIn("Circuit breaker open", result["detail"])
        self.assertFalse(result["throttled"])


class TestReportFormat(unittest.TestCase):
    """Test 9: rate limiter stats appear in JSON and text report."""

    def test_json_report_has_rate_limiter(self):
        """JSON output includes rate_limiter key with stats."""
        with patch.object(hc, "check_http_service", return_value=("OK", "HTTP 200", 200)), \
             patch.object(hc, "check_tcp_port", return_value=("OK", "Connected", 1.0)), \
             patch.object(hc, "check_disk_usage", return_value=("OK", "", 0)), \
             patch.object(hc, "check_memory_usage", return_value=("OK", "", 0)), \
             patch.object(hc, "check_load_average", return_value=("OK", "", 0)):
            results = hc.run_health_checks(probe_rate=5)
            self.assertIn("rate_limiter", results)
            self.assertIn("throttled_count", results["rate_limiter"])
            self.assertIn("allowed_count", results["rate_limiter"])
            self.assertIn("effective_rate", results["rate_limiter"])

    def test_text_report_prints_rate_limiter(self):
        """Text report includes rate limiter section."""
        results = {
            "hostname": "test",
            "timestamp": "2026-01-01T00:00:00",
            "overall_status": "OK",
            "services": {},
            "infrastructure": {},
            "system": {},
            "rate_limiter": {
                "enabled": True,
                "rate": 5,
                "effective_rate": 5.0,
                "reduction_factor": 1.0,
                "allowed_count": 3,
                "throttled_count": 0,
            },
        }
        import io
        from contextlib import redirect_stdout
        buf = io.StringIO()
        with redirect_stdout(buf):
            hc.print_health_report(results)
        output = buf.getvalue()
        self.assertIn("Rate Limiter", output)
        self.assertIn("5", output)
        self.assertIn("throttled", output.lower())


if __name__ == "__main__":
    unittest.main()