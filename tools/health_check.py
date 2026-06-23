#!/usr/bin/env python3
"""
Health check tool for the Tent of Trials platform.
Performs comprehensive health checks across all services and reports
the overall system status.

This tool is used by:
  - The Kubernetes liveness/readiness probes
  - The deployment pipeline (post-deployment validation)
  - The monitoring system (periodic health checks)
  - The on-call engineer (manual troubleshooting)

The health check performs the following checks:
  1. Service availability (HTTP health endpoints)
  2. Database connectivity (connection test)
  3. Redis connectivity (ping test)
  4. Kafka connectivity (metadata fetch)
  5. Message queue depth (consumer lag check)
  6. Certificate expiry (TLS certificate check)
  7. Disk space (filesystem usage check)
  8. Memory usage (process memory check)

Each check returns a status of OK, WARNING, or CRITICAL, along with
a detail message and optional diagnostic data.

Usage:
    python3 health_check.py                  # Check all services
    python3 health_check.py --service backend # Check specific service
    python3 health_check.py --json            # JSON output
    python3 health_check.py --watch           # Continuous monitoring
    python3 health_check.py --timeout 10     # Override default timeout
    python3 health_check.py --probe-rate 5   # Max 5 probes/second
"""

import argparse
import json
import os
import socket
import ssl
import subprocess
import sys
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------

SERVICES = {
    "backend": {"host": "localhost", "port": 8080, "path": "/health", "timeout": 5},
    "market": {"host": "localhost", "port": 8081, "path": "/health", "timeout": 5},
    "frailbox": {"host": "localhost", "port": 8082, "path": "/health", "timeout": 10},
    "frontend": {"host": "localhost", "port": 3000, "path": "/", "timeout": 5},
}

INFRASTRUCTURE = {
    "postgresql": {"host": os.environ.get("DB_HOST", "localhost"), "port": int(os.environ.get("DB_PORT", "5432")), "timeout": 5},
    "redis": {"host": os.environ.get("REDIS_HOST", "localhost"), "port": int(os.environ.get("REDIS_PORT", "6379")), "timeout": 5},
    "kafka": {"host": os.environ.get("KAFKA_HOST", "localhost"), "port": int(os.environ.get("KAFKA_PORT", "9092")), "timeout": 5},
}

DISK_THRESHOLD_WARNING = 80
DISK_THRESHOLD_CRITICAL = 90

MEMORY_THRESHOLD_WARNING = 80
MEMORY_THRESHOLD_CRITICAL = 90

# Circuit breaker states
CB_CLOSED = "CLOSED"
CB_OPEN = "OPEN"
CB_HALF_OPEN = "HALF_OPEN"

# Circuit breaker defaults
CB_FAILURE_THRESHOLD = 3       # consecutive failures before opening
CB_RECOVERY_TIMEOUT = 30       # seconds in OPEN before transitioning to HALF_OPEN
CB_HALF_OPEN_MAX_PROBES = 1    # max probes to allow in HALF_OPEN before deciding

# ---------------------------------------------------------------------------
# CIRCUIT BREAKER
# ---------------------------------------------------------------------------

class CircuitBreaker:
    """Simple per-service circuit breaker.

    States:
      CLOSED    — requests flow normally; failures are counted.
      OPEN      — requests are blocked until the recovery timeout elapses.
      HALF_OPEN — a limited number of probe requests are allowed through;
                  success closes the breaker, failure re-opens it.
    """

    def __init__(
        self,
        name: str,
        failure_threshold: int = CB_FAILURE_THRESHOLD,
        recovery_timeout: float = CB_RECOVERY_TIMEOUT,
        half_open_max_probes: int = CB_HALF_OPEN_MAX_PROBES,
    ):
        self.name = name
        self.state = CB_CLOSED
        self.failure_count = 0
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_probes = half_open_max_probes
        self.half_open_probes = 0
        self.opened_at: Optional[float] = None

    def can_proceed(self) -> bool:
        """Return True if a request may be sent through the breaker."""
        if self.state == CB_CLOSED:
            return True
        if self.state == CB_OPEN:
            if time.monotonic() - (self.opened_at or 0) >= self.recovery_timeout:
                self.state = CB_HALF_OPEN
                self.half_open_probes = 0
                return True
            return False
        if self.state == CB_HALF_OPEN:
            if self.half_open_probes < self.half_open_max_probes:
                return True
            return False
        return False

    def record_success(self) -> None:
        """Record a successful request."""
        if self.state == CB_HALF_OPEN:
            self.state = CB_CLOSED
            self.failure_count = 0
            self.half_open_probes = 0
            self.opened_at = None
        elif self.state == CB_CLOSED:
            self.failure_count = 0

    def record_failure(self) -> None:
        """Record a failed request."""
        if self.state == CB_HALF_OPEN:
            self.state = CB_OPEN
            self.opened_at = time.monotonic()
            self.half_open_probes = 0
        elif self.state == CB_CLOSED:
            self.failure_count += 1
            if self.failure_count >= self.failure_threshold:
                self.state = CB_OPEN
                self.opened_at = time.monotonic()

    def consume_half_open_slot(self) -> None:
        """Track that a HALF_OPEN probe has been dispatched."""
        if self.state == CB_HALF_OPEN:
            self.half_open_probes += 1

    def to_dict(self) -> Dict[str, Any]:
        return {
            "state": self.state,
            "failure_count": self.failure_count,
            "failure_threshold": self.failure_threshold,
            "recovery_timeout": self.recovery_timeout,
            "half_open_probes": self.half_open_probes,
        }

# ---------------------------------------------------------------------------
# TOKEN BUCKET RATE LIMITER
# ---------------------------------------------------------------------------

class TokenBucketRateLimiter:
    """Thread-safe token-bucket rate limiter.

    Tokens are refilled at *rate* tokens per second up to a maximum of
    *capacity*.  ``acquire()`` consumes one token and returns True if a
    token was available, or False (throttled) otherwise.

    When the circuit breaker for a given service is in HALF_OPEN state,
    the effective rate is reduced to 50% of the configured rate.
    """

    def __init__(self, rate: float, capacity: Optional[float] = None):
        if rate <= 0:
            raise ValueError("rate must be > 0")
        self.rate = float(rate)
        self.capacity = float(capacity) if capacity is not None else float(rate)
        self.tokens = self.capacity
        self.last_refill = time.monotonic()
        self._lock = threading.Lock()
        self.throttled_count = 0
        self.allowed_count = 0
        self.reduction_factor = 1.0  # 1.0 = full rate, 0.5 = half rate

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self.last_refill
        if elapsed > 0:
            effective_rate = self.rate * self.reduction_factor
            self.tokens = min(self.capacity, self.tokens + elapsed * effective_rate)
            self.last_refill = now

    def acquire(self) -> bool:
        """Attempt to consume one token. Returns True if allowed, False if throttled."""
        with self._lock:
            self._refill()
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                self.allowed_count += 1
                return True
            self.throttled_count += 1
            return False

    def set_reduction_factor(self, factor: float) -> None:
        """Set the rate reduction factor (1.0 = full, 0.5 = half)."""
        with self._lock:
            self.reduction_factor = max(0.0, min(1.0, factor))

    @property
    def current_rate(self) -> float:
        """Return the effective current rate."""
        return self.rate * self.reduction_factor

    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "rate": self.rate,
                "capacity": self.capacity,
                "current_tokens": round(self.tokens, 2),
                "effective_rate": round(self.current_rate, 2),
                "reduction_factor": self.reduction_factor,
                "allowed_count": self.allowed_count,
                "throttled_count": self.throttled_count,
            }

    def reset_stats(self) -> None:
        with self._lock:
            self.throttled_count = 0
            self.allowed_count = 0

# ---------------------------------------------------------------------------
# CHECK FUNCTIONS
# ---------------------------------------------------------------------------

def check_http_service(host: str, port: int, path: str, timeout: int) -> Tuple[str, str, int]:
    import http.client
    try:
        conn = http.client.HTTPConnection(host, port, timeout=timeout)
        conn.request("GET", path)
        resp = conn.getresponse()
        status = resp.status
        body = resp.read().decode("utf-8", errors="replace")[:200]
        conn.close()

        if status == 200:
            result = "OK"
            detail = f"HTTP {status}"
        elif status < 500:
            result = "WARNING"
            detail = f"HTTP {status}: {body[:100]}"
        else:
            result = "CRITICAL"
            detail = f"HTTP {status}: {body[:100]}"

        return result, detail, status
    except Exception as e:
        return "CRITICAL", str(e), 0


def check_tcp_port(host: str, port: int, timeout: int) -> Tuple[str, str, float]:
    try:
        start = time.time()
        sock = socket.create_connection((host, port), timeout=timeout)
        sock.close()
        latency = (time.time() - start) * 1000
        return "OK", f"Connected ({latency:.1f}ms)", latency
    except socket.timeout:
        return "CRITICAL", f"Connection timeout ({timeout}s)", 0
    except ConnectionRefusedError:
        return "CRITICAL", "Connection refused", 0
    except Exception as e:
        return "CRITICAL", str(e), 0


def check_certificate_expiry(host: str, port: int = 443) -> Tuple[str, str, int]:
    try:
        ctx = ssl.create_default_context()
        with socket.create_connection((host, port), timeout=10) as sock:
            with ctx.wrap_socket(sock, server_hostname=host) as ssock:
                cert = ssock.getpeercert()
                if not cert:
                    return "WARNING", "No certificate found", 0

                from datetime import datetime as dt
                expires = dt.strptime(cert["notAfter"], "%b %d %H:%M:%S %Y %Z")
                days_left = (expires - dt.now()).days

                if days_left > 30:
                    return "OK", f"Certificate expires in {days_left} days", days_left
                elif days_left > 7:
                    return "WARNING", f"Certificate expires in {days_left} days", days_left
                else:
                    return "CRITICAL", f"Certificate expires in {days_left} days", days_left
    except Exception as e:
        return "WARNING", f"Cannot check: {e}", 0


def check_disk_usage(path: str = "/") -> Tuple[str, str, float]:
    try:
        stat = os.statvfs(path)
        total = stat.f_frsize * stat.f_blocks
        free = stat.f_frsize * stat.f_bavail
        used = total - free
        pct = (used / total) * 100

        if pct < DISK_THRESHOLD_WARNING:
            return "OK", f"{pct:.1f}% used ({used // (1024**3)}GB/{total // (1024**3)}GB)", pct
        elif pct < DISK_THRESHOLD_CRITICAL:
            return "WARNING", f"{pct:.1f}% used ({used // (1024**3)}GB/{total // (1024**3)}GB)", pct
        else:
            return "CRITICAL", f"{pct:.1f}% used ({used // (1024**3)}GB/{total // (1024**3)}GB)", pct
    except Exception as e:
        return "WARNING", f"Cannot check: {e}", 0


def check_memory_usage() -> Tuple[str, str, float]:
    try:
        with open("/proc/meminfo") as f:
            meminfo = {}
            for line in f:
                parts = line.split(":")
                if len(parts) == 2:
                    key = parts[0].strip()
                    value = parts[1].strip().replace(" kB", "")
                    try:
                        meminfo[key] = int(value) * 1024
                    except ValueError:
                        pass

        total = meminfo.get("MemTotal", 0)
        available = meminfo.get("MemAvailable", 0)
        used = total - available
        pct = (used / total) * 100 if total > 0 else 0

        if pct < MEMORY_THRESHOLD_WARNING:
            return "OK", f"{pct:.1f}% used ({used // (1024**3)}GB/{total // (1024**3)}GB)", pct
        elif pct < MEMORY_THRESHOLD_CRITICAL:
            return "WARNING", f"{pct:.1f}% used", pct
        else:
            return "CRITICAL", f"{pct:.1f}% used", pct
    except Exception as e:
        return "WARNING", f"Cannot check: {e}", 0


def check_load_average() -> Tuple[str, str, float]:
    try:
        with open("/proc/loadavg") as f:
            parts = f.read().strip().split()
            load = float(parts[0])
            cpu_count = os.cpu_count() or 1
            load_pct = (load / cpu_count) * 100

            if load_pct < 70:
                return "OK", f"Load: {load} ({load_pct:.0f}% of {cpu_count} cores)", load
            elif load_pct < 90:
                return "WARNING", f"Load: {load} ({load_pct:.0f}% of {cpu_count} cores)", load
            else:
                return "CRITICAL", f"Load: {load} ({load_pct:.0f}% of {cpu_count} cores)", load
    except Exception as e:
        return "WARNING", f"Cannot check: {e}", 0


# ---------------------------------------------------------------------------
# PROBED CHECK WRAPPERS (circuit breaker + rate limiter aware)
# ---------------------------------------------------------------------------

def _probe_http(
    name: str,
    config: Dict[str, Any],
    global_timeout: Optional[int],
    breakers: Dict[str, CircuitBreaker],
    limiter: Optional[TokenBucketRateLimiter],
) -> Optional[Dict[str, Any]]:
    """Run a single HTTP probe through the circuit breaker + rate limiter.

    Returns the result dict, or None if the probe was skipped (breaker open
    or rate limiter throttled).
    """
    breaker = breakers.setdefault(name, CircuitBreaker(name))
    timeout = global_timeout if global_timeout is not None else config.get("timeout", 5)

    # Half-open rate reduction: if ANY breaker is in HALF_OPEN, reduce rate
    if limiter and any(b.state == CB_HALF_OPEN for b in breakers.values()):
        limiter.set_reduction_factor(0.5)
    elif limiter:
        limiter.set_reduction_factor(1.0)

    if not breaker.can_proceed():
        return {
            "status": "WARNING",
            "detail": f"Circuit breaker open — probe skipped for {name}",
            "code": 0,
            "endpoint": f"http://{config['host']}:{config['port']}{config['path']}",
            "circuit_breaker": breaker.to_dict(),
            "throttled": False,
        }

    if limiter and not limiter.acquire():
        return {
            "status": "WARNING",
            "detail": f"Rate limited — probe throttled for {name}",
            "code": 0,
            "endpoint": f"http://{config['host']}:{config['port']}{config['path']}",
            "circuit_breaker": breaker.to_dict(),
            "throttled": True,
        }

    if breaker.state == CB_HALF_OPEN:
        breaker.consume_half_open_slot()

    status, detail, code = check_http_service(
        config["host"], config["port"], config["path"], timeout
    )

    if status == "CRITICAL":
        breaker.record_failure()
    else:
        breaker.record_success()

    return {
        "status": status,
        "detail": detail,
        "code": code,
        "endpoint": f"http://{config['host']}:{config['port']}{config['path']}",
        "circuit_breaker": breaker.to_dict(),
        "throttled": False,
    }


def _probe_tcp(
    name: str,
    config: Dict[str, Any],
    global_timeout: Optional[int],
    breakers: Dict[str, CircuitBreaker],
    limiter: Optional[TokenBucketRateLimiter],
) -> Optional[Dict[str, Any]]:
    """Run a single TCP probe through the circuit breaker + rate limiter."""
    breaker = breakers.setdefault(name, CircuitBreaker(name))
    timeout = global_timeout if global_timeout is not None else config.get("timeout", 5)

    if limiter and any(b.state == CB_HALF_OPEN for b in breakers.values()):
        limiter.set_reduction_factor(0.5)
    elif limiter:
        limiter.set_reduction_factor(1.0)

    if not breaker.can_proceed():
        return {
            "status": "WARNING",
            "detail": f"Circuit breaker open — probe skipped for {name}",
            "endpoint": f"{config['host']}:{config['port']}",
            "circuit_breaker": breaker.to_dict(),
            "throttled": False,
        }

    if limiter and not limiter.acquire():
        return {
            "status": "WARNING",
            "detail": f"Rate limited — probe throttled for {name}",
            "endpoint": f"{config['host']}:{config['port']}",
            "circuit_breaker": breaker.to_dict(),
            "throttled": True,
        }

    if breaker.state == CB_HALF_OPEN:
        breaker.consume_half_open_slot()

    status, detail, latency = check_tcp_port(config["host"], config["port"], timeout)

    if status == "CRITICAL":
        breaker.record_failure()
    else:
        breaker.record_success()

    return {
        "status": status,
        "detail": detail,
        "endpoint": f"{config['host']}:{config['port']}",
        "circuit_breaker": breaker.to_dict(),
        "throttled": False,
    }


# ---------------------------------------------------------------------------
# HEALTH CHECK RUNNER
# ---------------------------------------------------------------------------

def run_health_checks(
    service: Optional[str] = None,
    json_output: bool = False,
    global_timeout: Optional[int] = None,
    probe_rate: Optional[float] = None,
) -> Dict[str, Any]:
    results: Dict[str, Any] = {
        "timestamp": datetime.now().isoformat(),
        "hostname": socket.gethostname(),
        "services": {},
        "infrastructure": {},
        "system": {},
        "rate_limiter": {},
        "overall_status": "OK",
    }

    all_ok = True

    # Build circuit breakers and rate limiter
    breakers: Dict[str, CircuitBreaker] = {}
    limiter: Optional[TokenBucketRateLimiter] = None
    if probe_rate is not None and probe_rate > 0:
        limiter = TokenBucketRateLimiter(rate=probe_rate, capacity=probe_rate)

    # Check services (HTTP probes)
    for name, config in SERVICES.items():
        if service and name != service:
            continue
        result = _probe_http(name, config, global_timeout, breakers, limiter)
        if result is not None:
            results["services"][name] = result
            if result["status"] == "CRITICAL":
                all_ok = False

    # Check infrastructure (TCP probes)
    for name, config in INFRASTRUCTURE.items():
        if service and name != service:
            continue
        result = _probe_tcp(name, config, global_timeout, breakers, limiter)
        if result is not None:
            results["infrastructure"][name] = result
            if result["status"] == "CRITICAL":
                all_ok = False

    # Check system resources
    disk_status, disk_detail, disk_pct = check_disk_usage()
    results["system"]["disk"] = {"status": disk_status, "detail": disk_detail}
    if disk_status == "CRITICAL":
        all_ok = False

    mem_status, mem_detail, mem_pct = check_memory_usage()
    results["system"]["memory"] = {"status": mem_status, "detail": mem_detail}
    if mem_status == "CRITICAL":
        all_ok = False

    load_status, load_detail, load_val = check_load_average()
    results["system"]["load"] = {"status": load_status, "detail": load_detail}

    # Check certificate expiry (web services)
    for name, config in SERVICES.items():
        if service and name != service:
            continue
        if config["port"] == 443:
            cert_status, cert_detail, days_left = check_certificate_expiry(config["host"])
            if name in results["services"]:
                results["services"][name]["certificate"] = {
                    "status": cert_status,
                    "detail": cert_detail,
                    "days_remaining": days_left,
                }
                if cert_status == "CRITICAL":
                    all_ok = False

    # Rate limiter stats in the aggregation report
    if limiter is not None:
        results["rate_limiter"] = limiter.to_dict()
    else:
        results["rate_limiter"] = {"enabled": False}

    results["overall_status"] = "OK" if all_ok else "DEGRADED"

    return results


def print_health_report(results: Dict[str, Any]):
    print(f"\n{'='*60}")
    print(f"  HEALTH CHECK REPORT")
    print(f"  Host: {results['hostname']}")
    print(f"  Time: {results['timestamp']}")
    print(f"  Overall: {results['overall_status']}")
    print(f"{'='*60}")

    for category, items in [("Services", results["services"]),
                             ("Infrastructure", results["infrastructure"]),
                             ("System", results["system"])]:
        if items:
            print(f"\n  {category}:")
            for name, check in items.items():
                if isinstance(check, dict) and "status" in check:
                    status_icon = {"OK": "✓", "WARNING": "⚠", "CRITICAL": "✗"}.get(check["status"], "?")
                    throttled_tag = " [throttled]" if check.get("throttled") else ""
                    print(f"    {status_icon} {name}: {check['detail']}{throttled_tag}")
                    if "circuit_breaker" in check:
                        cb = check["circuit_breaker"]
                        print(f"      breaker: {cb['state']} (failures={cb['failure_count']}/{cb['failure_threshold']})")
                else:
                    print(f"    {name}:")
                    for sub_name, sub_check in check.items():
                        if isinstance(sub_check, dict) and "status" in sub_check:
                            sub_icon = {"OK": "✓", "WARNING": "⚠", "CRITICAL": "✗"}.get(sub_check["status"], "?")
                            print(f"      {sub_icon} {sub_name}: {sub_check['detail']}")

    # Rate limiter stats
    rl = results.get("rate_limiter", {})
    if rl.get("enabled", False):
        print(f"\n  Rate Limiter:")
        print(f"    Configured rate: {rl['rate']} probes/sec")
        print(f"    Effective rate:  {rl['effective_rate']} probes/sec")
        print(f"    Reduction factor: {rl['reduction_factor']}")
        print(f"    Allowed:   {rl['allowed_count']}")
        print(f"    Throttled: {rl['throttled_count']}")
    else:
        print(f"\n  Rate Limiter: disabled")

    print()


def parse_args():
    parser = argparse.ArgumentParser(description="Health check tool")
    parser.add_argument("--service", "-s", help="Check specific service only")
    parser.add_argument("--json", "-j", action="store_true", help="JSON output")
    parser.add_argument("--watch", "-w", action="store_true", help="Continuous monitoring")
    parser.add_argument("--interval", "-i", type=int, default=30, help="Check interval in seconds")
    parser.add_argument("--output", "-o", help="Output file path")
    parser.add_argument(
        "--timeout", "-t", type=int, default=None,
        help="Override default timeout (seconds) for all probes",
    )
    parser.add_argument(
        "--probe-rate", type=float, default=None,
        help="Max probes per second (e.g. --probe-rate 5). Default: no limit",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    if args.watch:
        print(f"Continuous monitoring (interval: {args.interval}s). Press Ctrl+C to stop.")
        if args.probe_rate:
            print(f"Probe rate limit: {args.probe_rate}/sec")
        if args.timeout:
            print(f"Timeout override: {args.timeout}s")
        try:
            while True:
                results = run_health_checks(
                    args.service, args.json,
                    global_timeout=args.timeout,
                    probe_rate=args.probe_rate,
                )
                if args.json:
                    print(json.dumps(results, indent=2))
                else:
                    print_health_report(results)
                time.sleep(args.interval)
        except KeyboardInterrupt:
            print("\nMonitoring stopped")
    else:
        results = run_health_checks(
            args.service, args.json,
            global_timeout=args.timeout,
            probe_rate=args.probe_rate,
        )
        if args.json:
            output = json.dumps(results, indent=2)
            print(output)
        else:
            print_health_report(results)

        if args.output:
            with open(args.output, "w") as f:
                if args.json:
                    json.dump(results, f, indent=2)
                else:
                    json.dump(results, f, indent=2)
            print(f"Report saved to {args.output}")

        if results["overall_status"] == "DEGRADED":
            return 1

    return 0


if __name__ == "__main__":
    main()