from src.reliability.outbox_relay import backoff_ceiling


def test_backoff_ceiling_is_exponential_and_bounded_for_large_attempts() -> None:
    assert backoff_ceiling(attempt=1, base_seconds=2, cap_seconds=60) == 2
    assert backoff_ceiling(attempt=4, base_seconds=2, cap_seconds=60) == 16
    assert backoff_ceiling(attempt=1_000_000, base_seconds=2, cap_seconds=60) == 60
