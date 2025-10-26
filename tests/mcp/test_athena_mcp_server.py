"""Tests for pure core functions for MCP retry logic."""

from src.mcp.retry import generate_retry_delays


class TestGenerateRetryDelays:
    """Test exponential backoff delay generation."""

    def test_default_parameters(self):
        """Should generate exponential backoff with defaults."""
        delays = generate_retry_delays()
        assert len(delays) == 20
        assert delays[0] == 1.0
        assert delays[1] == 2.0
        assert delays[2] == 4.0
        assert delays[3] == 8.0
        assert delays[4] == 16.0

    def test_exponential_growth(self):
        """Should double delay each iteration until max_delay."""
        delays = generate_retry_delays(max_retries=10, initial_delay=1.0, multiplier=2.0, max_delay=100.0)
        assert delays == [1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 100.0, 100.0, 100.0]

    def test_max_delay_cap(self):
        """Should cap delays at max_delay."""
        delays = generate_retry_delays(max_retries=10, initial_delay=1.0, multiplier=2.0, max_delay=30.0)
        # 1, 2, 4, 8, 16, 32->30, 64->30, ...
        assert all(d <= 30.0 for d in delays)
        assert delays[5] == 30.0  # First capped value
        assert delays[-1] == 30.0  # All subsequent values capped

    def test_custom_initial_delay(self):
        """Should start with custom initial delay."""
        delays = generate_retry_delays(max_retries=5, initial_delay=0.5, multiplier=2.0, max_delay=100.0)
        assert delays[0] == 0.5
        assert delays[1] == 1.0
        assert delays[2] == 2.0

    def test_custom_multiplier(self):
        """Should use custom multiplier for growth."""
        delays = generate_retry_delays(max_retries=5, initial_delay=1.0, multiplier=3.0, max_delay=100.0)
        assert delays == [1.0, 3.0, 9.0, 27.0, 81.0]

    def test_zero_retries(self):
        """Should return empty list for zero retries."""
        delays = generate_retry_delays(max_retries=0)
        assert delays == []

    def test_one_retry(self):
        """Should return single delay for one retry."""
        delays = generate_retry_delays(max_retries=1, initial_delay=2.0)
        assert delays == [2.0]

    def test_total_wait_time(self):
        """Should calculate correct total wait time."""
        delays = generate_retry_delays(max_retries=20)
        # 1 + 2 + 4 + 8 + 16 + 30*15 = 481s
        assert sum(delays) == 481.0

    def test_all_delays_capped_immediately(self):
        """Should handle case where initial_delay >= max_delay."""
        delays = generate_retry_delays(max_retries=5, initial_delay=50.0, multiplier=2.0, max_delay=30.0)
        assert all(d == 30.0 for d in delays)

    def test_fractional_delays(self):
        """Should handle fractional delay values."""
        delays = generate_retry_delays(max_retries=3, initial_delay=0.25, multiplier=2.0, max_delay=10.0)
        assert delays == [0.25, 0.5, 1.0]

    def test_multiplier_of_one(self):
        """Should handle multiplier of 1 (constant delays)."""
        delays = generate_retry_delays(max_retries=5, initial_delay=2.0, multiplier=1.0, max_delay=100.0)
        assert all(d == 2.0 for d in delays)
