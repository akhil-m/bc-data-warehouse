"""Pure functions for retry logic with exponential backoff."""


def generate_retry_delays(max_retries=20, initial_delay=1.0, multiplier=2.0, max_delay=30.0):
    """
    Generate exponential backoff delay schedule.

    Args:
        max_retries: Maximum number of retry attempts
        initial_delay: Starting delay in seconds
        multiplier: Factor to multiply delay by each iteration
        max_delay: Maximum delay cap in seconds

    Returns:
        List of delays in seconds: [1, 2, 4, 8, 16, 30, 30, ...]
        Total wait ~481s for default parameters (20 retries)
    """
    delays = []
    delay = initial_delay
    for _ in range(max_retries):
        delays.append(min(delay, max_delay))
        delay *= multiplier
    return delays
