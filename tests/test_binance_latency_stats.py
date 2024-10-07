import pytest
import asyncio
import logging
import numpy as np
from ..binance_latency_stats import filter_outliers, calculate_delays, perform_statistical_tests, WebSocketMessage


# Sample data for testing outlier filtering
delays_with_outliers = [
    [0.5, 0.6, 0.7, 3.0],  # Outlier present
    [0.4, 0.6, 0.5, 0.7],
    [0.2, 0.3, 0.4, 0.5, 10.0]  # Outlier present
]

expected_filtered_delays = [
    [0.5, 0.6, 0.7],  # Outlier removed
    [0.4, 0.6, 0.5, 0.7],  # No outliers
    [0.2, 0.3, 0.4, 0.5]  # Outlier removed
]

# Parametrize multiple scenarios for robustness in filter_outliers tests
@pytest.mark.parametrize("delays, expected", [
    (delays_with_outliers, expected_filtered_delays),  # Case with valid outliers
    ([], []),  # Edge case: Empty input
    ([[0.5]], [[0.5]]),  # Edge case: Single-element list, no outliers
])
def test_filter_outliers(delays, expected):
    """
    Test the filter_outliers function with multiple cases:
    - Outliers
    - Empty input
    - Single-element list
    """
    filtered = filter_outliers(delays)
    assert filtered == expected, f"Outliers were not filtered correctly. Expected {expected}, got {filtered}"


@pytest.mark.asyncio
async def test_calculate_delays():
    """
    Test the calculate_delays function using simulated message queues and WebSocketMessages.
    """
    # Simulate message queues
    message_queues = [asyncio.Queue(), asyncio.Queue()]

    # Add dummy WebSocketMessages to the queues
    ws_message1 = WebSocketMessage(update_id=1, event_time=1632429200.0, data={'u': 1, 'E': 1632429200000})
    ws_message2 = WebSocketMessage(update_id=2, event_time=1632429205.0, data={'u': 2, 'E': 1632429205000})

    await message_queues[0].put((1632429201.0, ws_message1))
    await message_queues[1].put((1632429206.0, ws_message2))

    # Call calculate_delays
    delays = calculate_delays(message_queues)

    # Validate the calculated delays for both queues
    assert len(delays) == 2, f"Expected 2 delay lists, got {len(delays)}"
    assert delays[0][0] == 1.0, f"Incorrect delay for first message, expected 1.0, got {delays[0][0]}"
    assert delays[1][0] == 1.0, f"Incorrect delay for second message, expected 1.0, got {delays[1][0]}"


def test_perform_statistical_tests_anova(caplog):
    """
    Test perform_statistical_tests when ANOVA or Kruskal-Wallis should be performed depending on normality.
    """
    # Normally distributed sample delay data for performing statistical tests
    delays = [
        np.random.normal(0.5, 0.1, 100).tolist(),  # 100 points, normal distribution
        np.random.normal(0.5, 0.1, 100).tolist(),
        np.random.normal(0.5, 0.1, 100).tolist()
    ]

    # Capture logs for assertion
    with caplog.at_level(logging.INFO):
        perform_statistical_tests(delays)

    # Check if ANOVA or Kruskal-Wallis was performed
    if any("ANOVA: F-value" in record.message for record in caplog.records):
        # ANOVA and Bartlett's should be logged if data is detected as normal
        assert any("ANOVA: F-value" in record.message for record in caplog.records), "ANOVA test result not logged"
        assert any("Bartlett's test: statistic" in record.message for record in caplog.records), "Bartlett test result not logged"
    else:
        # Kruskal-Wallis should be logged if data is detected as non-normal
        assert any("Kruskal-Wallis test: statistic" in record.message for record in caplog.records), "Kruskal-Wallis test result not logged"



def test_perform_statistical_tests_kruskal_wallis(caplog):
    """
    Test perform_statistical_tests when Kruskal-Wallis should be performed (non-normally distributed data).
    """
    # Non-normally distributed sample delay data (uniform distribution)
    delays = [
        np.random.uniform(0.5, 1.0, 100).tolist(),  # 100 points, uniform distribution (non-normal)
        np.random.uniform(0.5, 1.0, 100).tolist(),
        np.random.uniform(0.5, 1.0, 100).tolist()
    ]

    # Capture logs for assertion
    with caplog.at_level(logging.INFO):
        perform_statistical_tests(delays)

    # Validate that Kruskal-Wallis test was performed and logged
    assert any("Kruskal-Wallis test: statistic" in record.message for record in caplog.records), "Kruskal-Wallis test result not logged"
    assert not any("ANOVA: F-value" in record.message for record in caplog.records), "ANOVA test should not be logged for non-normal data"


# Additional edge cases for perform_statistical_tests
@pytest.mark.parametrize("delays", [
    ([[0.1], [0.2], [0.3]]),  # Single-value delay lists (too few data points)
    ([[0.1, 0.2], [0.1, 0.2], [0.1, 0.2]]),  # Small identical lists (no variance)
])
def test_perform_statistical_tests_edge_cases(delays, caplog):
    """
    Test perform_statistical_tests with edge cases: single-value lists and identical lists.
    """
    with caplog.at_level(logging.INFO):
        perform_statistical_tests(delays)

    # Verify that a warning is logged for insufficient data
    assert any("Insufficient data for statistical tests" in record.message for record in caplog.records), "Expected warning for insufficient data not logged"


# Fixture to simulate setup for WebSocketMessage
@pytest.fixture
def websocket_message():
    """
    Pytest fixture to create a reusable WebSocketMessage instance for other tests.
    """
    return WebSocketMessage(update_id=1, event_time=1632429200.0, data={'u': 1, 'E': 1632429200000})


@pytest.mark.asyncio
async def test_calculate_delays_with_fixture(websocket_message):
    """
    Test calculate_delays using a fixture for WebSocketMessage.
    """
    message_queue = asyncio.Queue()
    await message_queue.put((1632429201.0, websocket_message))

    delays = calculate_delays([message_queue])

    assert delays[0][0] == 1.0, f"Expected delay of 1.0, got {delays[0][0]}"

