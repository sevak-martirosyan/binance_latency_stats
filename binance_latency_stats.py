import json
import time
import yaml
import signal
import asyncio
import logging
import websockets
import numpy as np
from scipy import stats
import matplotlib.pyplot as plt
from dataclasses import dataclass
from typing import Dict, List, Any


# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')


def load_config(config_path: str) -> Dict[str, Any]:
    """
    Loads configuration from a YAML file and handles errors.
    
    :param config_path: Path to the configuration file
    :return: Configuration dictionary
    """
    try:
        with open(config_path, 'r') as config_file:
            return yaml.safe_load(config_file)
    except FileNotFoundError:
        logging.error(f"Configuration file not found: {config_path}")
        raise
    except yaml.YAMLError as e:
        logging.error(f"Error parsing YAML config file: {e}")
        raise


class Config:
    """
    Configuration class to store the settings read from the YAML file.
    Ensures that required keys are present in the configuration.
    """
    def __init__(self, config_path: str):
        self.config = load_config(config_path)
        required_keys = ['BINANCE_WS_URL', 'STREAM_NAME', 'NUM_CONNECTIONS', 'COLLECTION_TIME', 'QUEUE_MAX_SIZE']
        missing_keys = [key for key in required_keys if key not in self.config]
        if missing_keys:
            raise KeyError(f"Missing required configuration keys: {', '.join(missing_keys)}")
        
        self.BINANCE_WS_URL = self.config['BINANCE_WS_URL']
        self.STREAM_NAME = self.config['STREAM_NAME']
        self.NUM_CONNECTIONS = self.config['NUM_CONNECTIONS']
        self.COLLECTION_TIME = self.config['COLLECTION_TIME']
        self.QUEUE_MAX_SIZE = self.config['QUEUE_MAX_SIZE']
        self.SEMAPHORE_LIMIT = self.config.get('SEMAPHORE_LIMIT', 10)  # Default to 10


@dataclass
class WebSocketMessage:
    update_id: int
    event_time: float
    data: Dict[str, Any]


async def retry_backoff(func, retries=5, base=2, jitter=True, max_backoff=60):
    """
    Retries a function with exponential backoff and optional jitter.
    
    :param func: The function to retry
    :param retries: Maximum number of retries
    :param base: Base delay for backoff
    :param jitter: Add randomness to delay if True
    :param max_backoff: Maximum delay for backoff
    :return: Result of the function call
    """
    for attempt in range(retries):
        try:
            return await func()
        except (websockets.exceptions.ConnectionClosedError, asyncio.TimeoutError, OSError) as e:
            backoff_time = min(base ** attempt + (np.random.uniform(0, 1) if jitter else 0), max_backoff)
            logging.error(f"Error: {e}. Retrying in {backoff_time:.2f}s (Attempt {attempt + 1}/{retries})")
            await asyncio.sleep(backoff_time)
    logging.error(f"Max retries reached. Exiting.")
    raise Exception("Max retries reached.")


async def connect_and_subscribe(connection_id: int, message_queue: asyncio.Queue, config: Config, semaphore: asyncio.Semaphore) -> None:
    """
    Connect to WebSocket and subscribe to the stream. Implements retry with backoff.
    
    :param connection_id: Unique identifier for each connection
    :param message_queue: Queue to store incoming WebSocket messages
    :param config: Configuration object containing settings
    :param semaphore: Semaphore to limit concurrent connections
    """
    retry_attempts = 5
    async with semaphore:
        while retry_attempts > 0:
            websocket = await retry_backoff(lambda: websockets.connect(config.BINANCE_WS_URL, timeout=10))
            try:
                subscribe_message = json.dumps({
                    "method": "SUBSCRIBE",
                    "params": [config.STREAM_NAME],
                    "id": connection_id
                })
                await websocket.send(subscribe_message)
                start_time = time.time()

                while time.time() - start_time < config.COLLECTION_TIME:
                    try:
                        response = await asyncio.wait_for(websocket.recv(), timeout=5)
                        message = json.loads(response)

                        # Handle subscription acknowledgment messages
                        if 'result' in message and message['result'] is None:
                            logging.info(f"Subscription acknowledgment received for connection {connection_id}.")
                            continue

                        # Ensure that the message contains required fields 'u' and 'E'
                        if 'u' in message and 'E' in message:
                            if message_queue.qsize() < config.QUEUE_MAX_SIZE:
                                ws_message = WebSocketMessage(
                                    update_id=message['u'],
                                    event_time=message['E'] / 1000,
                                    data=message
                                )
                                await message_queue.put((time.time(), ws_message))
                            else:
                                logging.warning(f"Queue full for connection {connection_id}, discarding message.")
                        else:
                            logging.warning(f"Message missing expected keys for connection {connection_id}: {message}")

                    except json.JSONDecodeError as e:
                        logging.error(f"JSON decode error in connection {connection_id}: {e}")
                    except asyncio.TimeoutError:
                        logging.warning(f"Connection {connection_id}: No message received within timeout period.")

                logging.info(f"Connection {connection_id} finished collecting data.")
                return
            except websockets.exceptions.ConnectionClosedError as e:
                logging.error(f"WebSocket connection closed for connection {connection_id}. Retrying...")
                retry_attempts -= 1
                continue
            finally:
                await websocket.close()

        logging.error(f"Connection {connection_id} failed after {retry_attempts} retries.")


async def gather_data(config: Config) -> List[asyncio.Queue]:
    """
    Manages multiple WebSocket connections concurrently and gathers data in queues.
    
    :param config: Configuration object containing settings
    :return: List of message queues for each connection
    """
    semaphore = asyncio.Semaphore(config.SEMAPHORE_LIMIT)
    message_queues = [asyncio.Queue(maxsize=config.QUEUE_MAX_SIZE) for _ in range(config.NUM_CONNECTIONS)]
    tasks = [connect_and_subscribe(i, message_queues[i], config, semaphore) for i in range(config.NUM_CONNECTIONS)]
    await asyncio.gather(*tasks)
    return message_queues


def filter_outliers(delays: List[List[float]], iqr_threshold: float = 1.5) -> List[List[float]]:
    """
    Filters outliers from delay data using the IQR (Interquartile Range) method.
    
    :param delays: List of delay data from each WebSocket connection
    :param iqr_threshold: Threshold for detecting outliers
    :return: List of filtered delays without outliers
    """
    filtered_delays = []
    
    for delay_list in delays:
        if len(delay_list) > 1:
            Q1 = np.percentile(delay_list, 25)
            Q3 = np.percentile(delay_list, 75)
            IQR = Q3 - Q1
            lower_bound = Q1 - iqr_threshold * IQR
            upper_bound = Q3 + iqr_threshold * IQR
            filtered_list = [x for x in delay_list if lower_bound <= x <= upper_bound]
            filtered_delays.append(filtered_list)
        else:
            filtered_delays.append(delay_list)
    
    return filtered_delays


def process_message(message: WebSocketMessage, previous_update_id: int, delays: List[float], received_time: float) -> int:
    """
    Processes a WebSocket message, computes the delay, and updates the previous update ID.
    
    :param message: WebSocket message received
    :param previous_update_id: The last update ID received
    :param delays: List of delays to append the calculated delay
    :param received_time: Timestamp when the message was received
    :return: Updated previous update ID
    """
    update_id = message.update_id
    if previous_update_id is None or update_id != previous_update_id:
        previous_update_id = update_id
        delays.append(received_time - message.event_time)
    return previous_update_id


def calculate_delays(message_queues: List[asyncio.Queue]) -> List[List[float]]:
    """
    Calculates delays for each WebSocket connection based on messages in the queues.
    
    :param message_queues: List of queues containing WebSocket messages
    :return: List of delays for each connection
    """
    delays = []
    for queue_index, message_queue in enumerate(message_queues):
        if message_queue.empty():
            logging.warning(f"Queue {queue_index} is empty. No messages collected.")
            delays.append([])
            continue
        queue_delays = []
        previous_update_id = None
        while not message_queue.empty():
            received_time, message = message_queue.get_nowait()
            previous_update_id = process_message(message, previous_update_id, queue_delays, received_time)
        delays.append(queue_delays)
    return delays


def plot_combined_delay_analysis(delays: List[List[float]], save_plot: bool = True, show_plot: bool = False) -> None:
    """
    Plots histograms, box plots, and CDFs of the delays for analysis.
    
    :param delays: List of delays for each WebSocket connection
    :param save_plot: Whether to save the plot as a PNG file
    :param show_plot: Whether to display the plot
    """
    fig, axes = plt.subplots(3, 1, figsize=(12, 18))
    plt.style.use('ggplot')

    colors = plt.cm.viridis(np.linspace(0, 1, len(delays)))
    num_bins = min(50, int(np.sqrt(max(len(delay_list) for delay_list in delays if delay_list))))
    
    # Histogram
    axes[0].set_title("Histogram of Delays per Connection", fontsize=20, fontweight='bold')
    axes[0].set_xlabel("Delay (seconds)", fontsize=16)
    axes[0].set_ylabel("Frequency", fontsize=16)

    for i, delay_list in enumerate(delays):
        if delay_list:
            axes[0].hist(delay_list, bins=num_bins, alpha=0.7, color=colors[i], label=f"Connection {i + 1}", edgecolor='black')
            mean_delay = np.mean(delay_list)
            axes[0].axvline(mean_delay, color='red', linestyle='dashed', linewidth=2, label=f'Mean {mean_delay:.2f}s for Connection {i + 1}')

    axes[0].legend(fontsize=12)
    axes[0].grid(True)
    
    # Box plot
    axes[1].boxplot([dl for dl in delays if dl], patch_artist=True,
                    boxprops=dict(facecolor='lightblue', color='blue'),
                    medianprops=dict(color='red', linewidth=2),
                    whiskerprops=dict(color='blue'))
    axes[1].set_title("Box Plot of Delays per Connection", fontsize=20, fontweight='bold')
    axes[1].set_xlabel("Connection", fontsize=16)
    axes[1].set_ylabel("Delay (seconds)", fontsize=16)
    axes[1].grid(True)

    # CDF (Cumulative Distribution Function)
    axes[2].set_title("CDF of Delays per Connection", fontsize=20, fontweight='bold')
    axes[2].set_xlabel("Delay (seconds)", fontsize=16)
    axes[2].set_ylabel("CDF", fontsize=16)

    for i, delay_list in enumerate(delays):
        if delay_list:
            sorted_delays = np.sort(delay_list)
            yvals = np.arange(1, len(sorted_delays) + 1) / len(sorted_delays)
            axes[2].plot(sorted_delays, yvals, label=f"Connection {i + 1}", color=colors[i])

    axes[2].legend(fontsize=12)
    axes[2].grid(True)

    plt.tight_layout()

    if save_plot:
        plt.savefig("combined_delay_analysis.png")
        logging.info("Saved combined delay analysis plot as 'combined_delay_analysis.png'")

    if show_plot:
        plt.show()


# Perform statistical tests (normality test, ANOVA, Kruskal-Wallis) on delay data
def perform_statistical_tests(delays: List[List[float]]) -> None:
    """
    Performs statistical tests on the delay data. Checks for normality and applies appropriate tests.
    
    :param delays: List of delays for each WebSocket connection
    """
    valid_delays = [delay_list for delay_list in delays if len(delay_list) > 5]
    if len(valid_delays) < 2:
        logging.warning("Insufficient data for statistical tests.")
        return

    # Anderson-Darling test for normality
    normality_passed = all(
        stats.anderson(delay_list, dist='norm').statistic < stats.anderson(delay_list, dist='norm').critical_values[2]
        for delay_list in valid_delays
    )

    logging.info(f"Normality test passed: {normality_passed}")

    if normality_passed:
        # ANOVA test for comparing means
        f_value, p_value_anova = stats.f_oneway(*valid_delays)
        logging.info(f"ANOVA: F-value={f_value}, p-value={p_value_anova}")
        
        # Bartlett's test for homogeneity of variances
        stat_bartlett, p_value_bartlett = stats.bartlett(*valid_delays)
        logging.info(f"Bartlett's test: statistic={stat_bartlett}, p-value={p_value_bartlett}")
    else:
        # Kruskal-Wallis test for non-parametric comparisons
        stat_kruskal, p_value_kruskal = stats.kruskal(*valid_delays)
        logging.info(f"Kruskal-Wallis test: statistic={stat_kruskal}, p-value={p_value_kruskal}")


def calculate_fastest_update_ratio(delays: List[List[float]]) -> List[float]:
    """
    Calculates the ratio of fastest updates for each WebSocket connection.
    
    :param delays: List of delays for each WebSocket connection
    :return: List of ratios of fastest updates for each connection
    """
    non_empty_delays = [dl for dl in delays if len(dl) > 0]
    if not non_empty_delays:
        logging.warning("No valid delays for calculating fastest update ratio.")
        return [0] * len(delays)
    
    min_length = min(len(dl) for dl in non_empty_delays)
    trimmed_delays = [dl[:min_length] for dl in non_empty_delays]

    combined_delays = np.array(trimmed_delays).T
    fastest_counts = np.sum(combined_delays == np.min(combined_delays, axis=1)[:, None], axis=0)
    fastest_ratios = fastest_counts / min_length

    return fastest_ratios


def main():
    """
    Main entry point of the program. Gathers WebSocket data, performs delay analysis, and runs statistical tests.
    """
    config = Config("config.yaml")

    loop = asyncio.get_event_loop()

    try:
        # Signal handler for graceful shutdown
        if hasattr(signal, 'SIGINT') and hasattr(signal, 'SIGTERM'):
            try:
                loop.add_signal_handler(signal.SIGINT, loop.stop)
                loop.add_signal_handler(signal.SIGTERM, loop.stop)
            except NotImplementedError:
                logging.warning("Signal handling not supported in this environment.")

        # Gather data and compute delays
        message_queues = loop.run_until_complete(gather_data(config))
        delays = calculate_delays(message_queues)

        # Filter outliers
        delays = filter_outliers(delays)

        # Plot the delay analysis
        plot_combined_delay_analysis(delays, save_plot=True, show_plot=True)

        # Perform statistical tests
        perform_statistical_tests(delays)

        # Calculate fastest update ratio
        fastest_ratios = calculate_fastest_update_ratio(delays)
        for i, ratio in enumerate(fastest_ratios):
            logging.info(f"Connection {i + 1} fastest update ratio: {ratio:.2f}")

    finally:
        loop.close()


if __name__ == "__main__":
    main()

