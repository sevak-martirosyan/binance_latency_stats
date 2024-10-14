
# Binance Futures WebSocket Data Collection & Latency Analysis

This project connects to Binance USDT-margined Futures market data streams via WebSockets to analyze the latency of updates across multiple connections. The goal is to compare the delay in receiving updates from the same stream (BTCUSDT@bookTicker) through multiple WebSocket connections and statistically analyze the distribution of these delays.

## Overview

The project:
- Establishes **5 WebSocket connections** to Binance Futures WebSocket API.
- Subscribes each connection to the `BTCUSDT@bookTicker` stream.
- Collects incoming messages from all connections over **1 minute**.
- Measures the **latency** for each message, specifically the delay between the time the message was generated and the time it was received.
- Analyzes the distribution of latencies for each connection by:
  - Plotting latency distribution functions for all connections on a single graph.
  - Identifying **"fast" updates**, where a connection receives a new `updateId` before others.
  - Calculating the **proportion** of fast updates for each connection.

## Key Goals
1. **Latency Distribution Analysis**: Compare the delay in receiving updates from different WebSocket connections.
2. **Performance Analysis**: Evaluate the performance of each connection by determining which one consistently receives updates the fastest.
3. **Statistical Testing**: Perform statistical tests to check for significant differences in the **mean** and **standard deviation** of latency across connections.

## Technical Approach
1. **WebSocket Setup**:
   - The program establishes 5 WebSocket connections to Binance's futures data endpoint.
   - Each connection subscribes to the `BTCUSDT@bookTicker` stream.

2. **Data Collection**:
   - Messages from each WebSocket connection are collected for 1 minute.
   - For each message, the `updateId` and the timestamps are recorded.

3. **Latency Calculation**:
   - The delay for each message is calculated as the difference between the current time and the timestamp of the message.
   - The latency data is stored for further analysis.

4. **Latency Distribution**:
   - The latency distribution for each WebSocket connection is plotted on a single graph.
   - The distribution functions help visualize the difference in delay between the connections.

5. **Statistical Analysis**:
   - We perform **statistical tests** to check if the mean and standard deviation of latencies are significantly different across connections.
   - These tests will help us determine if one connection is statistically faster than the others.

6. **Fast Update Identification**:
   - The first connection to receive a new `updateId` is marked as the **fast** connection for that update.
   - The proportion of fast updates is calculated for each connection to evaluate its performance.

## Conclusion
Based on the collected data and statistical analysis, the project provides insights into whether there are significant differences in delay between the WebSocket connections. Several potential reasons for these differences are considered, including:
- **Network Latency**: Different network paths and routing may introduce varying delays.
- **WebSocket Connection Handling**: Differences in how WebSocket connections are handled by the client and server could affect the speed of message delivery.
- **Server Load**: Binance servers may route requests through different infrastructure, leading to varied latencies.

## Installation & Usage

1. **Install Requirements**:
   Make sure you have Python 3.10 installed. 
   ```bash
   python -m venv venv
   . venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Run tests**:
   ```bash
   pytest -v --capture=tee-sys
   ```

3. **Run the Script**:
   The script will connect to the Binance WebSocket API, collect data, and generate the analysis.
   ```bash
   python binance_latency_analysis.py
   ```

3. **Visualize the Results**:
   After the data is collected and processed, a graph showing the latency distribution for each connection will be generated, and the results of the statistical tests will be displayed. The graph also saved as combined_delay_analysis.png file in the program root directory. 
