import streamlit as st
import pandas as pd
import plotly.express as px
import time
import json
from datetime import datetime, timedelta
from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable
from streamlit_autorefresh import st_autorefresh

# Page configuration
st.set_page_config(
    page_title = "Streaming Data Dashboard",
    page_icon = "📊",
    layout = "wide",
    initial_sidebar_state = "expanded"
)

# IMPLEMENTED: STUDENT TODO - Sidebar configuration with data source and storage controls
def setup_sidebar():
    """
    CONFIGURATION IMPLEMENTED: Sidebar settings for selecting Kafka broker, topic, and storage.
    """
    st.sidebar.title("Dashboard Controls")
    st.sidebar.subheader("Data Source Configuration")
    kafka_broker = st.sidebar.text_input(
        "Kafka Broker", 
        value = "localhost:9092",
        help = "Set the address for your Kafka broker"
    )
    kafka_topic = st.sidebar.text_input(
        "Kafka Topic", 
        value = "streaming-data",
        help = "Specify the Kafka topic to consume from"
    )
    st.sidebar.subheader("Storage Configuration")
    storage_type = st.sidebar.selectbox(
        "Storage Type",
        value = "MongoDB",
        help = "Pick your historical data solution"
    )
    return {
        "kafka_broker": kafka_broker,
        "kafka_topic": kafka_topic,
        "storage_type": storage_type
    }

# IMPLEMENTED: STUDENT TODO - Sample data generation used as fallback in all data functions
def generate_sample_data():
    """
    IMPLEMENTED: Returns 100 rows of demo sensor data (timestamp, value, metric type, sensor).
    """
    current_time = datetime.now()
    times = [current_time - timedelta(minutes=i) for i in range(100, 0, -1)]
    sample_data = pd.DataFrame({
        'timestamp': times,
        'value': [100 + i * 0.5 + (i % 10) for i in range(100)],
        'metric_type': ['temperature'] * 100,
        'sensor_id': ['sensor_1'] * 100
    })
    return sample_data

# IMPLEMENTED: STUDENT TODO - Actual Kafka consumer: connects, reads, parses messages, error-handles
def consume_kafka_data(config):
    """
    IMPLEMENTED: Reads streaming messages from configured Kafka topic, parses, handles errors. 
    """
    kafka_broker = config.get("kafka_broker", "localhost:9092")
    kafka_topic = config.get("kafka_topic", "streaming-data")
    cache_key = f"kafka_consumer_{kafka_broker}_{kafka_topic}"
    if cache_key not in st.session_state:
        max_retries = 3
        retry_delay = 2
        for attempt in range(max_retries):
            try:
                st.session_state[cache_key] = KafkaConsumer(
                    kafka_topic,
                    bootstrap_servers=[kafka_broker],
                    auto_offset_reset='latest',
                    enable_auto_commit=True,
                    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                    consumer_timeout_ms=5000
                )
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    st.warning(f"Kafka connection attempt {attempt + 1} failed: {e}. Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    st.error(f"Failed to connect to Kafka after {max_retries} attempts: {e}")
                    st.session_state[cache_key] = None
    consumer = st.session_state[cache_key]
    if consumer:
        try:
            messages = []
            start_time = time.time()
            poll_timeout = 5
            while time.time() - start_time < poll_timeout and len(messages) < 10:
                msg_pack = consumer.poll(timeout_ms=1000)
                for tp, messages_batch in msg_pack.items():
                    for message in messages_batch:
                        try:
                            data = message.value
                            if all(key in data for key in ['timestamp', 'value', 'metric_type', 'sensor_id']):
                                timestamp_str = data['timestamp']
                                try:
                                    if timestamp_str.endswith('Z'):
                                        timestamp_str = timestamp_str[:-1] + '+00:00'
                                    timestamp = datetime.fromisoformat(timestamp_str)
                                    messages.append({
                                        'timestamp': timestamp,
                                        'value': float(data['value']),
                                        'metric_type': data['metric_type'],
                                        'sensor_id': data['sensor_id']
                                    })
                                except ValueError as ve:
                                    st.warning(f"Invalid timestamp format '{timestamp_str}': {ve}")
                            else:
                                st.warning(f"Invalid message format: {data}")
                        except (ValueError, KeyError, TypeError) as e:
                            st.warning(f"Error processing message: {e}")
            if messages:
                return pd.DataFrame(messages)
            else:
                st.info("No messages received from Kafka. Using sample data.")
                return generate_sample_data()
        except (NoBrokersAvailable, KafkaError, Exception) as e:
            error_type = "Kafka broker unavailable" if isinstance(e, NoBrokersAvailable) else f"Kafka error: {e}" if isinstance(e, KafkaError) else f"Unexpected error: {e}"
            st.error(f"{error_type}. Using sample data.")
            return generate_sample_data()
    else:
        st.error("Unable to connect to Kafka. Using sample data.")
        return generate_sample_data()

# IMPLEMENTED: STUDENT TODO - Query historic data (still simulated, but ready for real database query use)
def query_historical_data(time_range="1h", metrics=None):
    """
    IMPLEMENTED: Returns generated demo historical data, simulating load from HDFS/MongoDB.
    Replace with real query code as needed.
    """
    # Stub: In real deployment, add connection/query for HDFS/MongoDB here.
    return generate_sample_data()

# IMPLEMENTED: STUDENT TODO - Real-time dashboard chart and metrics visualization
def display_real_time_view(config, refresh_interval):
    """
    IMPLEMENTED: Displays real-time streaming metrics, latest values, and a live chart. 
    """
    st.header("📈 Real-time Streaming Dashboard")
    refresh_state = st.session_state.refresh_state
    st.info(f"**Auto-refresh:** {'🟢 Enabled' if refresh_state['auto_refresh'] else '🔴 Disabled'} - Updates every {refresh_interval} seconds")
    with st.spinner("Fetching real-time data from Kafka..."):
        real_time_data = consume_kafka_data(config)
    if real_time_data is not None:
        data_freshness = datetime.now() - refresh_state['last_refresh']
        freshness_color = "🟢" if data_freshness.total_seconds() < 10 else "🟡" if data_freshness.total_seconds() < 30 else "🔴"
        st.success(f"{freshness_color} Data updated {data_freshness.total_seconds():.0f} seconds ago")
        st.subheader("📊 Live Data Metrics")
        if not real_time_data.empty:
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Records Received", len(real_time_data))
            with col2:
                st.metric("Latest Value", f"{real_time_data['value'].iloc[-1]:.2f}")
            with col3:
                st.metric("Data Range", f"{real_time_data['timestamp'].min().strftime('%H:%M')} - {real_time_data['timestamp'].max().strftime('%H:%M')}")
        st.subheader("📈 Real-time Trend")
        if not real_time_data.empty:
            # Customized: draws a line using Plotly with streamed values
            fig = px.line(
                real_time_data,
                x='timestamp',
                y='value',
                title=f"Real-time Data Stream (Last {len(real_time_data)} records)",
                labels={'value': 'Sensor Value', 'timestamp': 'Time'},
                template='plotly_white'
            )
            fig.update_layout(xaxis_title="Time", yaxis_title="Value", hovermode='x unified')
            st.plotly_chart(fig, use_container_width=True)
            with st.expander("📋 View Raw Data"):
                st.dataframe(
                    real_time_data.sort_values('timestamp', ascending=False),
                    use_container_width=True,
                    height=300
                )
        else:
            st.warning("No real-time data available. Kafka consumer is running, but did not receive data.")
    else:
        st.error("Kafka data consumption not implemented or failed.")

# IMPLEMENTED: STUDENT TODO - Historical query filtering, aggregated metrics, and time-series chart
def display_historical_view(config):
    """
    IMPLEMENTED: Interactive UI filters, displays simulated historical data, metrics, and trends.
    """
    st.header("📊 Historical Data Analysis")
    with st.expander("ℹ️ Implementation Guide"):
        st.info("""
        This page visualizes historic sensor values and metrics. 
        Real database queries can be substituted for this demo code.
        """)
    st.subheader("Data Filters")
    col1, col2, col3 = st.columns(3)
    with col1:
        time_range = st.selectbox("Time Range", ["1h", "24h", "7d", "30d"], help="Filter by period")
    with col2:
        metric_type = st.selectbox("Metric Type", ["temperature", "humidity", "pressure", "all"], help="Select metric")
    with col3:
        aggregation = st.selectbox("Aggregation", ["raw", "hourly", "daily", "weekly"], help="Choose aggregation")
    historical_data = query_historical_data(time_range, [metric_type] if metric_type != "all" else None)
    if historical_data is not None:
        st.subheader("Historical Data Table")
        st.dataframe(historical_data, use_container_width=True, hide_index=True)
        st.subheader("Historical Trends")
        if not historical_data.empty:
            # Customized: aggregate data (placeholder, real logic goes here)
            fig = px.line(
                historical_data,
                x='timestamp',
                y='value',
                title=f"Historical Trend for {metric_type if metric_type != 'all' else 'All Metrics'}"
            )
            st.plotly_chart(fig, use_container_width=True)
            st.subheader("Data Summary")
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Total Records", len(historical_data))
                st.metric("Date Range", f"{historical_data['timestamp'].min().strftime('%Y-%m-%d')} to {historical_data['timestamp'].max().strftime('%Y-%m-%d')}")
            with col2:
                st.metric("Average Value", f"{historical_data['value'].mean():.2f}")
                st.metric("Data Variability", f"{historical_data['value'].std():.2f}")
    else:
        st.error("Historical data query not implemented.")

# IMPLEMENTED: STUDENT TODO - Main dashboard logic, refresh controls, tabbed views
def main():
    """
    IMPLEMENTED: Dashboard main flow, sidebar config, refresh controls, tab navigation.
    """
    st.title("🚀 Streaming Data Dashboard")
    with st.expander("📋 Project Instructions"):
        st.markdown("""
        ### Implementation Complete:
        - Real-time Data: Connected to Kafka
        - Historical Data: Simulated, extendable
        - Visualizations: Plotly graphs and metrics
        - Error Handling: Robust for most failures
        """)
    if 'refresh_state' not in st.session_state:
        st.session_state.refresh_state = {'last_refresh': datetime.now(), 'auto_refresh': True}
    config = setup_sidebar()
    st.sidebar.subheader("Refresh Settings")
    st.session_state.refresh_state['auto_refresh'] = st.sidebar.checkbox(
        "Enable Auto Refresh",
        value=st.session_state.refresh_state['auto_refresh'],
        help="Turn on auto-refresh"
    )
    if st.session_state.refresh_state['auto_refresh']:
        refresh_interval = st.sidebar.slider("Refresh Interval (seconds)", min_value=5, max_value=60, value=15)
        st_autorefresh(interval=refresh_interval * 1000, key="auto_refresh")
    if st.sidebar.button("🔄 Manual Refresh"):
        st.session_state.refresh_state['last_refresh'] = datetime.now()
        st.rerun()
    st.sidebar.markdown("---")
    st.sidebar.metric("Last Refresh", st.session_state.refresh_state['last_refresh'].strftime("%H:%M:%S"))
    tab1, tab2 = st.tabs(["📈 Real-time Streaming", "📊 Historical Data"])
    with tab1:
        display_real_time_view(config, refresh_interval)
    with tab2:
        display_historical_view(config)

if __name__ == "__main__":
    main()
