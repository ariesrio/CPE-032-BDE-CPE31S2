"""
Streaming Data Dashboard Template
STUDENT PROJECT: Big Data Streaming Dashboard

This is a template for students to build a real-time streaming data dashboard.
Students will need to implement the actual data processing, Kafka consumption,
and storage integration.

IMPLEMENT THE TODO SECTIONS
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import time
import json
from datetime import datetime, timedelta, timezone
from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable
from streamlit_autorefresh import st_autorefresh
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

# Page configuration
st.set_page_config(
    page_title="Streaming Data Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

def setup_sidebar():
    """
    COMPLETED TODO: Configure sidebar settings and controls
    Implement any configuration options students might need
    """
    st.sidebar.title("Dashboard Controls")
    
    # COMPLETED TODO: Add configuration options for data sources
    st.sidebar.subheader("Data Source Configuration")
    
    # Placeholder for Kafka configuration
    kafka_broker = st.sidebar.text_input(
        "Kafka Broker", 
        value="localhost:9092",
        help="Configure your Kafka broker address"
    )
    
    kafka_topic = st.sidebar.text_input(
        "Kafka Topic", 
        value="streaming-data",
        help="Specify the Kafka topic to consume from"
    )
    
    # Placeholder for storage configuration
    st.sidebar.subheader("Storage Configuration")
    storage_type = st.sidebar.selectbox(
        "Storage Type",
        ["MongoDB"],
        help="Historical data storage solution"
    )
    
    mongo_uri = st.sidebar.text_input(
        "MongoDB URI",
        value="mongodb+srv://<username>:<password>@<cluster>.mongodb.net/",
        help="MongoDB Atlas connection string (format: mongodb+srv://username:password@cluster.mongodb.net/)"
    )
    
    mongo_db = st.sidebar.text_input(
        "MongoDB Database",
        value="streaming_data",
        help="MongoDB database name"
    )
    
    return {
        "kafka_broker": kafka_broker,
        "kafka_topic": kafka_topic,
        "storage_type": storage_type,
        "mongo_uri": mongo_uri,
        "mongo_db": mongo_db
    }

def generate_sample_data():
    """
    COMPLETED TODO: Replace this with actual data processing
    
    This function generates sample data for demonstration purposes.
    Students should replace this with real data from Kafka and storage systems.
    """
    # Sample data for demonstration - REPLACE WITH REAL DATA
    current_time = datetime.now()
    times = [current_time - timedelta(minutes=i) for i in range(100, 0, -1)]
    
    sample_data = pd.DataFrame({
        'timestamp': times,
        'value': [100 + i * 0.5 + (i % 10) for i in range(100)],
        'metric_type': ['temperature'] * 100,
        'stock_symbol': ['sensor_1'] * 100
    })
    
    return sample_data

def consume_kafka_data(config):
    """
    COMPLETED TODO: Implement actual Kafka consumer
    """
    kafka_broker = config.get("kafka_broker", "localhost:9092")
    kafka_topic = config.get("kafka_topic", "streaming-data")
    
    # Cache Kafka consumer to avoid recreation
    cache_key = f"kafka_consumer_{kafka_broker}_{kafka_topic}"
    if cache_key not in st.session_state:
        # Connection retry logic for Kafka consumer
        max_retries = 2  # Reduced from 3
        retry_delay = 1  # Reduced from 2 seconds
        
        for attempt in range(max_retries):
            try:
                st.session_state[cache_key] = KafkaConsumer(
                    kafka_topic,
                    bootstrap_servers=[kafka_broker],
                    auto_offset_reset='latest',
                    enable_auto_commit=True,
                    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                    consumer_timeout_ms=3000  # Reduced from 5000ms to 3000ms
                )
                break  # Success, break out of retry loop
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
            # Poll for messages
            messages = []
            start_time = time.time()
            poll_timeout = 3  # Reduced from 5 seconds
            
            while time.time() - start_time < poll_timeout and len(messages) < 10:
                msg_pack = consumer.poll(timeout_ms=1000)
                
                for tp, messages_batch in msg_pack.items():
                    for message in messages_batch:
                        try:
                            data = message.value
                            if all(key in data for key in ['timestamp', 'value', 'metric_type', 'sensor_id']):
                                # Robust timestamp parsing for various ISO 8601 formats
                                timestamp_str = data['timestamp']
                                try:
                                    # Handle common ISO 8601 formats including Zulu time
                                    if timestamp_str.endswith('Z'):
                                        timestamp_str = timestamp_str[:-1] + '+00:00'
                                    # Parse the timestamp
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

def query_historical_data(config, time_range="1h", metrics=None, aggregation="raw"):
    """
    Query historical data from MongoDB
    
    This function:
    1. Connects to MongoDB
    2. Queries historical data based on time range and selected metrics
    3. Returns aggregated historical data
    
    Parameters:
    - config: Configuration dictionary with MongoDB settings
    - time_range: time period to query (e.g., "1h", "24h", "7d")
    - metrics: list of metric types to include
    - aggregation: aggregation method ("raw", "hourly", "daily", "weekly")
    
    Returns:
    pandas DataFrame with historical data
    """
    mongo_uri = config.get("mongo_uri", "mongodb://localhost:27017/")
    mongo_db = config.get("mongo_db", "streaming_data")
    
    try:
        # Connect to MongoDB
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=3000)  # Reduced from 5000ms
        # Test connection
        client.admin.command('ping')
        db = client[mongo_db]
        collection = db['sensor_data']
        
        # Parse time range
        time_map = {
            "1h": timedelta(hours=1),
            "24h": timedelta(hours=24),
            "7d": timedelta(days=7),
            "30d": timedelta(days=30)
        }
        
        time_delta = time_map.get(time_range, timedelta(hours=1))
        # Use UTC+8 time to match MongoDB stored timestamps
        utc_plus_8 = timezone(timedelta(hours=8))
        start_time = datetime.now(utc_plus_8) - time_delta
        
        # Build query
        query = {"timestamp": {"$gte": start_time}}
        
        # Add metric filter if specified
        if metrics and metrics != ["all"] and "all" not in metrics:
            query["metric_type"] = {"$in": metrics}
        
        # Retrieve data from MongoDB
        cursor = collection.find(query).sort("timestamp", 1)
        data_list = list(cursor)
        
        client.close()
        
        if not data_list:
            st.warning(f"No historical data found for the selected time range ({time_range})")
            return pd.DataFrame()
        
        # Convert to DataFrame
        df = pd.DataFrame(data_list)
        
        # Drop MongoDB _id field if present
        if '_id' in df.columns:
            df = df.drop('_id', axis=1)
        
        # Apply aggregation if requested
        if aggregation != "raw" and not df.empty:
            df = apply_aggregation(df, aggregation)
        
        return df
        
    except (ConnectionFailure, ServerSelectionTimeoutError) as e:
        st.error(f"MongoDB connection failed: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error querying historical data: {e}")
        return pd.DataFrame()


def apply_aggregation(df, aggregation):
    """
    Apply time-based aggregation to the data
    
    Parameters:
    - df: Input DataFrame
    - aggregation: Aggregation period ("hourly", "daily", "weekly")
    
    Returns:
    Aggregated DataFrame
    """
    if df.empty:
        return df
    
    freq_map = {
        "hourly": "H",
        "daily": "D",
        "weekly": "W"
    }
    
    freq = freq_map.get(aggregation, "H")
    
    try:
        # Ensure timestamp is datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Group by time period and metric type
        df_agg = df.groupby([
            pd.Grouper(key='timestamp', freq=freq),
            'metric_type',
            'stock_symbol'
        ]).agg({
            'value': ['mean', 'min', 'max', 'std', 'count']
        }).reset_index()
        
        # Flatten column names
        df_agg.columns = ['timestamp', 'metric_type', 'stock_symbol', 'value', 'min_value', 'max_value', 'std_value', 'count']
        
        return df_agg
        
    except Exception as e:
        st.warning(f"Aggregation failed: {e}. Returning raw data.")
        return df


def display_real_time_view(config, refresh_interval=15):
    """
    Page 1: Real-time Streaming View
    Displays real-time stock price data from Kafka
    """
    st.header("💹 Real-time Stock Market Stream")
    
    # Refresh status
    refresh_state = st.session_state.refresh_state
    st.info(f"**Auto-refresh:** {'🟢 Enabled' if refresh_state['auto_refresh'] else '🔴 Disabled'} - Updates every {refresh_interval} seconds")
    
    # Fetch data without spinner to avoid screen darkening
    real_time_data = consume_kafka_data(config)
    
    # Cache the data in session state to persist across tab switches
    if real_time_data is not None and not real_time_data.empty:
        st.session_state['real_time_data'] = real_time_data
    elif 'real_time_data' in st.session_state:
        real_time_data = st.session_state['real_time_data']
    
    if real_time_data is not None:
        # Data freshness indicator
        data_freshness = datetime.now() - refresh_state['last_refresh']
        freshness_color = "🟢" if data_freshness.total_seconds() < 10 else "🟡" if data_freshness.total_seconds() < 30 else "🔴"
        
        st.success(f"{freshness_color} Data updated {data_freshness.total_seconds():.0f} seconds ago")
        
        # Real-time data metrics
        st.subheader("📊 Live Market Metrics")
        if not real_time_data.empty:
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Data Points", len(real_time_data))
            with col2:
                latest_stock = real_time_data.iloc[-1]
                latest_price = f"${latest_stock['value']:.2f}"
                latest_change = latest_stock.get('change_percent', 'N/A')
                st.metric(
                    f"{latest_stock.get('stock_symbol', 'Stock')} Price", 
                    latest_price,
                    f"{latest_change}%" if latest_change != 'N/A' else None
                )
            with col3:
                st.metric("Unique Stocks", real_time_data['stock_symbol'].nunique() if 'stock_symbol' in real_time_data.columns else 'N/A')
            with col4:
                avg_price = real_time_data['value'].mean()
                st.metric("Avg Price", f"${avg_price:.2f}")
        
        # Real-time chart
        st.subheader("📈 Live Stock Prices")
        
        if not real_time_data.empty:
            # Stock price chart with color-coded lines by stock symbol
            fig = px.line(
                real_time_data,
                x='timestamp',
                y='value',
                color='stock_symbol' if 'stock_symbol' in real_time_data.columns else None,
                title=f"Real-time Stock Prices (Last {len(real_time_data)} updates)",
                labels={'value': 'Stock Price (USD)', 'timestamp': 'Time', 'stock_symbol': 'Stock'},
                template='plotly_white'
            )
            fig.update_layout(
                xaxis_title="Time",
                yaxis_title="Price (USD)",
                hovermode='x unified',
                legend_title_text='Stock'
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Raw data table with auto-refresh
            with st.expander("📋 View Raw Data"):
                st.dataframe(
                    real_time_data.sort_values('timestamp', ascending=False),
                    width='stretch',
                    height=300
                )
        else:
            st.warning("No real-time data available. Kafka consumer implemented but no data received.")
    
    else:
        st.error("Kafka data consumption error occurred")

def display_historical_view(config):
    """
    Page 2: Historical Data Analysis
    Displays historical stock price data queried from MongoDB
    """
    st.header("📊 Historical Stock Market Analysis")
    
    # Interactive controls
    st.subheader("Data Filters")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        time_range = st.selectbox(
            "Time Range",
            ["1h", "24h", "7d", "30d"],
            help="Select the time period to analyze"
        )
    
    with col2:
        metric_type = st.multiselect(
            "Metric Type",
            ["stock_price", "all"],
            default=["all"],
            help="Select metric types to display"
        )
    
    with col3:
        aggregation = st.selectbox(
            "Aggregation",
            ["raw", "hourly", "daily", "weekly"],
            help="Select data aggregation level"
        )
    
    # Query button
    if st.button("🔍 Query Historical Data", type="primary"):
        # Query data and update state
        historical_data = query_historical_data(
            config,
            time_range=time_range,
            metrics=metric_type,
            aggregation=aggregation
        )
        st.session_state['historical_data'] = historical_data
        
        # Show brief success/info message
        if historical_data is not None and not historical_data.empty:
            st.success(f"✅ Query completed! Found {len(historical_data)} records.")
        else:
            st.info("Query completed - No data found for the selected criteria.")
    
    # Display cached data if available
    if 'historical_data' in st.session_state:
        historical_data = st.session_state['historical_data']
    else:
        historical_data = None
    
    if historical_data is not None and not historical_data.empty:
        # Display data summary metrics
        st.subheader("📈 Data Summary")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Records", len(historical_data))
        
        with col2:
            if 'timestamp' in historical_data.columns:
                date_range = f"{pd.to_datetime(historical_data['timestamp']).min().strftime('%m/%d %H:%M')} - {pd.to_datetime(historical_data['timestamp']).max().strftime('%m/%d %H:%M')}"
                st.metric("Time Range", date_range)
        
        with col3:
            if 'value' in historical_data.columns:
                st.metric("Average Price", f"${historical_data['value'].mean():.2f}")
        
        with col4:
            if 'stock_symbol' in historical_data.columns:
                st.metric("Stocks Tracked", historical_data['stock_symbol'].nunique())
        
        # Historical trend visualization
        st.subheader("📊 Historical Trends")
        
        if 'value' in historical_data.columns:
            # Time series chart
            fig = px.line(
                historical_data,
                x='timestamp',
                y='value',
                color='stock_symbol' if 'stock_symbol' in historical_data.columns else None,
                title=f"Historical Stock Prices - {time_range} ({aggregation} aggregation)",
                labels={'value': 'Stock Price (USD)', 'timestamp': 'Time', 'stock_symbol': 'Stock'},
                template='plotly_white'
            )
            fig.update_layout(
                xaxis_title="Time",
                yaxis_title="Price (USD)",
                hovermode='x unified',
                height=500,
                legend_title_text='Stock'
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Distribution chart
            if 'stock_symbol' in historical_data.columns and historical_data['stock_symbol'].nunique() > 1:
                st.subheader("📉 Price Distribution by Stock")
                fig_box = px.box(
                    historical_data,
                    x='stock_symbol',
                    y='value',
                    title="Stock Price Distribution",
                    labels={'value': 'Price (USD)', 'stock_symbol': 'Stock'},
                    template='plotly_white'
                )
                st.plotly_chart(fig_box, use_container_width=True)
            
            # Stock comparison
            if 'stock_symbol' in historical_data.columns and historical_data['stock_symbol'].nunique() > 1:
                st.subheader("🔍 Stock Performance Comparison")
                
                # Calculate percentage change from first value for each stock
                if aggregation == 'raw':
                    comparison_data = historical_data.copy()
                    comparison_data['normalized'] = comparison_data.groupby('sensor_id')['value'].transform(
                        lambda x: ((x / x.iloc[0]) - 1) * 100 if len(x) > 0 and x.iloc[0] != 0 else 0
                    )
                    
                    fig_comp = px.line(
                        comparison_data,
                        x='timestamp',
                        y='normalized',
                        color='sensor_id',
                        title="Normalized Stock Performance (% Change from Start)",
                        labels={'normalized': 'Change (%)', 'timestamp': 'Time', 'sensor_id': 'Stock Symbol'},
                        template='plotly_white'
                    )
                    fig_comp.update_layout(
                        xaxis_title="Time",
                        yaxis_title="Percentage Change (%)",
                        hovermode='x unified',
                        legend_title_text='Stock Symbol'
                    )
                    st.plotly_chart(fig_comp, use_container_width=True)
        
        # Statistical summary
        with st.expander("📊 Statistical Summary"):
            if 'value' in historical_data.columns:
                if 'stock_symbol' in historical_data.columns:
                    summary_df = historical_data.groupby('sensor_id')['value'].describe()
                    summary_df.columns = ['Count', 'Mean Price', 'Std Dev', 'Min Price', '25%', '50% (Median)', '75%', 'Max Price']
                else:
                    summary_df = historical_data['value'].describe()
                st.dataframe(summary_df, use_container_width=True)
        
        # Raw data table
        with st.expander("📋 View Raw Historical Data"):
            st.dataframe(
                historical_data.sort_values('timestamp', ascending=False) if 'timestamp' in historical_data.columns else historical_data,
                use_container_width=True,
                height=400
            )
    
    elif historical_data is not None and historical_data.empty:
        st.info("No data found for the selected criteria. Try a different time range or metric type.")
    
    else:
        st.info("👆 Click 'Query Historical Data' to load historical analysis from MongoDB")

def main():
    """
    COMPLETED TODO: Customize the main application flow as needed
    """
    st.title("📈 Stock Market Streaming Dashboard")
    
    with st.expander("📋 Project Information"):
        st.markdown("""
        **BIG DATA STREAMING DASHBOARD**
        
        ### Features:
        - **Real-time Stock Data**: Live stock prices via Alpha Vantage API
        - **Kafka Streaming**: Real-time message processing
        - **MongoDB Storage**: Historical data persistence
        - **Interactive Charts**: Dynamic visualizations with filters
        
        **Data Source:** Alpha Vantage API  
        **Stocks Tracked:** AAPL, MSFT, GOOGL, AMZN, TSLA, META, NVDA, JPM
        """)
    
    # Initialize session state for refresh management
    if 'refresh_state' not in st.session_state:
        st.session_state.refresh_state = {
            'last_refresh': datetime.now(),
            'auto_refresh': True
        }
    
    # Setup configuration
    config = setup_sidebar()
    
    # Refresh controls in sidebar
    st.sidebar.subheader("Refresh Settings")
    st.session_state.refresh_state['auto_refresh'] = st.sidebar.checkbox(
        "Enable Auto Refresh",
        value=st.session_state.refresh_state['auto_refresh'],
        help="Automatically refresh real-time data"
    )
    
    refresh_interval = 15  # Default value
    if st.session_state.refresh_state['auto_refresh']:
        refresh_interval = st.sidebar.slider(
            "Refresh Interval (seconds)",
            min_value=5,
            max_value=60,
            value=15,
            help="Set how often real-time data refreshes"
        )
        
        # Auto-refresh using streamlit-autorefresh package (runs silently without darkening)
        st_autorefresh(interval=refresh_interval * 1000, key="auto_refresh", limit=None, debounce=False)
    
    # Manual refresh button
    if st.sidebar.button("🔄 Manual Refresh"):
        st.session_state.refresh_state['last_refresh'] = datetime.now()
        st.rerun()
    
    # Display refresh status
    st.sidebar.markdown("---")
    st.sidebar.metric("Last Refresh", st.session_state.refresh_state['last_refresh'].strftime("%H:%M:%S"))
    
    # Create tabs for different views
    tab1, tab2 = st.tabs(["💹 Live Stock Prices", "📊 Historical Analysis"])
    
    with tab1:
        display_real_time_view(config, refresh_interval)
    
    with tab2:
        display_historical_view(config)
    

if __name__ == "__main__":
    main()