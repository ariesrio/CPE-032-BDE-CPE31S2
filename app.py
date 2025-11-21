import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import time
import json
from datetime import datetime, timedelta
from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable
from streamlit_autorefresh import st_autorefresh
from pymongo import MongoClient
import base64

# Page configuration
st.set_page_config(
    page_title="Crypto Market Dashboard",
    page_icon="₿",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Purple/Teal color palette theme with improved button contrast
st.markdown("""
<style>
    /* Main background - Dark purple */
    .stApp {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
    }
    
    /* Sidebar - Darker purple */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0f0f1e 0%, #1a1a2e 100%);
        border-right: 1px solid #2d4059;
    }
    
    /* Headers - Teal/Cyan */
    h1, h2, h3 {
        color: #00d4ff !important;
        font-weight: 700 !important;
        text-shadow: 0 0 10px rgba(0, 212, 255, 0.3);
    }
    
    /* Metrics */
    [data-testid="stMetricValue"] {
        color: #00ffc8 !important;
        font-size: 32px !important;
        font-weight: 700 !important;
    }
    
    [data-testid="stMetricLabel"] {
        color: #8b9dc3 !important;
        font-weight: 500 !important;
    }
    
    [data-testid="stMetricDelta"] {
        color: #00ffc8 !important;
    }
    
    /* Buttons - Dark background with bright text for readability */
    .stButton>button {
        background: linear-gradient(135deg, #2d4059 0%, #1a1a2e 100%);
        color: #00ffc8 !important;
        border: 2px solid #00d4ff !important;
        border-radius: 8px;
        padding: 10px 24px;
        font-weight: 700 !important;
        font-size: 16px !important;
        box-shadow: 0 4px 15px rgba(0, 212, 255, 0.4);
        transition: all 0.3s ease;
    }
    
    .stButton>button:hover {
        background: linear-gradient(135deg, #00d4ff 0%, #00ffc8 100%);
        color: #0f0f1e !important;
        box-shadow: 0 6px 20px rgba(0, 212, 255, 0.6);
        transform: translateY(-2px);
    }
    
    /* Input fields and select boxes */
    .stTextInput>div>div>input, .stSelectbox>div>div>select {
        background-color: #1a1a2e !important;
        color: #ffffff !important;
        border: 1px solid #2d4059 !important;
        border-radius: 6px;
    }
    
    /* Dataframe */
    [data-testid="stDataFrame"] {
        border: 1px solid #2d4059;
        border-radius: 8px;
        background-color: #16213e;
    }
    
    /* Success/Info boxes */
    .stSuccess {
        background-color: rgba(0, 255, 200, 0.1);
        border-left: 4px solid #00ffc8;
        color: #00ffc8 !important;
    }
    
    .stInfo {
        background-color: rgba(0, 212, 255, 0.1);
        border-left: 4px solid #00d4ff;
        color: #00d4ff !important;
    }
    
    .stWarning {
        background-color: rgba(255, 193, 7, 0.1);
        border-left: 4px solid #ffc107;
        color: #ffc107 !important;
    }
    
    .stError {
        background-color: rgba(255, 82, 82, 0.1);
        border-left: 4px solid #ff5252;
        color: #ff5252 !important;
    }
    
    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background-color: #1a1a2e;
        border-radius: 8px;
        padding: 6px;
    }
    
    .stTabs [data-baseweb="tab"] {
        border-radius: 6px;
        color: #8b9dc3;
        font-weight: 600;
        padding: 10px 20px;
    }
    
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #00d4ff 0%, #00ffc8 100%);
        color: #0f0f1e !important;
    }
    
    /* Expander */
    .streamlit-expanderHeader {
        background-color: #1a1a2e;
        border-radius: 8px;
        color: #00d4ff !important;
        font-weight: 600;
        border: 1px solid #2d4059;
    }
    
    /* Text */
    p, span, div, label {
        color: #dfe6e9 !important;
    }
    
    /* Slider */
    .stSlider>div>div>div>div {
        background-color: #00d4ff;
    }
</style>
""", unsafe_allow_html=True)

# CoinGecko symbol mapping
SYMBOL_MAPPING = {
    'BITCOIN': {'display': 'Bitcoin (BTC)', 'coingecko_id': 'bitcoin'},
    'ETHEREUM': {'display': 'Ethereum (ETH)', 'coingecko_id': 'ethereum'},
    'BINANCECOIN': {'display': 'BNB', 'coingecko_id': 'binancecoin'},
    'TETHER': {'display': 'Tether (USDT)', 'coingecko_id': 'tether'},
    'USD-COIN': {'display': 'USD Coin (USDC)', 'coingecko_id': 'usd-coin'},
    'CARDANO': {'display': 'Cardano (ADA)', 'coingecko_id': 'cardano'},
    'SOLANA': {'display': 'Solana (SOL)', 'coingecko_id': 'solana'},
    'DOGECOIN': {'display': 'Dogecoin (DOGE)', 'coingecko_id': 'dogecoin'}
}

def setup_sidebar():
    """Configure sidebar settings and controls"""
    st.sidebar.title("₿ Dashboard Controls")
    
    st.sidebar.subheader("Data Source Configuration")
    
    kafka_broker = st.sidebar.text_input(
        "Kafka Broker",
        value="localhost:9092",
        help="Kafka broker address"
    )
    
    kafka_topic = st.sidebar.text_input(
        "Kafka Topic",
        value="crypto-stream",
        help="Kafka topic to consume from"
    )
    
    mongo_uri = st.sidebar.text_input(
        "MongoDB URI",
        value="mongodb+srv://qacrio:tipqc@groceryinventorysystem.mxpcsxl.mongodb.net/?appName=GroceryInventorySystem",
        type="password",
        help="MongoDB connection URI"
    )
    
    return {
        "kafka_broker": kafka_broker,
        "kafka_topic": kafka_topic,
        "mongo_uri": mongo_uri
    }

@st.cache_resource
def get_kafka_consumer(kafka_broker, kafka_topic):
    """Initialize and cache Kafka consumer with graceful error handling"""
    try:
        consumer = KafkaConsumer(
            kafka_topic,
            bootstrap_servers=[kafka_broker],
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=2000,
            session_timeout_ms=10000,
            request_timeout_ms=12000
        )
        return consumer
    except Exception:
        return None

@st.cache_resource
def get_mongo_client(mongo_uri):
    """Initialize and cache MongoDB client"""
    try:
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        return client
    except Exception:
        return None

def consume_kafka_data(config):
    """Consume real-time data from Kafka with fallback handling"""
    consumer = get_kafka_consumer(config['kafka_broker'], config['kafka_topic'])
    
    if consumer is None:
        return None
    
    try:
        messages = []
        poll_count = 0
        max_polls = 3
        
        while poll_count < max_polls and len(messages) < 50:
            msg_pack = consumer.poll(timeout_ms=1000)
            
            for tp, messages_batch in msg_pack.items():
                for message in messages_batch:
                    try:
                        data = message.value
                        
                        timestamp_str = data['timestamp']
                        if timestamp_str.endswith('Z'):
                            timestamp_str = timestamp_str[:-1] + '+00:00'
                        
                        messages.append({
                            'timestamp': datetime.fromisoformat(timestamp_str),
                            'symbol': data['sensor_id'],
                            'price': float(data['value']),
                            'from_currency': data.get('from_currency_name', data['sensor_id']),
                            'to_currency': data.get('to_currency', 'USD'),
                            'bid_price': data.get('bid_price', 0),
                            'ask_price': data.get('ask_price', 0),
                            'volume_24h': data.get('volume_24h', 0),
                            'price_change_24h': data.get('price_change_24h', 0),
                            'price_change_percent_24h': data.get('price_change_percent_24h', 0),
                            'high_24h': data.get('high_24h', 0),
                            'low_24h': data.get('low_24h', 0),
                            'last_refreshed': data.get('last_refreshed', '')
                        })
                    except Exception:
                        continue
            
            poll_count += 1
        
        if messages:
            return pd.DataFrame(messages).sort_values('timestamp', ascending=False)
        else:
            return None
            
    except Exception:
        return None

def query_mongodb(mongo_uri, symbol, hours):
    """Query historical data from MongoDB for price trend analysis"""
    client = get_mongo_client(mongo_uri)
    
    if client is None:
        return None
    
    try:
        db = client['crypto_streaming']
        collection = db['crypto_data']
        
        start_time = datetime.utcnow() - timedelta(hours=hours)
        
        # Query using the symbol format stored in MongoDB
        cursor = collection.find({
            'sensor_id': symbol,
            'inserted_at': {'$gte': start_time}
        }).sort('inserted_at', 1)
        
        data = list(cursor)
        
        if not data:
            return None
        
        df = pd.DataFrame(data)
        df['timestamp'] = pd.to_datetime(df['timestamp'].str.replace('Z', '+00:00'))
        df['price'] = df['value'].astype(float)
        df['symbol'] = df['sensor_id']
        
        return df
        
    except Exception as e:
        st.error(f"MongoDB query error: {e}")
        return None

def get_available_symbols_from_mongodb(mongo_uri):
    """Get list of available cryptocurrency symbols from MongoDB"""
    client = get_mongo_client(mongo_uri)
    
    if client is None:
        return []
    
    try:
        db = client['crypto_streaming']
        collection = db['crypto_data']
        
        # Get distinct sensor_ids from the collection
        symbols = collection.distinct('sensor_id')
        return sorted(symbols)
        
    except Exception:
        return []

def create_download_link(fig, filename):
    """Create download button for plotly figures"""
    try:
        img_bytes = fig.to_image(format="png", width=1200, height=600)
        b64 = base64.b64encode(img_bytes).decode()
        href = f'<a href="data:image/png;base64,{b64}" download="{filename}" style="color: #00d4ff; text-decoration: none; font-weight: 600;">📥 Download Chart as PNG</a>'
        return href
    except:
        return ""

def display_real_time_view(config, refresh_interval):
    """Real-time streaming dashboard view with persistent data"""
    st.header("₿ Real-time Crypto Market Stream")
    
    # Initialize session state for cached data
    if 'cached_data' not in st.session_state:
        st.session_state.cached_data = None
    if 'last_successful_fetch' not in st.session_state:
        st.session_state.last_successful_fetch = None
    
    refresh_state = st.session_state.refresh_state
    
    col1, col2 = st.columns([3, 1])
    with col1:
        if refresh_state['auto_refresh']:
            st.success(f"🟢 Auto-refresh enabled • Updates every {refresh_interval}s")
        else:
            st.info("🔵 Auto-refresh disabled • Use manual refresh")
    
    with col2:
        data_age = (datetime.now() - refresh_state['last_refresh']).total_seconds()
        st.metric("Data Age", f"{data_age:.0f}s")
    
    # Try to fetch new data
    with st.spinner("Fetching live crypto data from Kafka..."):
        real_time_data = consume_kafka_data(config)
    
    # Update cache if new data received
    if real_time_data is not None and not real_time_data.empty:
        st.session_state.cached_data = real_time_data
        st.session_state.last_successful_fetch = datetime.now()
    
    # Use cached data if available, even if new fetch failed
    display_data = st.session_state.cached_data
    
    if display_data is not None and not display_data.empty:
        # Show cache age if using cached data
        if st.session_state.last_successful_fetch:
            cache_age = (datetime.now() - st.session_state.last_successful_fetch).total_seconds()
            if cache_age > refresh_interval:
                st.warning(f"⚠️ Showing cached data ({cache_age:.0f}s old). Producer may be loading or Kafka connection temporarily unavailable.")
        
        st.subheader("💰 Live Market Snapshot")
        
        cols = st.columns(min(len(display_data['symbol'].unique()), 5))
        
        for idx, symbol in enumerate(display_data['symbol'].unique()):
            if idx >= 5:
                break
            crypto_data = display_data[display_data['symbol'] == symbol].iloc[0]
            
            with cols[idx]:
                change_pct = crypto_data.get('price_change_percent_24h', 0)
                change_symbol = "📈" if change_pct > 0 else "📉" if change_pct < 0 else "➡️"
                
                # Get display name from mapping
                display_name = SYMBOL_MAPPING.get(symbol, {}).get('display', symbol)
                
                st.metric(
                    label=f"{display_name}",
                    value=f"${crypto_data['price']:,.4f}",
                    delta=f"{change_symbol} {change_pct:.2f}%"
                )
                st.caption(crypto_data['from_currency'])
        
        st.markdown("---")
        
        st.subheader("📈 Price Trends")
        
        fig = px.line(
            display_data,
            x='timestamp',
            y='price',
            color='symbol',
            title="Real-time Cryptocurrency Prices",
            template='plotly_dark',
            markers=True
        )
        
        fig.update_layout(
            plot_bgcolor='#16213e',
            paper_bgcolor='#1a1a2e',
            font=dict(color='#dfe6e9'),
            xaxis=dict(showgrid=True, gridcolor='#2d4059', title='Time'),
            yaxis=dict(showgrid=True, gridcolor='#2d4059', title='Price (USD)'),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1,
                bgcolor='rgba(26, 26, 46, 0.8)'
            )
        )
        
        st.plotly_chart(fig, use_container_width=True)
        st.markdown(create_download_link(fig, "realtime_crypto_prices.png"), unsafe_allow_html=True)
        
        st.markdown("---")
        
        st.subheader("💹 24h Performance")
        
        perf_data = display_data.groupby('symbol').agg({
            'price_change_percent_24h': 'last',
            'volume_24h': 'last'
        }).reset_index()
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig2 = px.bar(
                perf_data,
                x='symbol',
                y='price_change_percent_24h',
                title="24h Price Change %",
                template='plotly_dark',
                color='price_change_percent_24h',
                color_continuous_scale='RdYlGn'
            )
            
            fig2.update_layout(
                plot_bgcolor='#16213e',
                paper_bgcolor='#1a1a2e',
                font=dict(color='#dfe6e9'),
                showlegend=False,
                yaxis_title='Change %'
            )
            
            st.plotly_chart(fig2, use_container_width=True)
            st.markdown(create_download_link(fig2, "24h_price_change.png"), unsafe_allow_html=True)
        
        with col2:
            fig3 = px.bar(
                perf_data,
                x='symbol',
                y='volume_24h',
                title="24h Trading Volume",
                template='plotly_dark',
                color='volume_24h',
                color_continuous_scale='Teal'
            )
            
            fig3.update_layout(
                plot_bgcolor='#16213e',
                paper_bgcolor='#1a1a2e',
                font=dict(color='#dfe6e9'),
                showlegend=False,
                yaxis_title='Volume'
            )
            
            st.plotly_chart(fig3, use_container_width=True)
            st.markdown(create_download_link(fig3, "24h_volume.png"), unsafe_allow_html=True)
        
        with st.expander("📋 View Raw Stream Data"):
            st.dataframe(
                display_data,
                use_container_width=True,
                hide_index=True,
                height=400
            )
    
    else:
        st.info("⏳ Waiting for data... If producer is starting up, data will appear shortly and persist here.")
        st.info("💡 Tip: Once data appears, it will remain visible even during producer reload cycles.")

def display_historical_view(config):
    """Historical data analysis view - Price Trend Analysis only"""
    st.header("📊 Historical Crypto Analysis")
    
    st.subheader("Price Trend Analysis")
    st.markdown("**Track price movements and identify trends over time**")
    
    # Get available symbols from MongoDB
    available_symbols = get_available_symbols_from_mongodb(config['mongo_uri'])
    
    if not available_symbols:
        st.warning("⚠️ No data in MongoDB yet. Start the producer to collect data.")
        return
    
    # Create display options for dropdown
    symbol_options = {}
    for symbol in available_symbols:
        display_name = SYMBOL_MAPPING.get(symbol, {}).get('display', symbol)
        symbol_options[display_name] = symbol
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        selected_display = st.selectbox(
            "Cryptocurrency Symbol",
            options=list(symbol_options.keys()),
            index=0,
            help="Select a cryptocurrency to analyze"
        )
        selected_symbol = symbol_options[selected_display]
    
    with col2:
        hours = st.slider("Time Range (hours)", 1, 168, 24)
    
    with col3:
        run_query = st.button("🔍 Run Query", use_container_width=True)
    
    if run_query:
        with st.spinner(f"Analyzing {selected_display} price trends..."):
            df = query_mongodb(config['mongo_uri'], selected_symbol, hours)
            
            if df is not None and not df.empty:
                st.success(f"✓ Retrieved {len(df)} records for {selected_display}")
                
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Current Price", f"${df.iloc[-1]['price']:,.4f}")
                with col2:
                    price_change = df.iloc[-1]['price'] - df.iloc[0]['price']
                    pct_change = (price_change / df.iloc[0]['price']) * 100
                    st.metric("Price Change", f"${price_change:,.4f}", f"{pct_change:+.2f}%")
                with col3:
                    st.metric("High", f"${df['price'].max():,.4f}")
                with col4:
                    st.metric("Low", f"${df['price'].min():,.4f}")
                
                st.subheader(f"{selected_display} Price Movement")
                
                fig = go.Figure()
                
                fig.add_trace(go.Scatter(
                    x=df['timestamp'],
                    y=df['price'],
                    mode='lines+markers',
                    name='Price',
                    line=dict(color='#00ffc8', width=2),
                    fill='tozeroy',
                    fillcolor='rgba(0, 255, 200, 0.1)'
                ))
                
                # Add moving average
                df['ma'] = df['price'].rolling(window=min(5, len(df))).mean()
                fig.add_trace(go.Scatter(
                    x=df['timestamp'],
                    y=df['ma'],
                    mode='lines',
                    name='Moving Average',
                    line=dict(color='#00d4ff', width=2, dash='dash')
                ))
                
                fig.update_layout(
                    plot_bgcolor='#16213e',
                    paper_bgcolor='#1a1a2e',
                    font=dict(color='#dfe6e9'),
                    xaxis=dict(showgrid=True, gridcolor='#2d4059', title='Time'),
                    yaxis=dict(showgrid=True, gridcolor='#2d4059', title='Price (USD)'),
                    title=f"{selected_display} Price Trend - Last {hours} Hours",
                    hovermode='x unified',
                    legend=dict(bgcolor='rgba(26, 26, 46, 0.8)')
                )
                
                st.plotly_chart(fig, use_container_width=True)
                st.markdown(create_download_link(fig, f"{selected_symbol}_price_trend.png"), unsafe_allow_html=True)
                
                with st.expander("📋 View Data Table"):
                    st.dataframe(df[['timestamp', 'symbol', 'price']], use_container_width=True, hide_index=True)
            else:
                st.warning(f"No data found for {selected_display} in the last {hours} hours")

def main():
    """Main application"""
    st.title("₿ Real-time Cryptocurrency Dashboard")
    st.markdown("*Live streaming and historical analysis powered by CoinGecko API, Kafka, and MongoDB*")
    
    if 'refresh_state' not in st.session_state:
        st.session_state.refresh_state = {
            'last_refresh': datetime.now(),
            'auto_refresh': True
        }
    
    config = setup_sidebar()
    
    st.sidebar.markdown("---")
    st.sidebar.subheader("Refresh Settings")
    
    st.session_state.refresh_state['auto_refresh'] = st.sidebar.checkbox(
        "Enable Auto Refresh",
        value=st.session_state.refresh_state['auto_refresh']
    )
    
    if st.session_state.refresh_state['auto_refresh']:
        refresh_interval = st.sidebar.slider(
            "Refresh Interval (seconds)",
            min_value=30,
            max_value=300,
            value=60
        )
        st_autorefresh(interval=refresh_interval * 1000, key="auto_refresh")
    else:
        refresh_interval = 60
    
    if st.sidebar.button("🔄 Manual Refresh", use_container_width=True):
        st.session_state.refresh_state['last_refresh'] = datetime.now()
        st.rerun()
    
    st.sidebar.markdown("---")
    st.sidebar.metric(
        "Last Refresh",
        st.session_state.refresh_state['last_refresh'].strftime("%H:%M:%S")
    )
    
    tab1, tab2 = st.tabs(["₿ Real-time Streaming", "📊 Historical Analysis"])
    
    with tab1:
        display_real_time_view(config, refresh_interval)
    
    with tab2:
        display_historical_view(config)

if __name__ == "__main__":
    main()
