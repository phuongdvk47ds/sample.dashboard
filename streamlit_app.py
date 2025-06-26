import streamlit as st
import pandas as pd
import plotly.graph_objects as go

def main():
    st.title("Candle Stick Chart Dashboard")

    # Upload file
    uploaded_file = st.file_uploader("Upload a Parquet file", type=["parquet"])
    
    if uploaded_file is not None:
        df = pd.read_parquet(uploaded_file)
        
        # Check required columns
        required_columns = ['<Ticker>', '<DTYYYYMMDD>', '<Open>', '<High>', '<Low>', '<Close>', '<Volume>']
        if not all(col in df.columns for col in required_columns):
            st.error("File Parquet không có đúng cấu trúc cột yêu cầu")
            return
        
        # Rename columns
        df.rename(columns={
            '<Ticker>': 'Ticker',
            '<DTYYYYMMDD>': 'Date',
            '<Open>': 'Open',
            '<High>': 'High',
            '<Low>': 'Low',
            '<Close>': 'Close',
            '<Volume>': 'Volume'
        }, inplace=True)
        
        # Convert date column
        df['Date'] = pd.to_datetime(df['Date'].astype(str), format='%Y%m%d')
        
        # Sort data
        df.sort_values(['Ticker', 'Date'], inplace=True)
        
        # Filter by ticker
        ticker = st.selectbox("Select Ticker", df['Ticker'].unique())
        df_ticker = df[df['Ticker'] == ticker]
        
        # Create candlestick chart
        fig = go.Figure(data=[go.Candlestick(x=df_ticker['Date'],
                                             open=df_ticker['Open'],
                                             high=df_ticker['High'],
                                             low=df_ticker['Low'],
                                             close=df_ticker['Close'])])
        
        fig.update_layout(title=f"Candle Stick Chart for {ticker}",
                          xaxis_title="Date",
                          yaxis_title="Price")
        
        st.plotly_chart(fig)
        st.success("Biểu đồ nến đã được tạo thành công!")
    else:
        st.info("Vui lòng tải lên file Parquet để xem biểu đồ nến.")

if __name__ == "__main__":
    main()
