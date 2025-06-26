import pandas as pd
import plotly.graph_objects as go
from datetime import datetime
import boto3
from io import BytesIO
import os

def get_s3_client():
    """
    Khởi tạo S3 client với AWS credentials
    """
    # Có thể nhập từ biến môi trường hoặc file cấu hình AWS
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    aws_region = os.getenv('AWS_REGION', 'ap-southeast-1')  # Mặc định Singapore
    
    if not aws_access_key_id or not aws_secret_access_key:
        raise ValueError("AWS credentials chưa được cấu hình")
    
    return boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=aws_region
    )

def read_parquet_from_s3(s3_client, bucket_name, file_key):
    """
    Đọc file Parquet từ S3 và trả về DataFrame
    """
    try:
        # Lấy object từ S3
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        
        # Đọc nội dung file vào buffer
        parquet_buffer = BytesIO(response['Body'].read())
        
        # Đọc Parquet từ buffer
        df = pd.read_parquet(parquet_buffer)
            
        required_columns = ['<Ticker>', '<DTYYYYMMDD>', '<Open>', '<High>', '<Low>', '<Close>', '<Volume>']
        if not all(col in df.columns for col in required_columns):
            raise ValueError("File Parquet không có đúng cấu trúc cột yêu cầu")
        
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
        
        return df
    
    except Exception as e:
        print(f"Lỗi khi đọc file từ S3: {e}")
        return None

def preprocess_data(df):
    """
    Tiền xử lý dữ liệu: chuyển đổi ngày, sắp xếp dữ liệu
    """
    # Chuyển đổi cột ngày từ số sang datetime
    df['Date'] = pd.to_datetime(df['Date'].astype(str), format='%Y%m%d')
    
    # Sắp xếp theo mã và ngày
    df = df.sort_values(['Ticker', 'Date'])
    
    return df

def filter_data(df, ticker=None, start_date=None, end_date=None):
    """
    Lọc dữ liệu theo mã cổ phiếu và khoảng thời gian
    """
    if ticker:
        df = df[df['Ticker'] == ticker]
    
    if start_date:
        start_date = pd.to_datetime(start_date)
        df = df[df['Date'] >= start_date]
    
    if end_date:
        end_date = pd.to_datetime(end_date)
        df = df[df['Date'] <= end_date]
    
    return df

def plot_candlestick(df, title=''):
    """
    Vẽ biểu đồ nến từ DataFrame
    """
    if df.empty:
        print("Không có dữ liệu để vẽ biểu đồ")
        return
    
    fig = go.Figure(data=[go.Candlestick(
        x=df['Date'],
        open=df['Open'],
        high=df['High'],
        low=df['Low'],
        close=df['Close'],
        increasing_line_color='green',
        decreasing_line_color='red'
    )])
    
    fig.update_layout(
        title=title,
        xaxis_title='Ngày',
        yaxis_title='Giá',
        xaxis_rangeslider_visible=False,
        template='plotly_white'
    )
    
    fig.show()

def main():
    # Khởi tạo S3 client
    try:
        s3_client = get_s3_client()
    except Exception as e:
        print(f"Lỗi khi kết nối AWS S3: {e}")
        return
    
    # Nhập thông tin bucket và file từ người dùng
    bucket_name = input("Nhập tên S3 bucket: ").strip()
    file_key = input("Nhập đường dẫn file trong S3 (key): ").strip()
    
    # Đọc file Parquet từ S3
    df = read_parquet_from_s3(s3_client, bucket_name, file_key)
    if df is None:
        return
    
    # Tiền xử lý dữ liệu
    df = preprocess_data(df)
    
    # Lấy danh sách các mã cổ phiếu
    tickers = df['Ticker'].unique()
    print(f"Tìm thấy {len(tickers)} mã cổ phiếu trong file:")
    print(", ".join(tickers))
    
    while True:
        print("\nMENU:")
        print("1. Vẽ biểu đồ cho tất cả mã cổ phiếu")
        print("2. Vẽ biểu đồ cho một mã cổ phiếu cụ thể")
        print("3. Thoát")
        
        choice = input("Chọn chức năng (1-3): ").strip()
        
        if choice == '1':
            # Vẽ tất cả các mã
            start_date = input("Nhập ngày bắt đầu (YYYY-MM-DD, để trống nếu không cần): ").strip()
            end_date = input("Nhập ngày kết thúc (YYYY-MM-DD, để trống nếu không cần): ").strip()
            
            for ticker in tickers:
                filtered_df = filter_data(df, ticker, start_date or None, end_date or None)
                if not filtered_df.empty:
                    plot_candlestick(filtered_df, f'Biểu đồ nến {ticker}')
                else:
                    print(f"Không có dữ liệu cho mã {ticker} trong khoảng thời gian đã chọn")
        
        elif choice == '2':
            # Vẽ một mã cụ thể
            ticker = input(f"Nhập mã cổ phiếu (có sẵn: {', '.join(tickers)}): ").strip().upper()
            if ticker not in tickers:
                print("Mã cổ phiếu không tồn tại trong dữ liệu!")
                continue
            
            start_date = input("Nhập ngày bắt đầu (YYYY-MM-DD, để trống nếu không cần): ").strip()
            end_date = input("Nhập ngày kết thúc (YYYY-MM-DD, để trống nếu không cần): ").strip()
            
            filtered_df = filter_data(df, ticker, start_date or None, end_date or None)
            plot_candlestick(filtered_df, f'Biểu đồ nến {ticker}')
        
        elif choice == '3':
            print("Kết thúc chương trình")
            break
        
        else:
            print("Lựa chọn không hợp lệ, vui lòng chọn lại")

if __name__ == "__main__":
    main()