import pandas as pd
import plotly.graph_objects as go
from datetime import datetime
import boto3
from io import BytesIO
import os
import hashlib
import json
from pathlib import Path
from dotenv import load_dotenv
# Tải biến môi trường từ file .env
load_dotenv()

class S3ParquetHandler:
    def __init__(self):
        # Validate required environment variables
        self._validate_env_vars()
        ## Khởi tạo S3 client
        self.s3_client = self._get_s3_client()
        local_cache_dir = os.getenv('LOCAL_CACHE_DIR', '~/.s3_parquet_cache')
        self.local_cache_dir = os.path.expanduser(local_cache_dir)
        os.makedirs(self.local_cache_dir, exist_ok=True)

    def _validate_env_vars(self):
        """Validate required environment variables"""
        required_vars = [
            'AWS_ACCESS_KEY_ID',
            'AWS_SECRET_ACCESS_KEY',
            'AWS_REGION',
            'S3_BUCKET_NAME',
            'S3_FILE_KEY',
            'LOCAL_CACHE_DIR'
        ]
        
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
    
    def _get_s3_client(self):
        """Khởi tạo S3 client với AWS credentials"""
        aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        aws_region = os.getenv('AWS_REGION', 'ap-southeast-1')
        
        if not aws_access_key_id or not aws_secret_access_key:
            raise ValueError("AWS credentials chưa được cấu hình")
        
        return boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region
        )
    
    def _get_s3_file_metadata(self, bucket_name, file_key):
        """Lấy metadata của file trên S3"""
        try:
            response = self.s3_client.head_object(Bucket=bucket_name, Key=file_key)
            return {
                'last_modified': response['LastModified'].timestamp(),
                'size': response['ContentLength'],
                'etag': response['ETag'].strip('"')
            }
        except Exception as e:
            print(f"Lỗi khi lấy metadata từ S3: {e}")
            return None
    
    def _get_local_file_metadata(self, local_path):
        """Lấy metadata của file local"""
        if not os.path.exists(local_path):
            return None
            
        stat = os.stat(local_path)
        with open(local_path, 'rb') as f:
            md5_hash = hashlib.md5(f.read()).hexdigest()
        
        return {
            'last_modified': stat.st_mtime,
            'size': stat.st_size,
            'etag': md5_hash
        }
    
    def _has_file_changed(self, bucket_name, file_key, local_path):
        """Kiểm tra xem file trên S3 có thay đổi so với local không"""
        s3_meta = self._get_s3_file_metadata(bucket_name, file_key)
        if not s3_meta:
            return True  # Coi như có thay đổi nếu không lấy được metadata
        
        local_meta = self._load_metadata(local_path)
        if not local_meta:
            return True  # File local không tồn tại
        
        # So sánh các thuộc tính quan trọng
        return not (s3_meta['etag'] == local_meta['etag'] and 
                   s3_meta['size'] == local_meta['size'])
    
    def _get_local_cache_path(self, bucket_name, file_key):
        """Tạo đường dẫn cache local từ thông tin S3"""
        safe_bucket = bucket_name.replace('/', '_')
        safe_key = file_key.replace('/', '_')
        return os.path.join(self.local_cache_dir, f"{safe_bucket}_{safe_key}")
    
    def _save_metadata(self, local_path, metadata):
        """Lưu metadata vào file .meta"""
        meta_path = f"{local_path}.meta"
        with open(meta_path, 'w') as f:
            json.dump(metadata, f)
    
    def _load_metadata(self, local_path):
        """Đọc metadata từ file .meta"""
        meta_path = f"{local_path}.meta"
        if not os.path.exists(meta_path):
            return None
        with open(meta_path, 'r') as f:
            return json.load(f)
    
    def download_from_s3(self, bucket_name, file_key, force_download=False):
        """
        Tải file từ S3 xuống local nếu có thay đổi hoặc chưa có
        Trả về đường dẫn file local
        """
        local_path = self._get_local_cache_path(bucket_name, file_key)
        
        # Kiểm tra nếu cần tải lại file
        if not force_download and os.path.exists(local_path):
            if not self._has_file_changed(bucket_name, file_key, local_path):
                print(f"Sử dụng bản cache local cho {bucket_name}/{file_key}")
                return local_path
        
        print(f"Tải file mới từ S3: {bucket_name}/{file_key}")
        try:
            # Tải file từ S3
            response = self.s3_client.get_object(Bucket=bucket_name, Key=file_key)
            file_content = response['Body'].read()
            
            # Lưu file xuống local
            with open(local_path, 'wb') as f:
                f.write(file_content)
            
            # Lưu metadata
            s3_meta = self._get_s3_file_metadata(bucket_name, file_key)
            if s3_meta:
                self._save_metadata(local_path, s3_meta)
            
            return local_path
        except Exception as e:
            print(f"Lỗi khi tải file từ S3: {e}")
            if os.path.exists(local_path):
                os.remove(local_path)
            return None
    
    def read_parquet(self, bucket_name, file_key, force_download=False):
        """
        Đọc file Parquet từ S3 hoặc cache local
        """
        local_path = self.download_from_s3(bucket_name, file_key, force_download)
        if not local_path:
            return None
        
        try:
            df = pd.read_parquet(local_path)
            
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
            print(f"Lỗi khi đọc file Parquet: {e}")
            return None

def preprocess_data(df):
    """Tiền xử lý dữ liệu: chuyển đổi ngày, sắp xếp dữ liệu"""
    df['Date'] = pd.to_datetime(df['Date'].astype(str), format='%Y%m%d')
    df = df.sort_values(['Ticker', 'Date'])
    return df

def filter_data(df, ticker=None, start_date=None, end_date=None):
    """Lọc dữ liệu theo mã cổ phiếu và khoảng thời gian"""
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
    """Vẽ biểu đồ nến từ DataFrame"""
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
    handler = S3ParquetHandler()
    
    # Nhập thông tin bucket và file từ người dùng
    bucket_name = os.getenv('S3_BUCKET_NAME', 'your-bucket-name')
    file_key = os.getenv('S3_FILE_KEY', 'stock_data_20062025.parquet')
    
    # Hỏi người dùng có muốn tải lại file không
    force_download = input("Tải lại file từ S3 ngay cả khi đã có cache? (y/n): ").strip().lower() == 'y'
    
    # Đọc file Parquet từ S3 hoặc cache local
    df = handler.read_parquet(bucket_name, file_key, force_download)
    if df is None:
        return
    
    # Tiền xử lý dữ liệu
    df = preprocess_data(df)
    
    # Lấy danh sách các mã cổ phiếu
    tickers = df['Ticker'].unique()
    print(f"\nTìm thấy {len(tickers)} mã cổ phiếu trong file:")
    
    while True:
        print("\nMENU:")
        print("1. Vẽ biểu đồ cho tất cả mã cổ phiếu")
        print("2. Vẽ biểu đồ cho một mã cổ phiếu cụ thể")
        print("3. Kiểm tra và cập nhật dữ liệu từ S3")
        print("4. Thoát")
        
        choice = input("Chọn chức năng (1-4): ").strip()
        
        if choice == '1':
            start_date = input("Nhập ngày bắt đầu (YYYY-MM-DD, để trống nếu không cần): ").strip()
            end_date = input("Nhập ngày kết thúc (YYYY-MM-DD, để trống nếu không cần): ").strip()
            
            for ticker in tickers:
                filtered_df = filter_data(df, ticker, start_date or None, end_date or None)
                if not filtered_df.empty:
                    plot_candlestick(filtered_df, f'Biểu đồ nến {ticker}')
                else:
                    print(f"Không có dữ liệu cho mã {ticker} trong khoảng thời gian đã chọn")
        
        elif choice == '2':
            ticker = input(f"Nhập mã cổ phiếu: ").strip().upper()
            if ticker not in tickers:
                print("Mã cổ phiếu không tồn tại trong dữ liệu!")
                continue
            
            start_date = input("Nhập ngày bắt đầu (YYYY-MM-DD, để trống nếu không cần): ").strip()
            end_date = input("Nhập ngày kết thúc (YYYY-MM-DD, để trống nếu không cần): ").strip()
            
            filtered_df = filter_data(df, ticker, start_date or None, end_date or None)
            plot_candlestick(filtered_df, f'Biểu đồ nến {ticker}')
        
        elif choice == '3':
            print("Kiểm tra cập nhật từ S3...")
            ## {"last_modified": 1750911842.0, "size": 27762991, "etag": "76a4deff581fdcb81849764b1ed37c4a-2"}
            local_path = handler._get_local_cache_path(bucket_name, file_key)
            if handler._has_file_changed(bucket_name, file_key, local_path):
                print("Phát hiện thay đổi trên S3, tiến hành tải file mới...")
                df = handler.read_parquet(bucket_name, file_key, force_download=True)
                if df is not None:
                    df = preprocess_data(df)
                    tickers = df['Ticker'].unique()
                    print("Dữ liệu đã được cập nhật!")
            else:
                print("Dữ liệu local đã là bản mới nhất, không cần tải lại")
        
        elif choice == '4':
            print("Kết thúc chương trình")
            break
        
        else:
            print("Lựa chọn không hợp lệ, vui lòng chọn lại")

if __name__ == "__main__":
    main()