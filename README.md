# sample.dashboard

## Tạo user và phân quyền s3 tối thiểu cho user

* Đăng nhập vào [AWS](console.aws.amazon.com)
* Chọn IAM -> Policies -> Create Policy
    Name: least-vn-stock-data 
    Edit JSON:
    ```
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetObject",
                        "s3:PutObject",
                        "s3:DeleteObject"
                    ],
                    "Resource": "arn:aws:s3:::vn-stock-market/*"
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:ListBucket"
                    ],
                    "Resource": "arn:aws:s3:::vn-stock-market"
                }
            ]
        }
    ```
* Chọn IAM -> User -> Create User -> pds-stockdata -> Add Permissions -> Attach Policies directly --> least-vn-stock-data
* Chọn IAM -> User -> Security Credentials -> create access key
```toml
# AWS Credentials
AWS_ACCESS_KEY_ID='<your access key>'
AWS_SECRET_ACCESS_KEY='<your secret access key>'
AWS_REGION='<your bucket region>'
```

## Deploy Streamlit

* Đăng nhập vào [AWS](https://share.streamlit.io/)
* Create App --> Setting --> Secret :
```toml
# AWS Credentials
AWS_ACCESS_KEY_ID='<your access key>'
AWS_SECRET_ACCESS_KEY='<your secret access key>'
AWS_REGION='<your bucket region>'

# Application Settings
S3_BUCKET_NAME='vn-stock-market'
S3_FILE_KEY='stock_data_20062025.parquet'
LOCAL_CACHE_DIR='~/.s3_parquet_cache'
```
