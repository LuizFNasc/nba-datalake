import os
import boto3
import io
import json
import pandas as pd
from botocore.client import Config
from dotenv import load_dotenv
from prefect import task

load_dotenv()

@task
def transform_to_silver():
    s3 = boto3.client(
        's3',
        endpoint_url=f"http://{os.getenv('MINIO_ENDPOINT')}",
        aws_access_key_id=os.getenv('MINIO_ACCESS_KEY'),
        aws_secret_access_key=os.getenv('MINIO_SECRET_KEY'),
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

    response = s3.get_object(Bucket=os.getenv('MINIO_BUCKET'), Key='nba/bronze/players.json')
    content = response['Body'].read().decode('utf-8')
    players = json.loads(content)

    df = pd.DataFrame(players)
    df = df[['id', 'full_name', 'first_name', 'last_name', 'is_active']]

    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine='pyarrow')

    s3.put_object(
        Bucket=os.getenv('MINIO_BUCKET'),
        Key='nba/silver/players.parquet',
        Body=parquet_buffer.getvalue()
    )

    print("✅ Dados convertidos para camada Silver.")