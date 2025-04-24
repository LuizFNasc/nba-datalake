import os
import boto3
import io
import pandas as pd
from botocore.client import Config
from dotenv import load_dotenv
from prefect import task

load_dotenv()

@task
def transform_to_gold():
    s3 = boto3.client(
        's3',
        endpoint_url=f"http://{os.getenv('MINIO_ENDPOINT')}",
        aws_access_key_id=os.getenv('MINIO_ACCESS_KEY'),
        aws_secret_access_key=os.getenv('MINIO_SECRET_KEY'),
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

    # Lê o Parquet da camada Silver
    response = s3.get_object(Bucket=os.getenv('MINIO_BUCKET'), Key='nba/silver/players.parquet')
    parquet_data = response['Body'].read()
    df = pd.read_parquet(io.BytesIO(parquet_data))

    # Geração de métrica simples: quantidade de jogadores ativos por letra inicial do nome
    df_active = df[df['is_active'] == True]
    df_active['initial'] = df_active['full_name'].str[0]
    df_gold = df_active.groupby('initial').size().reset_index(name='active_player_count')

    # Salvar como Parquet na camada Gold
    buffer = io.BytesIO()
    df_gold.to_parquet(buffer, index=False, engine='pyarrow')

    s3.put_object(
        Bucket=os.getenv('MINIO_BUCKET'),
        Key='nba/gold/active_players_by_initial.parquet',
        Body=buffer.getvalue()
    )

    print("✅ Camada Gold gerada com sucesso.")