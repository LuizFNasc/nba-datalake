import json
import os
from nba_api.stats.static import players
import boto3
from botocore.client import Config
from dotenv import load_dotenv
from prefect import task

# Carrega variáveis de ambiente
load_dotenv()
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")

# Inicializa cliente S3 (MinIO)
s3 = boto3.client(
    's3',
    endpoint_url=f"http://{MINIO_ENDPOINT}",
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)

# Função auxiliar para criar bucket se não existir
def create_bucket(bucket_name):
    buckets = s3.list_buckets().get('Buckets', [])
    if not any(b['Name'] == bucket_name for b in buckets):
        s3.create_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' criado.")
    else:
        print(f"Bucket '{bucket_name}' já existe.")

@task
def ingest_players():
    """
    Tarefa Prefect para coletar dados dos jogadores via nba_api e salvar em MinIO
    """
    # Garante que o bucket exista
    create_bucket(MINIO_BUCKET)

    # Coleta dados dos jogadores
    print("[ingest_players] Coletando dados dos jogadores...")
    all_players = players.get_players()
    json_data = json.dumps(all_players)

    # Define caminho e salva no MinIO
    key_path = f"nba/bronze/players.json"
    s3.put_object(Bucket=MINIO_BUCKET, Key=key_path, Body=json_data)
    print(f"[ingest_players] Arquivo salvo em MinIO: {MINIO_BUCKET}/{key_path}")
