from nba_api.stats.static import teams
import json
import boto3
import os
from botocore.client import Config
from dotenv import load_dotenv
from prefect import task

load_dotenv()

@task
def ingest_teams():
    # Inicializa cliente S3/MinIO
    s3 = boto3.client(
        's3',
        endpoint_url=f"http://{os.getenv('MINIO_ENDPOINT')}",
        aws_access_key_id=os.getenv('MINIO_ACCESS_KEY'),
        aws_secret_access_key=os.getenv('MINIO_SECRET_KEY'),
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

    # Busca todos os times da NBA e WNBA
    all_teams = {
        'nba' : teams.get_teams(),
        'wnba': teams.get_wnba_teams()
    }
    json_data = json.dumps(all_teams, ensure_ascii=False)

    # Garante que o bucket existe
    bucket_name = os.getenv('MINIO_BUCKET')
    existing = [b['Name'] for b in s3.list_buckets().get('Buckets', [])]
    if bucket_name not in existing:
        s3.create_bucket(Bucket=bucket_name)

    # Grava JSON na camada bronze
    s3.put_object(
        Bucket=bucket_name,
        Key='nba/bronze/teams.json',
        Body=json_data.encode('utf-8')
    )

    print("✅ Estrutura dos times ingerida para camada Bronze em nba/bronze/teams.json")
