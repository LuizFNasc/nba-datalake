import json
import os
import boto3
from botocore.client import Config
from dotenv import load_dotenv

load_dotenv()

# ENV vars
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")

# Cliente MinIO
s3 = boto3.client(
    's3',
    endpoint_url=f"http://{MINIO_ENDPOINT}",
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)

def get_players_from_minio():
    response = s3.get_object(Bucket=MINIO_BUCKET, Key='nba/bronze/players.json')
    content = response['Body'].read().decode('utf-8')
    return json.loads(content)

def search_player_by_name(name_query):
    players = get_players_from_minio()
    results = [p for p in players if name_query.lower() in p['full_name'].lower()]
    return results

if __name__ == "__main__":
    search_term = input("Digite o nome do jogador a pesquisar: ")
    found_players = search_player_by_name(search_term)

    if found_players:
        print(f"\n🔍 Jogadores encontrados com '{search_term}':")
        for player in found_players:
            print(f"- {player['full_name']} (ID: {player['id']})")
    else:
        print("Nenhum jogador encontrado.")