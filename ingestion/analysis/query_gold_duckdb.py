import duckdb
import os
from dotenv import load_dotenv

load_dotenv()

# Configurações
endpoint_url = f"http://{os.getenv('MINIO_ENDPOINT')}"
access_key    = os.getenv('MINIO_ACCESS_KEY')
secret_key    = os.getenv('MINIO_SECRET_KEY')
bucket        = os.getenv('MINIO_BUCKET')
path          = 'nba/gold/active_players_by_initial.parquet'
s3_url        = f"s3://{bucket}/{path}"

# Conecta ao DuckDB e carrega o plugin httpfs
con = duckdb.connect()
con.execute("""
  INSTALL httpfs;
  LOAD httpfs;
""")

# Ajusta para acessar o MinIO via HTTP sem SSL
con.execute(f"""
  SET s3_access_key_id='{access_key}';
  SET s3_secret_access_key='{secret_key}';
  SET s3_endpoint='http://{os.getenv("MINIO_ENDPOINT")}';
  SET s3_url_style='path';
  SET s3_use_ssl='true';
  SET s3_verify_ssl='false';
""")

# Executa a consulta
df = con.execute(f"""
    SELECT initial, active_player_count
    FROM read_parquet('{s3_url}')
    ORDER BY active_player_count DESC
""").df()

print("📊 Jogadores ativos por letra inicial:")
print(df)
