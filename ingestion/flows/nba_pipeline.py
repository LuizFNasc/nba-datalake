from prefect import flow
#from tasks.ingest_teams import ingest_teams
from tasks.ingest_players import ingest_players
from tasks.transform_to_silver import transform_to_silver
from tasks.transform_to_gold import transform_to_gold

@flow(name="NBA Data Lake Pipeline")
def nba_pipeline():
    #ingest_teams()
    # 2) Ingestão de jogadores
    ingest_players()
    transform_to_silver()
    transform_to_gold()

if __name__ == "__main__":
    nba_pipeline()