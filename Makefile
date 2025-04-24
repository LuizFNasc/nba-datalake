run-query:
	# <- aqui é um TAB, não espaço
	docker-compose exec ingestion python analysis/query_gold_duckdb.py

run-flow:
	# <- aqui também é um TAB
	docker-compose exec ingestion python -m flows.nba_pipeline
