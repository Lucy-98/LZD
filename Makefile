# Tuong duong scripts/stack.ps1 cho ai dung bash/WSL/Linux.
.PHONY: help init build up up-all down reset ps status logs health test redis duckdb reconstruction track-a snapshot load-test

help:
	@echo "make init      - tao .env + build image"
	@echo "make up        - bat core + observability"
	@echo "make up-all    - bat toan bo stack"
	@echo "make down      - dung (giu du lieu)"
	@echo "make reset     - dung + xoa volume"
	@echo "make status    - trang thai + link UI"
	@echo "make logs s=<service> - xem log 1 service"
	@echo "make health    - goi thu cac endpoint"
	@echo "make test      - chay unit test"
	@echo "make snapshot  - ghi reconstruction snapshot vao docs/"
	@echo "make track-a   - gen Track A raw events tu data/full_trainset.csv (limit=1000)"
	@echo "make load-test - ban traffic vao inference API"

init:
	@test -f .env || cp .env.example .env
	@mkdir -p airflow/logs airflow/plugins data
	docker compose --profile all build

build:
	docker compose --profile all build

up:
	docker compose --profile core --profile obs up -d
	@$(MAKE) status

up-all:
	docker compose --profile all up -d
	@$(MAKE) status

down:
	docker compose --profile all down

reset:
	docker compose --profile all down -v
	rm -rf airflow/logs/*

ps status:
	@docker compose --profile all ps
	@echo ""
	@echo "Airflow       http://localhost:8080   (admin/admin)"
	@echo "Grafana       http://localhost:3000   (admin/admin)"
	@echo "Kafka UI      http://localhost:8082"
	@echo "MinIO         http://localhost:9001   (minioadmin/minioadmin123)"
	@echo "MLflow        http://localhost:5000"
	@echo "Prometheus    http://localhost:9090"
	@echo "RedisInsight  http://localhost:5540"
	@echo "Inference API http://localhost:8000/docs"

logs:
	docker compose logs -f --tail=200 $(s)

health:
	@for u in http://localhost:8080/health http://localhost:3000/api/health \
	          http://localhost:9090/-/healthy http://localhost:9000/minio/health/live \
	          http://localhost:3100/ready http://localhost:8000/health; do \
		printf "%-45s" "$$u"; \
		curl -s -o /dev/null -w "%{http_code}\n" $$u || echo "LOI"; \
	done

test:
	python -m pytest tests/ -v

reconstruction:
	PYTHONPATH=src python -m lzd_pipeline.reconstruction.e2e

track-a:
	PYTHONPATH=src python -m lzd_pipeline.reconstruction.track_a_batch --limit 1000 --verify-limit 50

snapshot:
	PYTHONPATH=src python -m lzd_pipeline.reconstruction.snapshot

redis:
	docker compose exec redis redis-cli

duckdb:
	docker compose exec airflow-scheduler python -c "from lzd_pipeline.common.clients import duckdb_conn;\
	  con=duckdb_conn(read_only=True).__enter__();print(con.execute('SHOW ALL TABLES').fetchdf())"

load-test:
	python scripts/load_test.py --rps 20 --duration 60
