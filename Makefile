.PHONY: install dev lint format typecheck test test-unit test-integration pipeline-daily pipeline-full backfill-elexon clean

install:
	pip install -e .

dev:
	pip install -e ".[dev]"

lint:
	ruff check src/ tests/

format:
	ruff format src/ tests/

format-check:
	ruff format --check src/ tests/

typecheck:
	mypy src/gridflow/

test:
	pytest tests/ -v --tb=short

test-unit:
	pytest tests/unit/ -v --tb=short

test-integration:
	pytest tests/integration/ -v --tb=short

pipeline-daily:
	python -m gridflow ingest elexon --all --last 24h
	python -m gridflow transform elexon --all --last 24h
	python -m gridflow build --all --last 7d
	python -m gridflow quality --all

pipeline-full:
	python -m gridflow ingest elexon --all --start 2024-01-01 --end 2024-12-31
	python -m gridflow transform elexon --all --start 2024-01-01 --end 2024-12-31
	python -m gridflow build --all --start 2024-01-01 --end 2024-12-31

backfill-elexon:
	python -m gridflow backfill elexon system_prices --start 2023-01-01 --end 2024-01-01 --chunk-days 7

init:
	python -m gridflow init

status:
	python -m gridflow status

clean:
	rm -rf data/ logs/ *.duckdb *.duckdb.wal
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
