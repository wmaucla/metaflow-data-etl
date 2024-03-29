lints:
	poetry run black core/* tests/
	poetry run ruff check --fix core/* tests/
	poetry run mypy core/ tests/ --strict
	
unittests:
	poetry run pytest

notebook:
	poetry run python -m ipykernel install --name metaflow-data-etl --user
	cd notebooks/ && poetry run jupyter notebook

setup:
	poetry install
	poetry run mypy --install-types 

dag:
	poetry run python core/main.py run

production.run:
	docker-compose up --build run_core_service

test.run:
	docker-compose up --build test_service