PYTHON ?= python
COMPOSE ?= docker compose

.PHONY: install data pipeline test lint typecheck check app up down

install:
	$(PYTHON) -m pip install -r requirements.txt -r requirements-dev.txt

data:
	$(PYTHON) -m data_gen.generate

pipeline:
	$(PYTHON) -m pipeline.run

test:
	$(PYTHON) -m pytest

lint:
	$(PYTHON) -m ruff check .

typecheck:
	$(PYTHON) -m mypy

check:
	$(PYTHON) -m ruff format --check .
	$(PYTHON) -m ruff check .
	$(PYTHON) -m mypy
	$(PYTHON) -m pytest

app:
	$(PYTHON) -m streamlit run app/streamlit_app.py

up:
	$(COMPOSE) up --build

down:
	$(COMPOSE) down
