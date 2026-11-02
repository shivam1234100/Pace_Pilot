PY ?= $(shell [ -x .venv/bin/python ] && echo .venv/bin/python || echo python)

up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f --tail=100

test:
	$(PY) -m pytest -q

test-integration:
	$(PY) -m pytest -q -m integration

lint:
	$(PY) -m ruff check .

.PHONY: up down logs test test-integration lint
