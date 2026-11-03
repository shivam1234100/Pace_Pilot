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

loadtest:
	$(PY) loadtest/run.py $(LOADTEST_ARGS) --out results/atomic.json

# Restart the API with the racy counter, run, then always restore atomic mode.
loadtest-naive:
	COUNTER_MODE=naive docker compose up -d api
	$(PY) loadtest/run.py $(LOADTEST_ARGS) --out results/naive.json; status=$$?; \
	COUNTER_MODE=atomic docker compose up -d api; exit $$status

lint:
	$(PY) -m ruff check .

.PHONY: up down logs test test-integration loadtest loadtest-naive lint
