VENV := .venv
PY := $(VENV)/bin/python
PIP := $(VENV)/bin/pip
RUFF := $(VENV)/bin/ruff
PYTEST := $(VENV)/bin/pytest

.PHONY: venv install dev test lint run prod

venv:
	python3 -m venv $(VENV)

install: venv
	$(PIP) install -r requirements.txt

dev: venv
	$(PIP) install -r requirements-dev.txt

test:
	$(PYTEST) -q

lint:
	$(RUFF) check .

run:
	$(PY) run.py

prod:
	gunicorn -w 4 -b 0.0.0.0:8000 wsgi:app