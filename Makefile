PYTHON ?= python3
PIP ?= pip3

.PHONY: install test run run-raw run-trusted run-refined validate-layers query-dw producer clean

install:
	$(PIP) install -r requirements.txt

test:
	PYTHONPATH=. $(PYTHON) -m pytest -q

run:
	PYTHONPATH=src $(PYTHON) src/medallion_pipeline.py

run-raw:
	PYTHONPATH=src $(PYTHON) src/raw.py

run-trusted:
	PYTHONPATH=src $(PYTHON) src/trusted.py

run-refined:
	PYTHONPATH=src $(PYTHON) src/refined.py

validate-layers:
	PYTHONPATH=src $(PYTHON) src/validate_layers.py

query-dw:
	PYTHONPATH=src $(PYTHON) src/query_dw.py

producer:
	$(PYTHON) producer/generate_events.py --events 20 --output data/input/events.jsonl

clean:
	rm -rf data/output/* data/checkpoint/* .pytest_cache src/__pycache__ tests/__pycache__
