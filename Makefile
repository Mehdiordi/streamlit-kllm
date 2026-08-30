PYTHON ?= python3
VENV := .venv
BIN := $(VENV)/bin
PIP := $(BIN)/pip
STREAMLIT := $(BIN)/streamlit
APP := streamlit_app.py

.PHONY: help install run

help:
	@echo "make install  Create .venv and install requirements"
	@echo "make run      Start the Streamlit app"

$(VENV)/bin/python:
	$(PYTHON) -m venv $(VENV)
	$(PIP) install -U pip

install: $(VENV)/bin/python
	$(PIP) install -r requirements.txt

run: $(VENV)/bin/python
	$(STREAMLIT) run $(APP)
