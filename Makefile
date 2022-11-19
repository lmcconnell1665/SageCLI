setup:
	python3 -m venv ~/.SageCLI

install:
	pip install --upgrade pip &&\
		pip install -r requirements.txt

test:
	python -m pytest -vv test_function.py

lint:
	pylint --disable=R,C,W1203,W1202,E1101,W0104,E1120 *.py

all: install lint test