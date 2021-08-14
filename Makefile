all: build

VERSION=$(shell python  -c 'from src.impira.version import VERSION; print(VERSION)')

build:
	python3 -m build

publish:
	python3 -m twine upload dist/impira-${VERSION}*

clean:
	rm -rf dist/*

develop:
	python3 -m venv venv
	source venv/bin/activate && python3 -m pip install --upgrade pip
	source venv/bin/activate && python3 -m pip install setuptools
	source venv/bin/activate && python3 -m pip install -e .
	echo 'run "source venv/bin/activate" to enter development mode'
