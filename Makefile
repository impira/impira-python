all: build

VERSION=$(shell python  -c 'from src.impira.version import VERSION; print(VERSION)')

.PHONY: build
build:
	python3 -m build

publish: build
	python3 -m twine upload dist/impira-${VERSION}*

clean:
	rm -rf dist/*

develop:
	python3 -m venv venv
	bash -c 'source venv/bin/activate && python -m pip install --upgrade pip setuptools'
	bash -c 'source venv/bin/activate && python -m pip install -e .[all]'
	@echo 'Run "source venv/bin/activate" to enter development mode'

.PHONY: docs publish-docs
docs:
	# sphinx-apidoc -f -o docs/code src/impira
	cd docsrc && make html
	mkdir -p docs
	touch docs/.nojekyll
	cp -r docsrc/_build/html/* docs/

publish-docs:
	./docsrc/publish-docs.sh
