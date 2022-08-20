all: build

VERSION=$(shell python  -c 'from src.impira.version import VERSION; print(VERSION)')

.PHONY: build
build:
	python3 -m build

publish: build
	python3 -m twine upload dist/impira-${VERSION}*

clean:
	rm -rf dist/*

.PHONY: docs publish-docs
docs:
	# sphinx-apidoc -f -o docs/code src/impira
	cd docsrc && make html
	mkdir -p docs
	touch docs/.nojekyll
	cp -r docsrc/_build/html/* docs/

publish-docs:
	./docsrc/publish-docs.sh

VENV_INITIALIZED := venv/.initialized

${VENV_INITIALIZED}:
	rm -rf venv && python3 -m venv venv
	@touch ${VENV_INITIALIZED}

VENV_PYTHON_PACKAGES := venv/.python_packages

${VENV_PYTHON_PACKAGES}: ${VENV_INITIALIZED} setup.py
	bash -c 'source venv/bin/activate && python -m pip install --upgrade pip setuptools'
	bash -c 'source venv/bin/activate && python -m pip install -e .[dev]'
	@touch $@

VENV_PRE_COMMIT := venv/.pre_commit

${VENV_PRE_COMMIT}: ${VENV_PYTHON_PACKAGES}
	bash -c 'source venv/bin/activate && pre-commit install'
	@touch $@

.PHONY: develop
develop: ${VENV_PRE_COMMIT}
	@echo 'Run "source venv/bin/activate" to enter development mode'
