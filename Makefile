all: build

VERSION=$(shell python  -c 'from src.impira.version import VERSION; print(VERSION)')

build:
	python3 -m build

publish: build
	python3 -m twine upload dist/impira-${VERSION}*

clean:
	rm -rf dist/*

develop:
	python3.8 -m venv venv
	bash -c 'source venv/bin/activate && python3 -m pip install --upgrade pip'
	bash -c 'source venv/bin/activate && python3 -m pip install setuptools'
	bash -c 'source venv/bin/activate && python3 -m pip install -e .'
	echo 'run "source venv/bin/activate" to enter development mode'

.PHONY: docs publish-docs
docs:
	# sphinx-apidoc -f -o docs/code src/impira
	cd docsrc && make html
	mkdir -p docs
	touch docs/.nojekyll
	cp -r docsrc/_build/html/* docs/

publish-docs:
	./docsrc/publish-docs.sh
