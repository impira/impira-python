all: build

build:
	python3 -m build

publish:
	python3 -m twine upload dist/*
