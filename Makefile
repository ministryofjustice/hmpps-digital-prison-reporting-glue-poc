
install:
	PIP_CONFIG_FILE=pip.conf pip install -r requirements.txt

install-dev:
	PIP_CONFIG_FILE=pip.conf pip install -r requirements-dev.txt


test:
	black --check -l 120 -t py37 src/
	python -m pytest -sv tests/

black:
	black -l 120 -t py37 src/

docs:
	pdoc3 --html -f -o ./doc src
	pdoc3 --pdf src > ./doc/doc.md


