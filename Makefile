PYTHON ?= python3
.PHONY: test demo
test:
	$(PYTHON) -m unittest discover -s tests -v
demo:
	$(PYTHON) -m runledger demo
