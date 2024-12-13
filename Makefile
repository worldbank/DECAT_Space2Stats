
all: build serve

build:
	sphinx-build docs _build/html -b html

serve:
	open _build/html/index.html
