uv-sync:
	uv sync --only-dev

install-editable-deps:
	uv pip install -e .

install-all-deps: uv-sync install-editable-deps
