.PHONY: all install dev test lint format proto clean build

all: install

install:
	uv sync

dev:
	uv sync --all-extras

test:
	uv run pytest

lint:
	uv run ruff check .

format:
	uv run ruff format .
	uv run ruff check --fix .

proto:
	wget https://raw.githubusercontent.com/alpacahq/marketstore/master/proto/marketstore.proto -O ./pymarketstore/proto/marketstore.proto
	uv run python -m grpc_tools.protoc -I./ --python_out=./ --grpc_python_out=./ ./pymarketstore/proto/marketstore.proto

clean:
	rm -rf dist/ build/ *.egg-info/ .pytest_cache/ .coverage htmlcov/ .ruff_cache/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

build:
	uv build
