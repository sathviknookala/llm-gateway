# One image, every mode: `docker run llmgw <mode>` (gateway, worker, sweeper,
# mock-provider, seed, check-ledger, create-key). Migrations run from Flyway's
# own image (see docker-compose.yml).

FROM python:3.13-slim AS build
COPY --from=ghcr.io/astral-sh/uv:0.8.22 /uv /bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never
WORKDIR /app
# Dependencies first, so code changes don't rebuild this layer.
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project
COPY README.md ./
COPY src ./src
RUN uv sync --locked --no-dev --no-editable

FROM python:3.13-slim
RUN useradd --system --uid 10001 --no-create-home llmgw
COPY --from=build /app/.venv /app/.venv
ENV PATH=/app/.venv/bin:$PATH PYTHONUNBUFFERED=1
USER llmgw
ENTRYPOINT ["llmgw"]
CMD ["--help"]
