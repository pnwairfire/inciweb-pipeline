FROM python:3.12-slim
WORKDIR /app

ENV PYTHONPATH=/app

# Install uv from official image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

COPY pyproject.toml .
COPY inciweb_pipeline/ inciweb_pipeline/
RUN uv pip install --system --no-cache .

ENTRYPOINT ["python", "-m", "inciweb_pipeline"]
