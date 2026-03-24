FROM python:3.13-slim

ENV USER=ceda

RUN apt-get update && \
    apt-get -y upgrade && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        build-essential \
        curl && \
    rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

RUN useradd -m $USER
WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --extra frontend --extra all-backends

COPY src ./src
COPY data ./data

RUN chown -R $USER:$USER /app
USER $USER

ENV PATH="/app/.venv/bin:$PATH"

CMD ["streamlit", "run", "src/main.py", "--server.port=8000", "--server.address=0.0.0.0"]
