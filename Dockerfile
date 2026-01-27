FROM --platform=${BUILDPLATFORM} python:3.12.0-slim
ENV USER=ceda

# Upgrade system packages to install security updates
# The installer requires curl (and certificates) to download the release archive
# hadolint ignore=DL3008
RUN apt-get update && \
    apt-get -y upgrade && \
    apt-get install -y --no-install-recommends \
        vim=2:9.0.1378-2+deb12u2 \
        ca-certificates=20230311+deb12u1 \
        build-essential=12.9 && \
    apt-get install -y --no-install-recommends \
        jq \
        git \
        curl && \
    rm -rf /var/lib/apt/lists/*

## Upgrade pip to its latest release to speed up dependencies installation
COPY requirements.txt /
RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir /.cache /.venv && chmod 777 /.cache /.venv

COPY src /src
COPY pyproject.toml /

# We want to proceed with a less privileged user, after the OS is now ready.
RUN useradd -m $USER
WORKDIR /
COPY uv.lock  /
RUN chown -R $USER:$USER /uv.lock

USER $USER
ENV UV_CACHE_DIR=/.cache
RUN uv sync --frozen --no-dev --extra api # This will install the packages without starting the app yet.
ENV PATH="/.venv/bin:$PATH"

CMD ["uv", "run", "src/main.py"]
