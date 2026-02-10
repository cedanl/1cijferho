FROM python:3.12-slim

# Set user and home as ENV variables
ENV USERNAME=ceda \
    WORKSPACE=/workspace/vscode \
    USERHOME=/home/ceda


# Install EVERYTHING - no more missing deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    # Core compilers & tools
    build-essential cmake pkg-config \
    # Git & networking
    git curl wget unzip \
    # Python headers
    python3-dev python3-pip \
    # Graphics (matplotlib, pillow)
    libfreetype6-dev libpng-dev libjpeg-dev libwebp-dev libtiff-dev \
    # XML/HTML (lxml)
    libxml2-dev libxslt1-dev \
    # Database
    libpq-dev \
    # Audio
    libsndfile1-dev \
    # SSL/Crypto
    libssl-dev \
    # Fonts
    fonts-dejavu-core \
    # Skip arrow-dev (pyarrow uses wheels)
    && rm -rf /var/lib/apt/lists/*


# Create non-root user with home directory, and ensure workspace exists before chown
RUN useradd -m "$USERNAME" -s /bin/bash \
    && mkdir -p "$WORKSPACE" \
    && chown -R "$USERNAME":"$USERNAME" "$WORKSPACE"

# Install uv for the user
USER $USERNAME
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="$USERHOME/.local/bin:${PATH}"


WORKDIR $WORKSPACE

# Keep the container running
CMD ["tail", "-f", "/dev/null"]

