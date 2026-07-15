FROM ubuntu:22.04

LABEL maintainer="LazyLeech"
LABEL description="Telegram Torrent Leecher Bot"

# Prevent interactive prompts during build
ARG DEBIAN_FRONTEND=noninteractive

# Set timezone
ENV TZ=UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    python3-dev \
    ffmpeg \
    aria2 \
    file \
    p7zip-full \
    curl \
    ca-certificates \
    git \
    procps \
    tzdata \
    ntpdate \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Create working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Upgrade pip and install Python dependencies
RUN pip3 install --no-cache-dir --upgrade pip \
    && pip3 install --no-cache-dir -r requirements.txt

# Copy application code
COPY lazyleech/ ./lazyleech/
COPY ytdl/ ./ytdl/
COPY testwatermark.jpg .

# Create necessary directories
RUN mkdir -p /app/logs /app/downloads

# Copy entrypoint script
COPY docker-entrypoint.sh .
RUN chmod +x docker-entrypoint.sh

# Expose port (for potential future use)
EXPOSE 6800

# Health check - aria2 speaks JSON-RPC, so POST a getVersion call. A bare GET
# returns HTTP 400; a POST returns HTTP 200 (even when an rpc-secret is set, an
# auth error still comes back as HTTP 200 with a JSON error body), which is all
# we need to confirm the aria2 daemon is up and listening.
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD curl -fsS -X POST http://127.0.0.1:6800/jsonrpc \
        -d '{"jsonrpc":"2.0","id":"health","method":"aria2.getVersion","params":[]}' \
        || exit 1

# Entrypoint
ENTRYPOINT ["./docker-entrypoint.sh"]
