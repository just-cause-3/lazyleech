#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}   LazyLeech Docker Container Starting   ${NC}"
echo -e "${GREEN}========================================${NC}"

# Create required directories
mkdir -p /app/logs
mkdir -p /app/downloads
mkdir -p /app/data
mkdir -p /app/session

# Set proper permissions
chmod 777 /app/session 2>/dev/null || true

# Generate Aria2 secret if not provided
if [ -z "$ARIA2_SECRET" ]; then
    export ARIA2_SECRET=$(tr -dc 'A-Za-z0-9!"#$%&'\''()*+,-./:;<=>?@[\]^_`{|}~' </dev/urandom | head -c 100)
    echo -e "${YELLOW}Generated ARIA2_SECRET: ${ARIA2_SECRET:0:20}...${NC}"
fi

# Load best trackers for Aria2
TRACKERS=""
if [ -f "best_trackers.txt" ]; then
    TRACKERS=$(cat best_trackers.txt)
fi

# Validate required environment variables
echo -e "${YELLOW}Validating environment variables...${NC}"

if [ -z "$API_ID" ]; then
    echo -e "${RED}ERROR: API_ID is not set!${NC}"
    exit 1
fi

if [ -z "$API_HASH" ]; then
    echo -e "${RED}ERROR: API_HASH is not set!${NC}"
    exit 1
fi

if [ -z "$BOT_TOKEN" ]; then
    echo -e "${RED}ERROR: BOT_TOKEN is not set!${NC}"
    exit 1
fi

echo -e "${GREEN}All required environment variables are set!${NC}"

# Set defaults for optional variables
export ADMIN_CHATS="${ADMIN_CHATS:-441422215}"
export EVERYONE_CHATS="${EVERYONE_CHATS:--1001378211961}"
export PROGRESS_UPDATE_DELAY="${PROGRESS_UPDATE_DELAY:-5}"
export LEECH_TIMEOUT="${LEECH_TIMEOUT:-300}"
export MAGNET_TIMEOUT="${MAGNET_TIMEOUT:-60}"
export IGNORE_PADDING_FILE="${IGNORE_PADDING_FILE:-1}"

# Start Aria2 daemon
echo -e "${YELLOW}Starting Aria2 daemon...${NC}"
aria2c \
    --enable-rpc=true \
    --rpc-listen-all=true \
    --rpc-allow-origin-all \
    --rpc-secret="$ARIA2_SECRET" \
    -j5 \
    -x16 \
    -s16 \
    --continue=true \
    --max-connection-per-server=16 \
    --min-split-size=10M \
    --split=16 \
    --bt-max-peers=150 \
    --bt-tracker-connect-timeout=15 \
    --bt-tracker-timeout=15 \
    --enable-dht=true \
    --enable-dht6=true \
    --enable-peer-exchange=true \
    --bt-tracker="$TRACKERS" \
    --peer-id-prefix="-TR2770-" \
    --user-agent="Transmission/2.77" \
    --seed-time=0 \
    --dir=/app/downloads \
    > /app/logs/aria2.log 2>&1 &

# Wait for Aria2 to start
sleep 2

# Check if Aria2 is running
if pgrep -x "aria2c" > /dev/null; then
    echo -e "${GREEN}Aria2 daemon started successfully!${NC}"
else
    echo -e "${RED}Failed to start Aria2 daemon!${NC}"
    exit 1
fi

# Display configuration
echo -e "${GREEN}Configuration:${NC}"
echo -e "  API_ID: ${API_ID}"
echo -e "  ADMIN_CHATS: ${ADMIN_CHATS}"
echo -e "  EVERYONE_CHATS: ${EVERYONE_CHATS}"
echo -e "  PROGRESS_UPDATE_DELAY: ${PROGRESS_UPDATE_DELAY}s"
echo -e "  LEECH_TIMEOUT: ${LEECH_TIMEOUT}s"

# Check existing session
SESSION_FILE="/app/session/lazyleech.session"
if [ -f "$SESSION_FILE" ]; then
    echo -e "${YELLOW}Found existing session file, using it...${NC}"
else
    echo -e "${YELLOW}No existing session, will create new one...${NC}"
fi

# Give network time to stabilize
sleep 2

# Start the bot
echo -e "${GREEN}Starting LazyLeech Bot...${NC}"
exec python3 -m lazyleech
