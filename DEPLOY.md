# LazyLeech Docker Deployment Guide

## 🚀 Quick Start

### 1. Prerequisites
- Docker Engine 20.10+
- Docker Compose 2.0+
- Telegram API credentials (https://my.telegram.org)
- Bot token from @BotFather

### 2. Setup

```bash
# Clone the repository
git clone https://github.com/your-username/lazyleech.git
cd lazyleech

# Copy example environment file
cp .env.example .env

# Edit .env with your credentials
nano .env
```

### 3. Configure Environment Variables

Edit `.env` file with your actual values:

```env
# Required
API_ID=12345678
API_HASH=your_api_hash_here
BOT_TOKEN=1234567890:ABCdefGHIjklMNOpqrsTUVwxyz
ADMIN_CHATS=your_telegram_user_id

# Optional
EVERYONE_CHATS=-100123456789
```

### 4. Run with Docker Compose

```bash
# Build and start (basic mode)
docker-compose up -d

# Build and start (with MongoDB for RSS)
docker-compose --profile rss up -d

# Build and start (with MongoDB for persistent Bunkr sessions)
docker-compose --profile sessions up -d

# View logs
docker-compose logs -f

# Stop
docker-compose down
```

---

## 📋 Configuration Details

### Required Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `API_ID` | Telegram API ID | `12345678` |
| `API_HASH` | Telegram API Hash | `abc123def456...` |
| `BOT_TOKEN` | Bot token from BotFather | `123456:ABC-...` |
| `ADMIN_CHATS` | Admin user chat IDs | `123456789 987654321` |

### Optional Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `EVERYONE_CHATS` | Public chat IDs | None |
| `PROGRESS_UPDATE_DELAY` | Progress update interval | `5` |
| `LEECH_TIMEOUT` | Download timeout (seconds) | `300` |
| `MAGNET_TIMEOUT` | Magnet timeout (seconds) | `60` |
| `DB_URL` | MongoDB URL for persistent Bunkr sessions and RSS | None |
| `LAZYLEECH_DB_NAME` | Mongo database containing LazyLeech collections | `ASWFeed` |
| `NYAA_RSS_LINKS` | RSS feed URLs | None |

---

## 🐳 Docker Commands

### Build
```bash
# Build image
docker build -t lazyleech:latest .

# Build with no cache
docker build --no-cache -t lazyleech:latest .
```

### Run
```bash
# Run with environment file
docker run -d \
  --name lazyleech \
  --env-file .env \
  -v ./downloads:/app/downloads \
  -v ./data:/app/data \
  -v ./logs:/app/logs \
  -p 6800:6800 \
  lazyleech:latest

# Run interactively (for debugging)
docker run -it --rm \
  --env-file .env \
  lazyleech:latest \
  /bin/bash
```

### Logs
```bash
# View logs
docker logs -f lazyleech

# View last 100 lines
docker logs --tail 100 lazyleech
```

### Cleanup
```bash
# Stop and remove container
docker-compose down

# Remove volumes
docker-compose down -v

# Remove all containers and images
docker-compose down --rmi all -v
```

---

## 📁 Volume Structure

```
./
├── downloads/     # Downloaded files (temporary)
├── data/         # User thumbnails and watermarks
├── logs/         # Application and Aria2 logs
└── ytdl/         # YouTube download cache
```

---

## 🔧 Advanced Configuration

### Using MongoDB for RSS Auto-Download

1. Start with RSS profile:
```bash
docker-compose --profile rss up -d
```

2. Configure environment:
```env
DB_URL=mongodb://lazyleech:lazyleech_password@mongodb:27017/lazyleech?authSource=admin
NYAA_RSS_LINKS=https://nyaa.si/?page=rss&c=0_0&f=0&u=UploaderName
RSS_RECHECK_INTERVAL=5
```

### Resource Limits

The default configuration sets:
- Memory limit: 2GB
- CPU limit: 2 cores
- Memory reservation: 512MB
- CPU reservation: 0.5 cores

Adjust in `docker-compose.yml` if needed.

---

## 🐛 Troubleshooting

### Bot won't start
```bash
# Check logs
docker-compose logs -f

# Verify environment variables
docker-compose config

# Rebuild
docker-compose build --no-cache
docker-compose up -d
```

### Aria2 connection issues
```bash
# Check if Aria2 is running
docker exec lazyleech pgrep -x aria2c

# Test Aria2 RPC
curl -d '{"jsonrpc":"2.0","id":"test","method":"aria2.getVersion"}' \
  http://localhost:6800/jsonrpc
```

### Permission issues
```bash
# Fix permissions
sudo chown -R $USER:$USER downloads/ data/ logs/ ytdl/
```

---

## 📊 Health Check

The container includes a health check that verifies Aria2 RPC is responding:

```bash
# Check container health
docker inspect lazyleech | grep -A 5 Health
```

---

## 🔄 Backup & Restore

### Backup
```bash
# Backup volumes
tar -czf lazyleech-backup.tar.gz downloads/ data/ ytdl/
```

### Restore
```bash
# Restore volumes
tar -xzf lazyleech-backup.tar.gz
```

---

## 📝 Notes

1. **File Persistence**: Downloads are stored in `./downloads/` and automatically cleaned after upload to Telegram
2. **User Data**: Thumbnails and watermarks are stored in `./data/`
3. **Logs**: Available in `./logs/` for debugging
4. **Port 6800**: Aria2 RPC port (useful for external management tools)

---

## 🆘 Support

For issues, please check:
1. Docker logs: `docker-compose logs -f`
2. Aria2 logs: `./logs/aria2.log`
3. Bot logs: `./logs/lazyleech.log`
