# Medical Telegram Warehouse

End-to-end data pipeline that scrapes Ethiopian medical business Telegram channels, stores raw data in a partitioned data lake, transforms it with dbt, and exposes it via a FastAPI service.

---

## Quick Start

### 1. Clone & configure

```bash
git clone <repo-url>
cd medical-telegram-warehouse
cp .env .env.local   # fill in real values — never commit .env
```

Edit `.env` and add your credentials:

| Variable | Where to get it |
|---|---|
| `TELEGRAM_API_ID` | https://my.telegram.org |
| `TELEGRAM_API_HASH` | https://my.telegram.org |
| `TELEGRAM_PHONE` | Your phone number (+251...) |
| `POSTGRES_*` | Your PostgreSQL instance |

### 2. Install dependencies

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Run the scraper

```bash
# Scrape all default channels (unlimited messages)
python src/scraper.py

# Scrape specific channels with a message limit
python src/scraper.py --channels CheMed123 lobelia4cosmetics --limit 200

# Scrape a date range
python src/scraper.py --start-date 2024-01-01 --end-date 2024-06-30
```

On first run, Telegram will send a verification code to your phone. Enter it in the terminal.

### 4. Run with Docker

```bash
docker-compose up --build
```

---

## Project Structure

```
medical-telegram-warehouse/
├── src/
│   └── scraper.py              # Task 1 — Telegram scraper
├── api/
│   ├── main.py                 # FastAPI app
│   ├── database.py             # SQLAlchemy async engine
│   └── schemas.py              # Pydantic models
├── medical_warehouse/          # dbt project
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/            # Raw → cleaned views
│   │   └── marts/              # Business-level tables
│   └── tests/
├── tests/
│   └── test_scraper.py         # Unit tests
├── data/
│   └── raw/
│       ├── telegram_messages/  # Partitioned JSON (YYYY-MM-DD/channel.json)
│       └── images/             # Photos (channel_name/message_id.jpg)
├── logs/                       # Scraper run logs & summaries
├── .env                        # Secrets — DO NOT COMMIT
├── docker-compose.yml
├── Dockerfile
└── requirements.txt
```

---

## Data Lake Layout

```
data/raw/
├── telegram_messages/
│   ├── 2024-06-15/
│   │   ├── CheMed123.json          # one JSON-lines file per channel per day
│   │   └── lobelia4cosmetics.json
│   └── 2024-06-16/
│       └── tikvahpharma.json
└── images/
    ├── CheMed123/
    │   ├── 12345.jpg
    │   └── 12346.jpg
    └── lobelia4cosmetics/
        └── 99001.jpg
```

Each line in a `.json` file is a self-contained JSON object with these fields:

| Field | Description |
|---|---|
| `message_id` | Telegram message ID |
| `date` | ISO-8601 timestamp (UTC) |
| `text` | Message text body |
| `views` | View count |
| `forwards` | Forward count |
| `media` | `null` or `{type, has_photo, photo_id}` |
| `channel` | Sanitised channel name |
| `scraped_at` | When the record was collected |

---

## Running Tests

```bash
pytest tests/ -v
```

---

## Channels Scraped

| Channel | Username |
|---|---|
| CheMed | `CheMed123` |
| Lobelia Cosmetics | `lobelia4cosmetics` |
| Tikvah Pharma | `tikvahpharma` |

Add more channels in `src/scraper.py` → `DEFAULT_CHANNELS` or pass them via `--channels`.
