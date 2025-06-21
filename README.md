# NYC Weather ETL Script

This script pulls real-time weather data from Open-Meteo for 8 NYC locations and uploads it to an S3 bucket.

## Files

- `weather_fetcher.py`: The main ETL script.
- `requirements.txt`: Python dependencies.
- `setup.sh`: Optional install script for EC2.
- `crontab.txt`: Cron schedule.

## Usage

```bash
python3 weather_fetcher.py
