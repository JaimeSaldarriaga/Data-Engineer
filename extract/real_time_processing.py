import os
import shutil
import subprocess
import time
from datetime import datetime, timedelta

# Configuración
symbol = "btcusd"
timeframe = "m1"
volume_units = "thousands"
format_type = "csv"
batch_size = 1
batch_pause = 5
retries = 3

def iso_format(dt):
    """Datetime to format ISO for dukascopy-node"""
    return dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')

def ingest_delayed_minute():
    try:
        now = datetime.utcnow().replace(second=0, microsecond=0)
        from_time = now - timedelta(minutes=30)
        to_time = now - timedelta(minutes=29)

        command = [
            'npx', 'dukascopy-node',
            '-i', symbol,
            '-from', iso_format(from_time),
            '-to', iso_format(to_time),
            '--timeframe', timeframe,
            '--volumes',
            '--volume-units', volume_units,
            '--format', format_type,
            '--batch-size', str(batch_size),
            '--batch-pause', str(batch_pause),
            '--retries', str(retries)
        ]

        print(f"📡 Ingesting data for {symbol.upper()} from {iso_format(from_time)} to {iso_format(to_time)}...")
        subprocess.run(['env', 'NODE_OPTIONS=--max-old-space-size=2048'] + command)

        return {
            "status": 200,
            "message": "✅ Data successfully ingested."
        }   
    except Exception as e:
        return {
            "status": 400,
            "message": f"❌ Error during ingestion: {e}"
        } 

if __name__ == "__main__":
    print("⏱️  Real-time ingestion started with a 5-minute delay...\n")
    while True:
        status = ingest_delayed_minute()
        print(f"📊 Ingestion status → {status['status']}: {status['message']}\n")
        time.sleep(60)
