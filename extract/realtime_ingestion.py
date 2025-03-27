import os
import subprocess
import time
import pandas as pd
from datetime import datetime, timedelta

from utils import upload_to_bigquery, upload_frame_to_bigquery
from google.cloud import bigquery

# Configuración
symbol = "btcusd"
timeframe = "m1"
volume_units = "thousands"
format_type = "csv"
batch_size = 1
batch_pause = 5
retries = 3

def iso_format(dt):
    return dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')

def ingest_delayed_minute():
    try:
        now = datetime.utcnow().replace(second=0, microsecond=0)
        from_time = now - timedelta(minutes=60)
        to_time = now - timedelta(minutes=59)

        command = [
            'env', 'NODE_OPTIONS=--max-old-space-size=2048',
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
        subprocess.run(command)

        # Buscar el archivo CSV descargado
        folder = 'download'
        csv_file = None
        for file in os.listdir(folder):
            if file.endswith(".csv"):
                csv_file = os.path.join(folder, file)
                break

        if csv_file:
            upload_to_bigquery(csv_file, f"test-data-engenieer.bronze.{symbol}_{timeframe}_realtime")
            return {"status": 200, "message": "✅ CSV successfully uploaded to BigQuery.", "from": from_time, "to": to_time, "process": "real_time"}
        else:
            return {"status": 204, "message": "⚠️ CSV not found after download.", "from": from_time, "to": to_time, "process": "real_time"}

    except Exception as e:
        return {"status": 500, "message": f"❌ Error during ingestion: {e}", "from": from_time, "to": to_time, "process": "real_time"}

if __name__ == "__main__":
    print("⏱️  Real-time ingestion started with a 5-minute delay...\n")
    while True:
        try:
            status = ingest_delayed_minute()

            print(f"📊 Status → {status['status']}: {status['message']}\n")
            upload_frame_to_bigquery(pd.DataFrame([status]), f"test-data-engenieer.gold.extract_realtime_status")
            print(f"📊 Table Status → Updated\n")
            time.sleep(30)
        except KeyboardInterrupt:
            print("🛑 Real-time ingestion stopped.")
