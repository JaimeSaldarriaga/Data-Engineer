import pandas as pd
from utils import upload_frame_to_bigquery
from datetime import datetime, timedelta


def ingest_batch_news():
    try:
        # Leer CSV
        data = pd.read_csv("/workspaces/Data-Engineer/data/btc-news-recent-f.csv")

        # Convertir todas las columnas a string
        data = data.astype(str)

        # Subir a BigQuery
        upload_frame_to_bigquery(
            df=data,
            table_id="test-data-engenieer.bronze.btc_news_recent",  # Cambia según tu proyecto y dataset
        )

        return {
            "status": 200,
            "message": "✅ CSV successfully uploaded to BigQuery.",
            "from": datetime.utcnow().replace(second=0, microsecond=0),
            "to": datetime.utcnow().replace(second=0, microsecond=0),
            "process": "batch"
        }
    except Exception as e:
        return {
            "status": 500,
            "message": f"❌ Error uploading CSV to BigQuery: {str(e)}",
            "from": datetime.utcnow().replace(second=0, microsecond=0),
            "to": datetime.utcnow().replace(second=0, microsecond=0),
            "process": "batch"
    }

if __name__ == "__main__":
    status = ingest_batch_news()
    status_df = pd.DataFrame([status])

    upload_frame_to_bigquery(
        df = status_df,
        table_id = "test-data-engenieer.gold.extract_realtime_status"
    )