from google.cloud import bigquery

def run_silver_deduplication_query(project_id: str, dataset: str, bronze_table: str, silver_table: str) -> None:
    """
    Ejecuta una consulta en BigQuery que crea o reemplaza la tabla Silver con datos únicos y timestamp convertido.
    """
    client = bigquery.Client(project=project_id)

    query = f"""
    CREATE OR REPLACE TABLE `{project_id}.{dataset}.{silver_table}` AS (
        WITH deduplicated_data AS (
        SELECT
            -- Convertir de milisegundos a TIMESTAMP
            TIMESTAMP_MILLIS(CAST(timestamp AS INT64)) AS timestamp,
            CAST(open AS FLOAT64) AS open,
            CAST(high AS FLOAT64) AS high,
            CAST(low AS FLOAT64) AS low,
            CAST(close AS FLOAT64) AS close,
            CAST(volume AS FLOAT64) AS volume,
            ROW_NUMBER() OVER (
            PARTITION BY timestamp
            ORDER BY timestamp
            ) AS row_num
        FROM
            `test-data-engenieer.bronze.btcusd_m1_realtime`
        )
        SELECT
        timestamp, open, high, low, close, volume
        FROM
        deduplicated_data
        WHERE
        row_num = 1
        )
        ;
    """

    job = client.query(query)
    job.result()  # Espera a que termine la ejecución

    print(f"✅ Tabla `{silver_table}` actualizada exitosamente en el dataset `{dataset}`.")


if __name__ == "__main__":
    run_silver_deduplication_query("test-data-engenieer", "silver", "btcusd_m1_realtime", "btcusd_m1_realtime")