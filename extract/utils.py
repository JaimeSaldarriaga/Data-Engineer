import os
import pandas as pd
from google.cloud import bigquery

def read_and_validate_csv(csv_path: str) -> pd.DataFrame:
    """
    Lee el archivo CSV y valida que exista y tenga datos.
    Lanza excepción si no cumple.
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"❌ El archivo no existe: {csv_path}")
    
    df = pd.read_csv(csv_path)
    
    if df.empty:
        raise ValueError(f"⚠️ El archivo CSV está vacío: {csv_path}")
    
    return df

def upload_to_bigquery(csv_path: str, table_id: str, schema=None) -> None:
    """
    Valida y sube un archivo CSV a BigQuery (reemplaza la tabla si ya existe).
    """
    df = read_and_validate_csv(csv_path)
    
    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        write_disposition = bigquery.WriteDisposition.WRITE_APPEND,
        autodetect = True if schema is None else False,
        schema = schema
    )

    job = client.load_table_from_dataframe(df, table_id, job_config = job_config)
    job.result()

    print(f"✅ Tabla {table_id} actualizada en BigQuery con {len(df)} fila(s).")


def upload_frame_to_bigquery(df, table_id: str, schema=None) -> None:
    """
    Valida y sube un archivo CSV a BigQuery (reemplaza la tabla si ya existe).
    """
    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        write_disposition = bigquery.WriteDisposition.WRITE_APPEND,
        autodetect = True if schema is None else False,
        schema = schema
    )

    job = client.load_table_from_dataframe(df, table_id, job_config = job_config)
    job.result()

    print(f"✅ Tabla {table_id} actualizada en BigQuery con {len(df)} fila(s).")
