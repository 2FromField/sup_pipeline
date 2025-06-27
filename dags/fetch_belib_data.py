from datetime import datetime, timedelta
import requests
import os
import sys
from dotenv import load_dotenv

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


# Ajoute le dossier parent de "dags" dans le path Python
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from scripts.get_api import BelibAPIClient, MongoDBPipeline

# === Fonction à appeler dans Airflow ===
def fetch_and_store_data():
    load_dotenv()

    api_url = os.getenv("API_URL")
    if not api_url:
        raise ValueError("API_URL non défini dans .env")

    client = BelibAPIClient(api_url)
    data, total = client.fetch_data(limit=50)

    if data:
        mongo = MongoDBPipeline()
        mongo.insert_data_to_mongodb(data)
        mongo.close_connection()
    else:
        print("Aucune donnée récupérée")

# === DAG ===
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 6, 27),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "fetch_belib_data",
    default_args=default_args,
    description="Télécharge les données Belib et les sauvegarde",
    schedule="*/5 * * * *", # toutes les 5 minutes
    catchup=False, 
    tags=["belib", "api"]
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_belib_api",
        python_callable=fetch_and_store_data,
    )
