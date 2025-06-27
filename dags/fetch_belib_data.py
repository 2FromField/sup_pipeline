from datetime import datetime, timedelta
import requests
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

# === Config ===
API_URL = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/belib-points-de-recharge-pour-vehicules-electriques-donnees-statiques/records?limit=20"
DATA_DIR = os.path.join(os.getcwd(), "data")
OUTPUT_FILE = os.path.join(DATA_DIR, "belib_data.json")

# === Tâche ===
def fetch_and_save_data():
    response = requests.get(API_URL)
    response.raise_for_status()  # stop si erreur
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(response.text)
    print(f"✅ Données sauvegardées dans : {OUTPUT_FILE}")

# === DAG ===
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 6, 15),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "fetch_belib_data",
    default_args=default_args,
    description="Télécharge les données Belib et les sauvegarde",
    schedule=None,  
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_belib_api",
        python_callable=fetch_and_save_data,
    )
