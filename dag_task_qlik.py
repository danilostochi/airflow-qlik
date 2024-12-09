import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.http_hook import HttpHook
from datetime import datetime, timedelta
import time

default_args = {
    'owner': 'Industry',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def check_reload_status(http_hook, base_url, headers, app_id):
    """
    Checks if there is a reload in progress (status: 'QUEUED' or 'RUNNING').
    """
    endpoint = f'/api/v1/reloads?appId={app_id}'
    response = requests.get(f"{base_url}{endpoint}", headers=headers)
    if response.status_code == 200:
        reloads = response.json().get('data', [])
        for reload in reloads:
            if reload['status'] in ['QUEUED', 'RUNNING']:
                return False  # Indicates that a reload is in progress
    else:
        raise ValueError(f"Error checking the status of application {app_id}: {response.status_code} - {response.text}")
    return True  # No reloads in progress

def monitor_reload_status(http_hook, base_url, headers, reload_id):
    """
    Monitors the reload status until it is completed or fails.
    """
    endpoint = f'/api/v1/reloads/{reload_id}'
    
    while True:
        response = requests.get(f"{base_url}{endpoint}", headers=headers)
        if response.status_code == 200:
            status = response.json().get('status', '')
            print(f"Current status of reload {reload_id}: {status}")
            if status in ['SUCCEEDED', 'FAILED', 'CANCELED', 'ABORTED']:
                return status  # Returns the final status
            time.sleep(300)  # Interval between status checks
        else:
            raise ValueError(f"Error monitoring the reload status {reload_id}: {response.status_code} - {response.text}")

def reload_qs_app(app_id, **kwargs):
    """
    Initiates the app reload in Qlik Sense and monitors its status.
    """
    http_hook = HttpHook(http_conn_id='qlik_api_conn', method='POST')
    connection = http_hook.get_connection(http_hook.http_conn_id)
    base_url = connection.host
    if not base_url.startswith('http'):
        base_url = f"http://{base_url}"
    endpoint = '/api/v1/reloads'
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {connection.extra_dejson["Authorization"].split(" ")[1]}'
    }
    
    # Check if there is a reload in progress
    if check_reload_status(http_hook, base_url, headers, app_id):
        data = {"appId": app_id}
        response = requests.post(f"{base_url}{endpoint}", headers=headers, json=data)

        if response.status_code in [200, 201]:
            reload_id = response.json().get('id')
            print(f"Reload successfully initiated for app {app_id}, reload ID: {reload_id}")

            # Monitor the reload status until a final status is obtained
            final_status = monitor_reload_status(http_hook, base_url, headers, reload_id)
            
            if final_status == 'SUCCEEDED':
                print(f"Reload of app {app_id} completed successfully.")
                return True  # Indicates success to Airflow
            elif final_status in ['FAILED', 'CANCELED', 'ABORTED']:
                raise ValueError(f"Reload of app {app_id} failed. Status: {final_status}")
        else:
            raise ValueError(f"Error initiating the reload of application {app_id}: {response.status_code} - {response.content}")
    else:
        print(f"Pending reload for application {app_id}. Skipping this reload.")
        return False  # Do not initiate the reload, but the task succeeded

with DAG(
    'qlik_refresh_app_dashboard_industry_daily',
    default_args=default_args,
    description='DAG to reload QlikSense applications',
    schedule_interval='0 5 * * *', 
    start_date=datetime(2024, 11, 1),
    catchup=False,
    tags=["Reload", "QlikSense Cloud", "Industry", "Application"],
) as dag:

    app_ids = [
        "f8312e46-92a1-41ae-bae3-931a07aa28f9", # 1.0 Gazin - Extractor Prog Tanks Industry
        "cc275a31-b484-4cc5-adbf-17f35b73bd86", # 1.1 Gazin - Extractor Balance Est Consumption Industry
        "54d4dde7-08df-4554-9fe7-12318590bbfe"  # Gazin - Industry Dashboard
    ]

    reload_tasks = []
    for i, app_id in enumerate(app_ids):
        task = PythonOperator(
            task_id=f'reload_app_{i+1}',
            python_callable=reload_qs_app,
            op_kwargs={'app_id': app_id},
        )
        reload_tasks.append(task)

    # Define the execution sequence (task1 >> task2 >> task3...)
    for i in range(len(reload_tasks) - 1):
        reload_tasks[i] >> reload_tasks[i + 1]
