from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os

# Установите аргументы DAG
default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),  # Установите начальную дату выполнения
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Определите функцию для удаления логов старше трех дней
def delete_old_logs():
    log_dir = "/opt/airflow/logs"  # Укажите путь к директории с логами
    retention_period = timedelta(days=3)

    # Определите текущую дату
    current_date = datetime.now()

    # Пройдитесь по файлам в директории и удалите те, которые старше retention_period
    for root, dirs, files in os.walk(log_dir):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            file_modified_time = datetime.fromtimestamp(os.path.getmtime(file_path))
            if current_date - file_modified_time > retention_period:
                os.remove(file_path)
                #print(f"Удален файл: {file_path}")

# Создайте DAG
dag = DAG(
    'delete_old_logs',
    default_args=default_args,
    schedule_interval=timedelta(days=1),  # Установите интервал выполнения, например, ежедневно
    catchup=False,
    max_active_runs=1,
)

# Создайте PythonOperator для выполнения функции удаления логов
delete_logs_task = PythonOperator(
    task_id='delete_logs',
    python_callable=delete_old_logs,
    dag=dag,
)

# Установите зависимость между задачами
delete_logs_task

if __name__ == "__main__":
    dag.cli()
