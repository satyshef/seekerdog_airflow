# ACTUAL
import json
from datetime import datetime, timedelta

from airflow import DAG
#from airflow.hooks.base_hook import BaseHook

#import es_collector.eslibs.contented as Contented   
     
DIRS = [
    "/opt/airflow/dags/projects/",
    "/opt/airflow/projects/"
]

def load_project(*args):
    
    if len(args) > 0:
        name = args[0]
    if len(args) > 1:
        project_dir = DIRS[args[1]]
    else:
        project_dir = DIRS[0]

    if name == "" or name == None:
        return None

    path = project_dir + name + '.json'
    try:
        with open(path, 'r') as file:
            project = json.load(file)

        if "enable" in project and project["enable"] == False:
            return None
        
        project['name'] = name
        project['path'] = path
        project['start_date'] = datetime.strptime(project['start_date'], '%Y-%m-%d %H:%M:%S')
        project['end_date'] = datetime.strptime(project['end_date'], '%Y-%m-%d %H:%M:%S')
        project['interval'] = timedelta(minutes=project['interval'])
        if "project_index" not in project:
            project["project_index"] = "project_" + name

        if "dag_id" not in project:
            project['dag_id'] = name
            
        return project
    except FileNotFoundError:
        print("Ошибка: Файл не найден.", path)
    except json.JSONDecodeError:
        print("Ошибка: Некорректный формат JSON.")
    except Exception as e:
        print("Произошла другая ошибка:", str(e))

    return None


def create_dag(project):
    if project == None:
        return None

    dag = DAG(
        project['dag_id'],
        tags=project['dag_tags'],
        schedule= project['interval'],
        start_date= project['start_date'],
        end_date= project['end_date'],
        catchup=False
    )
    return dag