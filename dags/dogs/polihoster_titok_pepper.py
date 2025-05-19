import os
import importlib
import es_collector.eslibs.dager as dager

from airflow import DAG

name = os.path.splitext(os.path.basename(__file__))[0]
project = dager.load_project(name, 1)
if project == None:
    print("Project not load")
    exit(1)

dag = dager.create_dag(project)

if "module" in project:
    module_name = project["module"]
else:
    print("Module not set")
    exit(1)

try:
    dog = importlib.import_module(module_name)
    dog.run_dag(dag, project)
except ImportError:
    print(f"Module {module_name} not found")


