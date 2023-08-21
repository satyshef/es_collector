# Скрипт сбора пользователей
import json   
from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from es_collector.operators.es_operator import ESCollector

     
server = BaseHook.get_connection('elasticsearch_host')
project_dir = "/opt/airflow/dags/projects/"

def load_project(name):
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
        
def run_dag(dag, project):
    if dag == None or project == None:
        return

    succession_default(dag, project)


# порядок выполнения задач
def succession_default(dag, project):
    with dag: 
        check = ESCollector.date_checker(project)
        filter = ESCollector.get_filter(server, project, check)
        messages = ESCollector.get_messages(server, project, filter)
        ESCollector.extract_users(messages)
        #ESCollector.send_messages(server, project, messages, 1)
        