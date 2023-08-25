# Скрипт сбора пользователей
import json
import re
from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.decorators import task
from es_collector.operators.es_operator import ESCollector

     
SERVER = BaseHook.get_connection('elasticsearch_host2')
PROJECT_DIR = "/opt/airflow/dags/projects/"
DATA_DIR = '/opt/airflow/dags/data/'
RESULTFILE_EXTENSION = 'csv'

def load_project(name):
    if name == "" or name == None:
        return None

    path = PROJECT_DIR + name + '.json'
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
    if "succession" in project:
        succession = project["succession"]
    else:
        succession = "default"

    if succession == "extract_phones":
        succession_extract_phones(dag, project)
    else:
        succession_default(dag, project)
    


# Извлечение username. сценарий по умолчанию
def succession_default(dag, project):
    with dag: 
        file_path = get_filepath(project['name'])

        check = ESCollector.date_checker(project)
        filter = ESCollector.get_filter(SERVER, project, check)
        messages = ESCollector.get_messages(SERVER, project, filter)
        users = extract_users(messages)
        save_list = ESCollector.save_list_to_file(file_path, users)
        send_document = ESCollector.send_document(project, file_path)
        check >> filter >> messages >> users >> save_list >> send_document


# Извлечение username+message. Из message парсятся номера телефонов
def succession_extract_phones(dag, project):
    with dag: 
        file_path = get_filepath(project['name'])

        check = ESCollector.date_checker(project)
        filter = ESCollector.get_filter(SERVER, project, check)
        messages = ESCollector.get_messages(SERVER, project, filter)
        result = extract_phone_messages(messages)
        save_list = ESCollector.save_list_to_file(file_path, result)
        send_document = ESCollector.send_document(project, file_path)

        check >> filter >> messages >> result >> save_list >> send_document



# ========================== tasks ===============================
# Извлекаем usernames из сообщений
@task.python
def extract_users(messages):
    users = []
    for msg in messages:
        username = msg['sender']['username']
        if (username != '') and (username not in users):
            users.append(username)

    #print("USERS", users)
    return users

@task.python
def extract_phone_messages(messages):
    result = []
    # Анализ каждого сообщения и извлечение номеров
    for m in messages:
        message = m['content']['text']
        message = message.strip()
        message = message.replace("\n", "\\n")

        phone_numbers = parse_phone_numbers(message)
        for number in phone_numbers:
            if number != message:
                #print(number)
                line = number + ";" + message
                result.append(line)
    
    return result

# ======================== Service ===============================

# извлекаем номера телефонов из текста
def parse_phone_numbers(text):
    
    text = text.replace(" ", "")
    text = text.replace(" ", "")
    text = text.replace("-", "")
    text = text.replace("(", "")
    text = text.replace(")", "")
    # Шаблон для поиска номеров телефонов
    pattern = r'\+?\d{1,2}[-\s]?\(?\d{3}\)?[-\s]?\d{3}[-\s]?\d{2}[-\s]?\d{2}'

    # Используем регулярное выражение для поиска номеров
    phone_numbers = re.findall(pattern, text)

    # Заменяем номера, начинающиеся с 8, на номера, начинающиеся с +7
    formatted_numbers = []
    for number in phone_numbers:
        #formatted_number = re.sub(r'^\+?8', '+7', number)
        formatted_number = re.sub(r'^\+?[78]', '+7', number)
        formatted_numbers.append(formatted_number)

    return formatted_numbers

def get_filepath(name):
    current_datetime = datetime.now() - timedelta(days=1)
    current_date_string = current_datetime.strftime('%Y-%m-%d')
    file_path = DATA_DIR + name + '_' + current_date_string + '.' + RESULTFILE_EXTENSION
    return file_path