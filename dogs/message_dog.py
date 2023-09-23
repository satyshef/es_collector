# Скрипт сбора сообщений
from airflow import DAG
from airflow.hooks.base_hook import BaseHook

import es_collector.eslibs.es as Elastic
import es_collector.operators.project_operators as Project
import es_collector.operators.tgmsg_operators as Message
        
server = BaseHook.get_connection('elasticsearch_host2')
es = Elastic.New(server)

def run_dag(dag, project):
    if dag == None or project == None:
        return
    if project["succession"] == "dubler":
        succession_dubler(dag, project)
    else:
        succession_default(dag, project)


# порядок выполнения задач
def succession_default(dag, project):
    with dag: 
        check = Project.check_actual(project)
        filter = Project.get_filter(es, project, check)
        messages = Project.get_messages(es, project, filter)
        Message.send_messages(es, project, messages, 1)
        
def succession_dubler(dag, project):
    with dag:
        check = Project.check_actual(project)
        filter = Project.get_filter(es, project, check)
        messages_all = Project.get_messages(es, project, filter)
        messages = Message.dublicates_checker(es, project, messages_all)
        Message.send_messages(es, project, messages, 1)
