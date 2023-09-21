from airflow import DAG
from airflow.hooks.base_hook import BaseHook

import es_collector.eslibs.es as Elastic
import es_collector.operators.project_operators as Project
import es_collector.operators.movie_operators as Movies
import es_collector.operators.telegram_operators as TG

     
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
        messages = Movies.get_movies(es, project, filter)
        #ESCollector.send_messages(server, project, messages, 1)
        
def succession_dubler(dag, project):
    with dag:
        check = Project.check_actual(project)
        filter = Project.get_filter(es, project, check)
        movies_source = Movies.get_movies(es, project, filter)
        movies_messages = Movies.check_dublicates_movies(es, project, movies_source)
        movies_messages = Movies.prepare_messages(movies_messages)
        TG.send_messages(es, project, movies_messages, 1)

