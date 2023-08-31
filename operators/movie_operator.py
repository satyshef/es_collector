import time
from datetime import datetime, timezone

from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException

from elasticsearch import exceptions

from es_collector.operators.es_operator import ESCollector
import es_collector.eslibs.sender as Sender     
#import es_collector.eslibs.contented as Contented   
     
server = BaseHook.get_connection('elasticsearch_host2')

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
        check = ESCollector.date_checker(project)
        filter = ESCollector.get_filter(server, project, check)
        messages = get_movies(server, project, filter)
        #ESCollector.send_messages(server, project, messages, 1)
        
def succession_dubler(dag, project):
    with dag:
        check = ESCollector.date_checker(project)
        filter = ESCollector.get_filter(server, project, check)
        movies_source = get_movies(server, project, filter)
        movies_messages = check_dublicates_movies(server, project, movies_source)
        movies_messages = prepare_messages(movies_messages)
        send_messages(server, project, movies_messages, 1)



@task.python
def send_messages(server, project, messages, interval=1):
        bot_token = project["bot_token"]
        chat_id = project["chat_id"]
        if "disable_preview" in project:
            disable_preview = project["disable_preview"]
        else:
            disable_preview = True

        bot = Sender.TelegramWorker(bot_token)

        for msg in messages:
            ESCollector.set_last_message(project, msg)
            for cid in chat_id:
               #text = Contented.prepare_markdown(msg["content"]["text"])
               text =msg["content"]["text"]
               res = bot.send_text(cid, text, disable_preview)
               print("Send result", res)
               time.sleep(interval)
            log = {
                "name": msg["name"],
                "time": current_date(),
            }
            ESCollector.save_message(server, project["project_index"], log)


@task.python
def check_dublicates_movies(server, project, movies):
    es = ESCollector.ESNew(server)
    result = []
    for movie in movies:
        name = movie["_source"]["name"]
        query = {
          "query": {
               "match_phrase": {
                      "name": name
                }
           }
        }
        try:
            res = es.search(index=project["project_index"], body=query)
            print("QQQQQQQQQQ", query)
            if len(res["hits"]["hits"]) == 0:
                result.append(movie)
            else:
                ESCollector.set_last_message(project, movie["_source"])
        except exceptions.NotFoundError:
            result.append(movie)

    if len(result) == 0:
        raise AirflowSkipException
    
    return result

@task.python
def get_movies(server, project, query):
      es = ESCollector.ESNew(server)
      if query == None:
          raise ValueError("Empty Query")
      result = es.search(index=project["index"], body=query)
      if len(result["hits"]["hits"]) == 0:
          #raise ValueError('Messages %s not found' % project["filter_name"])
          print('Movies %s not found' % project["filter_name"])
          raise AirflowSkipException

      return result["hits"]["hits"]



@task.python
def prepare_messages(movies):
      # Проверить возможность избавиться от поля content
      result = []
      for m in movies:
          content = prepare_content(m)
          movie = {
             "content": {
                 "type": "text",
                 "text": content
             },
             "name": m["_source"]["name"],
             "sender": {"id": "movie_parser"},
             "time": m["_source"]["time"]
          }
          result.append(movie)
      #print(result)
      return result


def prepare_content(movie):
      m = movie["_source"]
      info = ""
      for key, value in m["info"].items():
        info += "\n*" + key + "* : " + value
      
      content = "*" + m["name"] + "*" 
      content += "\n\n" + m["description"]
      if info != "":
        content += "\n" + info
      content += "\n\n[🍿](" + m["url_youtube"] + ")"
      return content



def current_date():
    # Получение текущей даты и времени
    current_datetime = datetime.now(timezone.utc)
    # Форматирование даты в нужный формат
    formatted_date = current_datetime.strftime("%Y-%m-%dT%H:%M:%S.%f%z")
    # Преобразование временной зоны в формат "+0000"
    return formatted_date[:-2] + ":" + formatted_date[-2:]