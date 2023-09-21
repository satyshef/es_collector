from airflow.decorators import task
from airflow.exceptions import AirflowSkipException

from elasticsearch import exceptions

import es_collector.eslibs.project as Prolib  

#import es_collector.eslibs.contented as Contented   
     



@task.python
def check_dublicates_movies(es, project, movies):
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
            if len(res["hits"]["hits"]) == 0:
                result.append(movie)
            else:
                Prolib.save_last_message(project, movie["_source"])
        except exceptions.NotFoundError:
            result.append(movie)

    if len(result) == 0:
        raise AirflowSkipException
    
    return result

@task.python
def get_movies(es, project, query):
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
