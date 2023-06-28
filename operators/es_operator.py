
import time
import json
from datetime import datetime, timedelta

import es_collector.eslibs.contented as Contented
import es_collector.eslibs.sender as sender

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.decorators import task
from airflow import models
from airflow.exceptions import AirflowSkipException

from elasticsearch import Elasticsearch, exceptions

#import telegram
#from telegram import InputMediaPhoto, InputMediaVideo

class ESCollector(BaseOperator):
    #server = ['server']

    @apply_defaults
    def __init__(self, host, port, *args, **kwargs):
        self.host = host
        self.port = port
        super().__init__(*args, **kwargs)

    def execute(self, context):
        return


    @task.python
    def send_messages(server, project_name, filter_index, messages, project, interval=1, post_type='forward', check_user=False):
        bot_token = project["bot_token"]
        chat_id = project["chat_id"]
        #bot = telegram.Bot(token = bot_token)
        bot = sender.TelegramWorker(bot_token)

        for msg in messages:
            ESCollector.set_last_msg(server, filter_index, project["filter_name"], msg["_source"]["time"])
            # Проверяем использовали уже пользователя 
            if check_user:
                tags = '#'+project_name
                user_index = 'tgusers_'+project_name
                if ESCollector.save_user(server, user_index, msg['_source']['sender'], tags) != True:
                    print('User Dont Save', msg['_source']['sender'])
                    continue
           
            if post_type == 'information_1':
                post = Contented.prepare_information1_post(msg["_source"])
            else:
                post = Contented.prepare_forward_post(msg["_source"])
            
            # Если post_type == 'information_1' и при этом текст отсутствует тогда post == None
            if post == None:
                print('Post is empty')
                continue
            
            #print("POST", post)
            
            if post['type']=='text':
                try:
                    msg = post['text']
                except:
                    print('Text in text post not set')
                    continue
            

            for cid in chat_id:              
                if post['type']=='videonote':
                    print("SEND VIDEONOTE", post)
                    response = bot.send_videonote(cid, post)
                elif post['type']=='text':
                    print("SEND TEXT", post, " TO CHAT ", cid)
                    response = bot.send_text(cid, msg)
                else:
                    print("SEND MEDIA", post)
                    response = bot.send_media_post(cid, post)

                #if response.status_code != 200:
                #    print("SEND ERROR:", response)
                #    raise ValueError(response)

                #print("Response: ", response)
                #print(response.text)
                time.sleep(interval)


    # Загружаем поисковый запрос пользователя
    @task.python
    def get_project(server, index, name):
      es = ESCollector.ESNew(server)
      query = {
          "query": {
                  "term": {
                      "_id": name
                  }
          }
      }
      result = es.search(index=index, body=query)
      if len(result["hits"]["hits"]) == 0:
          raise ValueError('Project %s not found' % name)

      params = result["hits"]["hits"][0]["_source"]
      print(params)
      return params 

        

    # Проверяем время актуальности задачи пользователя. Если задача не актуальна отправляем уведомление
    @task.python
    def date_checker(project, end_date, interval):
        # Получение экземпляра задачи по идентификаторам DAG, task_id и execution_date
        #interval = timedelta(minutes=10)
        current_date = datetime.now()
        h = 10
        one_day = timedelta(minutes=h)
        bot = sender.TelegramWorker(project["bot_token"])
        start = current_date + one_day
        end = start + interval
        # Проверяем суточный остаток
        if start <= end_date and end > end_date:
            info = 'ℹ #info\n\nРабота парсера прекратится через %s минут\n\nДля продления услуги свяжитесь с @vagerman' % h
            for cid in project['chat_id']:
                response = bot.send_text(cid, info)
                print(response)
            raise AirflowSkipException

        # Проверяем окончание веремени
        if current_date <= end_date and (current_date+interval) > end_date:
            info = 'ℹ #info\n\nРабота парсера прекращена\n\nДля продления услуги свяжитесь с @vagerman'
            for cid in project['chat_id']:
                response = bot.send_text(cid, info)
                print(response)
            raise AirflowSkipException

    # Загружаем поисковый запрос пользователя
    @task.python
    def get_filter(server, index, project):
      es = ESCollector.ESNew(server)
      query = {
          "query": {
                  "term": {
                      "_id": project["filter_name"]
                  }
          }
      }

      result = es.search(index=index, body=query)
      if len(result["hits"]["hits"]) == 0:
          raise ValueError('Filter %s not found' % project["filter_name"])

      #print(result["hits"]["hits"][0]["_source"]["query"])
      filter = result["hits"]["hits"][0]["_source"]
      print(filter)
      return filter



    # Применяем пользовательский запрос
    @task.python
    def apply_filter(server, query, project):
      es = ESCollector.ESNew(server)
      if query == None:
          raise ValueError("Empty Query")
      result = es.search(index=project["index"], body=query)
      if len(result["hits"]["hits"]) == 0:
          #raise ValueError('Messages %s not found' % project["filter_name"])
          print('Messages %s not found' % project["filter_name"])
          raise AirflowSkipException
      return result["hits"]["hits"]

#=================================================================================================================================

    def set_last_msg(server, index, filter_id, msg_id):
        es = ESCollector.ESNew(server) 
        query = {
            "doc": {
                "search_after": [msg_id]
            }
        }
        result = es.update(index=index, id=filter_id ,body=query)
        if result['result'] != "updated":
            print("Set Message ID Error. Respone :", result, "\nMessage ID :", msg_id)
            raise ValueError('Message ID %s dont set' % msg_id)


    def save_user(server, index, user, tags): 
        es = ESCollector.ESNew(server) 
        #user = Contented.prepare_user(msg['_source']['sender'], '#pankruxin')
        query = Contented.prepare_user(user, tags)
        try:
            result = es.create(index=index, id=user['id'] ,body=query)
            print("RESULT", result)
            return True
        except exceptions.ConflictError:
            return False
        
    def ESNew(server):
        #extra = json.loads(server.extra)
        if server.login != "":
            es = Elasticsearch(
                hosts=[{"host": server.host, "port": server.port, "scheme": server.schema}],
                ssl_show_warn=False,
                use_ssl = False,
                verify_certs = False,
                #ssl_assert_fingerprint=extra['fingerprint'],
                http_auth=(server.login, server.password)
            )
        else:
            es = Elasticsearch(
                hosts=[{"host": server.host, "port": server.port, "scheme": server.schema}],
                ssl_show_warn=False,
                use_ssl = False,
                verify_certs = False
            )
        
        print("ELASTIC")
        return es
