
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

ADVANCE_WARNING = 24
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
    def send_messages(server, project, messages, interval=1, check_user=False):
        bot_token = project["bot_token"]
        chat_id = project["chat_id"]
        
        bot = sender.TelegramWorker(bot_token)
        result = []
        for msg in messages:
            ESCollector.set_last_msg(server, project["filter_index"], project["filter_name"], msg["time"])
            # Проверяем использовали уже пользователя 
            if check_user:
                tags = '#' + project["name"]
                user_index = 'tgusers_' + project["name"]
                if ESCollector.save_user(server, user_index, msg['sender'], tags) != True:
                    print('User Dont Save', msg['sender'])
                    continue
           
            if project["post_template"] == 'template_1':
                post = Contented.prepare_template1_post(msg)
            elif project["post_template"] == 'template_2':
                post = Contented.prepare_template2_post(msg)
            elif project["post_template"] == 'template_3':
                post = Contented.prepare_template3_post(msg)
            elif project["post_template"] == 'template_4':
                post = Contented.prepare_template4_post(msg)
            else:
                post = Contented.prepare_forward_post(msg)

            
            # Если post_type == 'information_1' и при этом текст отсутствует тогда post == None
            if post == None:
                print('Post is empty')
                continue
            
            #print("POST", post)
            text = ''
            if post['type']=='text':
                if 'text' in post:
                    text = post['text']
                else:
                    print('Text in text post not set')
                    continue
            

            for cid in chat_id:              
                if post['type']=='videonote':
                    print("SEND VIDEONOTE", post)
                    response = bot.send_videonote(cid, post)
                elif text !='':
                    print("SEND TEXT", text, " TO CHAT ", cid)
                    response = bot.send_text(cid, text)
                else:
                    print("SEND MEDIA", post)
                    response = bot.send_media_post(cid, post)

                time.sleep(interval)
            ESCollector.save_message(server, project["customer_index"], msg)
            result.append(msg)
                #if response.status_code != 200:
                #    print("SEND ERROR:", response)
                #    raise ValueError(response)

                #print("Response: ", response)
                #print(response.text)
                
        return result

    # Загружаем поисковый запрос пользователя
    @task(task_id="load_project_dont_used")
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
    def date_checker(project):
        end_date = project['end_date']
        interval = project['interval']
        current_date = datetime.now()
        one_day = timedelta(hours=ADVANCE_WARNING)
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
    def get_filter(server, project):
      es = ESCollector.ESNew(server)
      query = {
          "query": {
                  "term": {
                      "_id": project["filter_name"]
                  }
          }
      }
      result = es.search(index=project["filter_index"], body=query)
      if len(result["hits"]["hits"]) == 0:
          raise ValueError('Filter %s not found' % project["filter_name"])

      #print(result["hits"]["hits"][0]["_source"]["query"])
      filter = result["hits"]["hits"][0]["_source"]
      print(filter)
      return filter



    # Применяем пользовательский запрос
    @task.python
    def get_messages(server, project, query):
      es = ESCollector.ESNew(server)
      if query == None:
          raise ValueError("Empty Query")
      result = es.search(index=project["index"], body=query)
      if len(result["hits"]["hits"]) == 0:
          #raise ValueError('Messages %s not found' % project["filter_name"])
          print('Messages %s not found' % project["filter_name"])
          raise AirflowSkipException
      sources = result["hits"]["hits"]
      result = []
      for s in sources:
          if 'content' not in s['_source']:
              print('Empty content')
              continue
          post = s['_source']
          # Если поле text не существует то присваевыем ему ''
          if 'text' not in post['content']:
              post['content']['text'] = ''
          result.append(post)

      return result
    
    @task.python
    def dublicates_checker(server, project, messages):
        result = []
        for msg in messages:
            if ESCollector.search_message(server, project["customer_index"], msg) == None:
                result.append(msg)
            else:
                print("Double", msg)
        
        return result

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
    
    def save_message(server, index, post): 
        es = ESCollector.ESNew(server) 
        #user = Contented.prepare_user(msg['_source']['sender'], '#pankruxin')
        try:
            result = es.index(index=index, body=post)
            print("Save Post", result)
            return True
        except exceptions.ConflictError:
            return False


    def search_message(server, index, message):
        
        text = message["content"]["text"]
        
        es = ESCollector.ESNew(server) 
        query = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match_phrase": {
                                "content.text": text
                            }
                        }
                    ]
                }
            }
        }
         
        # Ищем документ в пользовательском индексе
        result = es.search(index=index, body=query)
        if len(result["hits"]["hits"]) == 0:
            return None
        return result["hits"]["hits"][0]
        
    
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
