import json
from datetime import datetime, timedelta

import es_collector.eslibs.sender as Sender

from airflow.decorators import task
from airflow.exceptions import AirflowSkipException

ADVANCE_WARNING = 24

# Проверяем время актуальности задачи пользователя. Если задача не актуальна отправляем уведомление
@task.python
def check_actual(project):
    end_date = project['end_date']
    interval = project['interval']
    current_date = datetime.now()
    first_term = timedelta(hours=ADVANCE_WARNING)
    bot = Sender.TelegramWorker(project["bot_token"])
    start = current_date + first_term
    end = start + interval
    # Проверяем суточный остаток
    if start <= end_date and end > end_date:
        info = 'ℹ #info\n\nРабота парсера прекратится через %s\n\nДля продления услуги свяжитесь с @vagerman' % first_term
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
    
    return True



# Загружаем поисковый запрос пользователя
# checked ??? - результат проверки актуальности проекта. Нужен для того что бы дождаться результата date_ckecker
@task.python
def get_filter(es, project, checked):
    if checked != True:
        raise AirflowSkipException
    
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
    if "size" in project:
        filter["size"] = project["size"]

    if "search_after" in project:
        filter["search_after"]= [project["search_after"]]

    if "sort" in project:
        filter["sort"] = project["sort"]

    print(filter)
    return filter

