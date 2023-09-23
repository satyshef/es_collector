import time

from airflow.decorators import task
from airflow.exceptions import AirflowSkipException

import es_collector.eslibs.sender as Sender 
import es_collector.eslibs.project as Prolib
import es_collector.eslibs.contented as Contented
import es_collector.eslibs.es as Eslib

@task.python
def send_messages(es, project, messages, interval=1):
    bot_token = project["bot_token"]
    chat_id = project["chat_id"]
    if "disable_preview" in project:
        disable_preview = project["disable_preview"]
    else:
        disable_preview = True
    
    bot = Sender.TelegramWorker(bot_token)
    result = []
    for msg in messages:
        Prolib.save_last_message_time(project, msg)

        if project["post_template"] == 'template_1':
            post = Contented.prepare_template1_post(msg)
        elif project["post_template"] == 'template_2':
            post = Contented.prepare_template2_post(msg)
        elif project["post_template"] == 'template_3':
            post = Contented.prepare_template3_post(msg)
        elif project["post_template"] == 'template_4':
            post = Contented.prepare_template4_post(msg)
        elif project["post_template"] == 'demo_1':
            post = Contented.prepare_demo1_post(msg)
        elif project["post_template"] == 'forward_media':
            post = Contented.prepare_forward_media(msg)
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
                response = bot.send_text(cid, text, disable_preview)
            else:
                print("SEND MEDIA", post)
                response = bot.send_media_post(cid, post)

            time.sleep(interval)

        Eslib.save_doc(es, project["project_index"], msg)
        result.append(msg)
            
    return result


@task.python
def dublicates_checker(es, project, messages):
    ##
    result = []
    for msg in messages:
        last_msg = msg
        if Eslib.search_message(es, project["project_index"], msg, project["check_double_text"], project["check_double_user"]) == None:
            result.append(msg)
        else:
            print("Double", msg)
    #Если все дубли записываем время последнего сообщения в БД
    if len(result) == 0:
        if last_msg != None:
            Prolib.save_last_message_time(project, last_msg)
            #ESCollector.set_last_msg(server, project["filter_index"], project["filter_name"], last_msg)
        raise AirflowSkipException
    return result
