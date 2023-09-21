import time

from airflow.decorators import task

import es_collector.eslibs.sender as Sender  
import es_collector.eslibs.project as Prolib
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

        for msg in messages:
            Prolib.save_last_message(project, msg)
            for cid in chat_id:
               #text = Contented.prepare_markdown(msg["content"]["text"])
               text =msg["content"]["text"]
               res = bot.send_text(cid, text, disable_preview)
               print("Send result", res)
               time.sleep(interval)
            log = {
                "name": msg["name"],
                "time": Prolib.current_date(),
            }
            Eslib.save_doc(es, project["project_index"], log)
