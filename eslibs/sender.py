import telebot
import requests

class TelegramWorker:
    def __init__(self, bot_token) -> None:
        self.bot =  telebot.TeleBot(bot_token, parse_mode='Markdown')

    def send_text(self, chat_id, text):
       return self.bot.send_message(chat_id, text, disable_web_page_preview=True)

    def send_videonote(self, chat_id, post):
       url = post['video_link']
       if url == '':
           raise ValueError('Empty VideoNote link')
       #vid = telebot.types.InputVideoNote(url)
       #media.append(vid)
       data = requests.get(url).content
       #return None
       return self.bot.send_video_note(chat_id, data)

    def send_media_post(self, chat_id, post):
       # список объектов с параметрами для каждой картинки
       if post['text'] != None:
          caption = post['text']

       media = []
       for image_file in post['foto_link']:
          if len(media)==0 and caption != None:
              img = telebot.types.InputMediaPhoto(media=image_file, caption=caption)
          else:
              img = telebot.types.InputMediaPhoto(image_file)
          media.append(img)

       for video_file in post['video_link']:
          if len(media)==0 and caption != None:
              vid = telebot.types.InputMediaVideo(media=video_file, caption=caption)
          else:
              vid = telebot.types.InputMediaVideo(video_file)
          media.append(vid)
       #print("MEDIA ", media)
       if len(media) == 0:
           if caption != None:
               print("Send text instead of media")
               return self.send_text(chat_id, caption)
           else:
               raise ValueError('Media is empty')
       return self.bot.send_media_group(chat_id, media)
