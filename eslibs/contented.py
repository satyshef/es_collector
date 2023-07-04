import re
import es_collector.eslibs.webparser as webparser

def generate_link(destination):
        if destination["type"] == "user":
            if destination["username"] != "":
                return "https://t.me/%s" % destination["username"]
            return ""
        # return "tg://user?id=%d" % destination["id"]
        else :
            if destination["username"] != "":
                return "https://t.me/%s" % destination["username"]
            return "https://t.me/c/%d" % destination["id"] 

# название чата:имя отправителя:ссылка на сообщение:текст сообщения
def prepare_template1_post(source):
    if source["content"] == None:
        return None
    
    text = source["content"].get("text")
    if text == None:
        return None
    
    text = prepare_markdown(text)
    if text == '':
        print('Empty text')
        return ''
    
    chatLink = generate_link(source["location"])
    chatName = source["location"]["first_name"]
    chatName = re.sub(r'\[|\]', '', chatName)
    postLink = "[Сообщение](%s):\n" % source["content"]["link"]

    result = "[%s](%s)\n" % (chatName, chatLink)

    if source['sender']['id'] != source["location"]['id']:
        senderLink = generate_link(source["sender"])
        senderName = source["sender"]["first_name"]+" "+source["sender"]["last_name"] 
        senderName = re.sub(r'\[|\]', '', senderName)
        result += f"[%s](%s)\n\n" % (senderName, senderLink)
    else:
        result += "\n\n"

    result += postLink
    result += text
    #text +=  re.sub(r'\*|_|`|~', '', msg)
    
    post = source["content"]
    post["type"] = "text"
    post["text"] = result
    return post


# название чата:имя отправителя:текст сообщения:ссылка на сообщение
def prepare_template2_post(source):
    if source["content"] == None:
        return None
    
    text = source["content"].get("text")
    if text == None:
        return None
    
    text = prepare_markdown(text)
    if text == '':
        print('Empty text')
        return ''
    
    chatLink = generate_link(source["location"])
    chatName = source["location"]["first_name"]
    chatName = re.sub(r'\[|\]', '', chatName)
    postLink = "[ссылка на пост](%s)" % source["content"]["link"]

    result = "[%s](%s)\n" % (chatName, chatLink)

    if source['sender']['id'] != source["location"]['id']:
        senderLink = generate_link(source["sender"])
        senderName = source["sender"]["first_name"]+" "+source["sender"]["last_name"] 
        senderName = re.sub(r'\[|\]', '', senderName)
        result += f"[%s](%s)\n\n" % (senderName, senderLink)
    else:
        result += "\n\n"

    result += text
    #text +=  re.sub(r'\*|_|`|~', '', msg)
    result += "\n\n%s" % postLink
    post = source["content"]
    post["type"] = "text"
    post["text"] = result
    return post

# ссылка на сообщение:имя отправителя:текст сообщения
def prepare_template3_post(source):
    if source["content"] == None:
        return None

    text = source["content"].get("text")
    if text == None:
        return None

    text = prepare_markdown(text)
    if text == '':
        print('Empty text')
        return ''

    chatName = source["location"]["first_name"]
    chatName = re.sub(r'\[|\]', '', chatName)

    result = "[%s](%s)\n" % (chatName, source["content"]["link"])
  
    if source['sender']['id'] != source["location"]['id']:
        senderLink = generate_link(source["sender"])
        senderName = source["sender"]["first_name"]+" "+source["sender"]["last_name"] 
        senderName = re.sub(r'\[|\]', '', senderName)
        result += f"[%s](%s)\n\n" % (senderName, senderLink)
    else:
        result += "\n\n"

    result += text
    #text +=  re.sub(r'\*|_|`|~', '', msg)

    post = source["content"]
    post["type"] = "text"
    post["text"] = result
    return post

def prepare_template4_post(source):
    if source["content"] == None:
        print("None content")
        return None
  
    post = source["content"].copy()
    postLink = "[%s](%s)" % (source["location"]["first_name"], source["content"]["link"])
    parser = webparser.TelegramParser(post['link'])
  
    if post['type'] == 'videonote':
        post['video_link'] = parser.get_videonote()
        return post
    # FULL POST
    post["foto_link"] = parser.get_img_links()
    post["video_link"] = parser.get_video_links()

    #if 'text' not in post or not post['text']:
    # if post['text'] == '':
    #     text = parser.get_text()
    # else:

    text = post['text']
    if text != '':
        text = prepare_markdown(text)
        text = "🟢 %s \n \n%s" % (postLink, text)
        post['text'] = text
    return post


def prepare_forward_post(source):
    if source["content"] == None:
        print("None content")
        return None
    post = source["content"]
    parser = webparser.TelegramParser(post['link'])
    # print("PARSER", parser.soup)
    # return None
    if post['type'] == 'videonote':
        post['video_link'] = parser.get_videonote()
        return post
    # FULL POST
    #url = post["link"] + "?embed=1&mode=tme"
    
    post["foto_link"] = parser.get_img_links()
    post["video_link"] = parser.get_video_links()
    # response = requests.get(url)
    # soup = BeautifulSoup(response.content, 'html.parser')
    # post["foto_link"] = Webparser.parse_img_links(soup)
    # post["video_link"] = Webparser.parse_video_links(soup)

    #if 'text' not in post or not post['text']:
    if post['text'] == '':
        post['text'] = parser.get_text()
    
    if post['text'] != '':
        post['text'] = prepare_markdown(post['text'])
    return post





def prepare_user(user, tags):
    user = {
        'first_name' : user['first_name'],
        'last_name' : user['last_name'],
        'username' : user['username'],
        'phone' : user['phone'],
        'tags' : tags,
        #'link' : source['content']['link']
        #source['location']['first_name']
    }
    return user

def prepare_markdown(text):
    if text == '':
        return ''
    text = text.strip()
    text = text.replace('**__', '_')
    text = text.replace('__**', '_')
    text = text.replace('__', '_')
    text = text.replace('**', '*')
    text = text.replace('[*', '[')
    text = text.replace('*]', ']')

    #Проверяем парность форматирующих символов, если не четное количество тогда все удаляем
    if text.count("*") % 2 != 0:
        text = text.replace('*', '')
    if text.count("_") % 2 != 0:
        text = text.replace('_', '')
    if text.count("~") % 2 != 0:
        text = text.replace('~', '')

    result = ''
    for i in range(len(text)):
        is_formating = is_formatting_char(text, i)
        if is_formating != None and is_formating == False:
            result += "\\"
        result += text[i]

    text = text.replace('\n', ' \n')
    return text.strip()

# Проверка, что символ окружен другими символами или находится в начале или конце строки
def is_formatting_char(text, index):
    if text[index] != '*' and text[index] != '_' and text[index] != '~':
        return None

    if index == 0 or index == len(text) - 1:
        return True
    
    if text[index - 1] == ' ' or text[index + 1] == ' ' or text[index - 1] == '\n' or text[index + 1] == '\n':
        return True
    
    return False
