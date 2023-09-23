#import json
import re

# извлекаем номера телефонов из текста
def parse_phone_numbers(text):
    
    text = text.replace(" ", "")
    text = text.replace(" ", "")
    text = text.replace("-", "")
    text = text.replace("(", "")
    text = text.replace(")", "")
    # Шаблон для поиска номеров телефонов
    pattern = r'\+?\d{1,2}[-\s]?\(?\d{3}\)?[-\s]?\d{3}[-\s]?\d{2}[-\s]?\d{2}'

    # Используем регулярное выражение для поиска номеров
    phone_numbers = re.findall(pattern, text)

    # Заменяем номера, начинающиеся с 8, на номера, начинающиеся с +7
    formatted_numbers = []
    for number in phone_numbers:
        #formatted_number = re.sub(r'^\+?8', '+7', number)
        formatted_number = re.sub(r'^\+?[78]', '+7', number)
        formatted_numbers.append(formatted_number)

    return formatted_numbers