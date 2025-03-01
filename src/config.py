# config.py
from datetime import datetime
import numpy as np
import pandas as pd
import openai
from dotenv import load_dotenv
import os 


load_dotenv()
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
client = openai.OpenAI(api_key=OPENAI_API_KEY)
# Путь к файлам
CSV_FILE_PATH = "data/amocrm18fev.csv"
TEMP_FILE = "temp.xlsx"
FILE_URL = "https://amo.promoweb.kz/amo.xlsx"
CSV_FILE = "data/amocrm18fev.csv"

# config.py
CUMULATIVE_JSON = "data/cumulative_report.json"


# Статусы сделок
DEAL_STATUSES = {
    "successful": {"ОТПРАВКА В БУХГАЛТЕРИЮ", "НА ПРОВЕРКЕ", "ПОД ЗАКАЗ", "ОТЛОЖЕННЫЙ", "ОТПРАВЛЕНО", "УСПЕШНО РЕАЛИЗОВАНО"},
    "failed": {"ЗАКРЫТО И НЕ РЕАЛИЗОВАНО"},
    "in_progress": {"В РАБОТЕ | БРОНЬ", "ВЫСТАВИЛ СЧЕТ", "РАССЫЛКА", "БЛИЖЕ К СЕЗОНУ", "СДЕЛАЛИ ВТОРОЙ КОНТАКТ", "B2B + Гос закуп", "Квалификация +Прайс", "Лиды просроченные", "БИРЖА ЗАЯВОК"}
}

# Маржинальность
MARGIN_PERCENTAGE = 0.2


def convert_to_python_types(obj):
    """Converts numpy/pandas types to native Python types for JSON serialization."""
    if isinstance(obj, (pd.Timestamp, datetime)):
        return obj.strftime('%Y-%m-%d')
    elif isinstance(obj, (np.integer, np.int64)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64)):
        return float(obj)
    elif isinstance(obj, pd.Series):
        return obj.to_dict()
    elif isinstance(obj, dict):
        return {k: convert_to_python_types(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_python_types(item) for item in obj]
    else:
        return obj

def format_russian_date(date_str):
    """Преобразует дату из 'YYYY-MM-DD' в формат '26 февраля 2025 года'."""
    months = {
        1: "января", 2: "февраля", 3: "марта", 4: "апреля",
        5: "мая", 6: "июня", 7: "июля", 8: "августа",
        9: "сентября", 10: "октября", 11: "ноября", 12: "декабря"
    }
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    return f"{date_obj.day} {months[date_obj.month]} {date_obj.year} года"


def generate_report(data_summary):
    """Generates a report using OpenAI API."""
    prompt = f"""Analyze the following data summary and provide insights with the following structure: 
    1. Общая выручка за вчерашний день, сумма продаж (оборот), пример: Общий оборот отдела продаж за вчерашний день: 1 000 000 тенге или (10 продаж)
    2. Маржинальность 20%, напрмиер: Оборот за прошлый день: 1 000 000 тенге, прибыль 200 000 (20%)
    3. Количество успешных/проваленных сделок (здесь значения количественные, например 10 сделок успешных, 20 проваленных (закрыто нереализовано))
    4. Лучший и худший сотрудник считается только по количеству успешных сделок: {data_summary['successful_deals']}, при равном количестве учитывать сумму сделок. 
    Если и сумма, и количество у всех сотрудников равны, то нет худшего сутрудника, и нужно указать что они показали одинаковые результаты. 
    Наихудший сотрдник тот, кто показал наименьшее количество успешных сделок c продажами.
    Один и тот же сотрудник не может быть и лучшим и худшим сотрудником. Всегда указывай сумму сделок вместе с количеством.
    5. Сводная активность сотрудников (количество изменений в сделках). Например, Сколько взял новых сделок и сколько закрыл сделок
    6. Анализ эффективности по каждому сотруднику всегда зависит от количества успешных сделок каждого сотрудника: и считается по следующей формуле: (количество успешных сделок сотрудника)/(количество всех взятых сделок для сотрудника)*100)
Все данные бери отсюда:
    {data_summary}"""
    response = client.chat.completions.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": "You are a marketing specialist assistant. Calculate the best and worst employee based on their generated revenue. Answer always in Russian."},
            {"role": "user", "content": prompt}
        ]
    )
    return response.choices[0].message.content.strip()
