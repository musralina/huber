# config.py

# Путь к файлам
CSV_FILE_PATH = "data/amocrm18fev.csv"
TEMP_FILE = "temp.xlsx"
FILE_URL = "https://amo.promoweb.kz/amo.xlsx"
CSV_FILE = "data/amocrm18fev.csv"
TEMP_FILE = "temp.xlsx"
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

