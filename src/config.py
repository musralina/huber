# config.py
from datetime import datetime
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


def format_russian_date(date_str):
    """Преобразует дату из 'YYYY-MM-DD' в формат '26 февраля 2025 года'."""
    months = {
        1: "января", 2: "февраля", 3: "марта", 4: "апреля",
        5: "мая", 6: "июня", 7: "июля", 8: "августа",
        9: "сентября", 10: "октября", 11: "ноября", 12: "декабря"
    }
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    return f"{date_obj.day} {months[date_obj.month]} {date_obj.year} года"