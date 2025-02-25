import os
import logging
import pandas as pd

CSV_FILE_PATH = "/Users/aluamusralina/Documents/VScode/huber/data/amocrm18fev.csv"


class Process:
    @staticmethod
    def read_csv_file():
        """Reads the CSV file and returns a pandas DataFrame."""
        if not os.path.exists(CSV_FILE_PATH):
            logging.error("CSV file not found!")
            return None
        
        try:
            df = pd.read_csv(CSV_FILE_PATH, encoding="utf-8")
            return df
        except Exception as e:
            logging.error(f"Error reading CSV file: {e}")
            return None

    @staticmethod
    def calculate_total_revenue(df):
        """Calculates the total revenue from the 'Бюджет ₸' column."""
        if "Бюджет ₸" not in df.columns:
            logging.error("Column 'Бюджет ₸' not found in CSV file!")
            return None
        
        df["Бюджет ₸"] = df["Бюджет ₸"].replace(",", "", regex=True).astype(float)
        total = df["Бюджет ₸"].sum()
        margin = calculate_margin(total)
        return total, margin

    @staticmethod
    def calculate_revenue_per_employee(df):
        """Calculates, prints, and returns the total revenue for each employee, excluding 'Биржа заявок'."""
        if "Ответственный" not in df.columns or "Бюджет ₸" not in df.columns:
            logging.error("Required columns not found in CSV file!")
            return None
        
        # Исключаем строки, где "Ответственный" == "Биржа заявок"
        df_filtered = df[df["Ответственный"] != "Биржа заявок"]
        
        revenue_per_employee = df_filtered.groupby("Ответственный")["Бюджет ₸"].sum()
        
        formatted_revenue = {employee: f"{revenue:,.2f} ₸" for employee, revenue in revenue_per_employee.items()}

        return formatted_revenue

    @staticmethod
    def count_deal_stages(df):
        """Подсчитывает количество успешных, проваленных и находящихся в работе сделок."""
        
        if "Этап сделки" not in df.columns:
            logging.error("В DataFrame отсутствует колонка 'Этап сделки'!")
            return None

        # Определяем категории сделок
        successful_stages = {
            "ОТПРАВКА В БУХГАЛТЕРИЮ", "НА ПРОВЕРКЕ", "ПОД ЗАКАЗ", 
            "ОТЛОЖЕННЫЙ", "ОТПРАВЛЕНО", "УСПЕШНО РЕАЛИЗОВАНО"
        }
        failed_stages = {"ЗАКРЫТО И НЕ РЕАЛИЗОВАНО"}
        in_progress_stages = {
            "В РАБОТЕ | БРОНЬ", "ВЫСТАВИЛ СЧЕТ", "РАССЫЛКА", 
            "БЛИЖЕ К СЕЗОНУ", "СДЕЛАЛИ ВТОРОЙ КОНТАКТ", "B2B + Гос закуп", 
            "Квалификация +Прайс", "Лиды просроченные", "БИРЖА ЗАЯВОК"
        }

        # Подсчитываем количество сделок по категориям
        successful_count = df["Этап сделки"].isin(successful_stages).sum()
        failed_count = df["Этап сделки"].isin(failed_stages).sum()
        in_progress_count = df["Этап сделки"].isin(in_progress_stages).sum()

        return {
            "Успешные сделки": successful_count,
            "Проваленные сделки": failed_count,
            "Сделки в работе": in_progress_count
        }

    @staticmethod
    def calculate_employee_activity(df):
        """Подсчитывает активность сотрудников: взятые, закрытые и нереализованные сделки."""
        
        if "Ответственный" not in df.columns or "Дата редактирования" not in df.columns or "Дата закрытия" not in df.columns:
            logging.error("Отсутствуют необходимые колонки!")
            return None
        df["Дата закрытия"] = df["Дата закрытия"].replace("не закрыта", pd.NA)
        # Преобразуем даты в формат datetime для работы с ними
        df["Дата редактирования"] = pd.to_datetime(df["Дата редактирования"], format="%d.%m.%Y %H:%M:%S", errors="coerce")
        df["Дата закрытия"] = pd.to_datetime(df["Дата закрытия"], format="%d.%m.%Y %H:%M:%S", errors="coerce")

        # Отбираем сделки, которые были переведены с "Биржа заявок" на конкретного сотрудника
        df["Количество сделок, взятые в работу сотрудником"] = (df["Ответственный"] != "Биржа заявок")

        # Закрытые сделки (есть дата закрытия)
        df["Закрытая сделка"] = df["Дата закрытия"].notna()

        # Нереализованные сделки (закрытые со статусом "ЗАКРЫТО И НЕ РЕАЛИЗОВАНО")
        df["Закрытая и Нереализованная сделка"] = df["Закрытая сделка"] & (df["Этап сделки"] == "ЗАКРЫТО И НЕ РЕАЛИЗОВАНО")

        # Группируем по ответственным сотрудникам
        activity_summary = df.groupby("Ответственный").agg(
            {"Количество сделок, взятые в работу сотрудником": "sum", "Закрытая и Нереализованная сделка": "sum"}
        )
        print(activity_summary)
        # Преобразуем в словарь для удобства
        activity_summary_dict = activity_summary.to_dict(orient="index")
        return activity_summary_dict


def calculate_margin(total):
    margin = 0.2
    return total * margin

