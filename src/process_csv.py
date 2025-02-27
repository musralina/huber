import os
import logging
import pandas as pd
from config import DEAL_STATUSES, MARGIN_PERCENTAGE

class Process:
    @staticmethod
    def read_csv_file(CSV_FILE_PATH):
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
        """Calculates the total revenue from the 'price' column."""
        if "price" not in df.columns:
            logging.error("Column 'price' not found in CSV file!")
            return None
        
        df["price"] = df["price"].replace(",", "", regex=True).astype(float)
        total = df["price"].sum()
        margin = calculate_margin(total)
        return total, margin

    @staticmethod
    def calculate_revenue_per_employee(df):
        """Calculates, prints, and returns the total revenue for each employee, excluding 'Биржа заявок'."""
        if "Ответственный" not in df.columns or "price" not in df.columns:
            logging.error("Required columns not found in CSV file!")
            return None
        
        # Исключаем строки, где "Ответственный" == "Биржа заявок"
        df_filtered = df[df["contact_responsible_user_id"] != "Биржа заявок"]
        
        revenue_per_employee = df_filtered.groupby("contact_responsible_user_id")["price"].sum()
        
        formatted_revenue = {employee: f"{revenue:,.2f} ₸" for employee, revenue in revenue_per_employee.items()}

        return formatted_revenue

    @staticmethod
    def count_deal_stages(df):
        """Подсчитывает количество успешных, проваленных и находящихся в работе сделок."""
        
        if "status_id" not in df.columns:
            logging.error("В DataFrame отсутствует колонка 'status_id'!")
            return None

        # Определяем категории сделок
        successful_stages = DEAL_STATUSES["successful"]
        failed_stages = DEAL_STATUSES["failed"]
        in_progress_stages = DEAL_STATUSES["in_progress"]

        # Подсчитываем количество сделок по категориям
        successful_count = df["status_id"].isin(successful_stages).sum()
        failed_count = df["status_id"].isin(failed_stages).sum()
        in_progress_count = df["status_id"].isin(in_progress_stages).sum()

        return {
            "Успешные сделки": successful_count,
            "Проваленные сделки": failed_count,
            "Сделки в работе": in_progress_count
        }

    @staticmethod
    def calculate_employee_activity(df):
        """Подсчитывает активность сотрудников: взятые, закрытые и нереализованные сделки."""
        
        if "contact_responsible_user_id" not in df.columns or "updated_at" not in df.columns or "closed_at" not in df.columns:
            logging.error("Отсутствуют необходимые колонки!")
            return None
        # df["closed_at"] = df["closed_at"].replace("не закрыта", pd.NA)
        # Преобразуем даты в формат datetime для работы с ними
        df["updated_at"] = pd.to_datetime(df["updated_at"], format="%d.%m.%Y %H:%M:%S", errors="coerce")
        df["closed_at"] = pd.to_datetime(df["closed_at"], format="%d.%m.%Y %H:%M:%S", errors="coerce")

        # Отбираем сделки, которые были переведены с "Биржа заявок" на конкретного сотрудника
        df["Количество сделок, взятые в работу сотрудником"] = (df["contact_responsible_user_id"] != "Биржа заявок")

        # Закрытые сделки (есть дата закрытия)
        df["closed_at"] = df["closed_at"].notna()

        # Нереализованные сделки (закрытые со статусом "ЗАКРЫТО И НЕ РЕАЛИЗОВАНО")
        df["Закрытая и Нереализованная сделка"] = df["closed_at"] & (df["status_id"] == "ЗАКРЫТО И НЕ РЕАЛИЗОВАНО")

        # Группируем по ответственным сотрудникам
        activity_summary = df.groupby("contact_responsible_user_id").agg(
            {"Количество сделок, взятые в работу сотрудником": "sum", "Закрытая и Нереализованная сделка": "sum"}
        )
        print(activity_summary)
        # Преобразуем в словарь для удобства
        activity_summary_dict = activity_summary.to_dict(orient="index")
        return activity_summary_dict


def calculate_margin(total):
    margin = MARGIN_PERCENTAGE
    return total * margin
