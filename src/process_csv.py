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
        return df["Бюджет ₸"].sum()

    @staticmethod
    def calculate_revenue_per_employee(df):
        """Calculates, prints, and returns the total revenue for each employee."""
        if "Ответственный" not in df.columns or "Бюджет ₸" not in df.columns:
            logging.error("Required columns not found in CSV file!")
            return None
        
        revenue_per_employee = df.groupby("Ответственный")["Бюджет ₸"].sum()
        
        formatted_revenue = {employee: f"{revenue:,.2f} ₸" for employee, revenue in revenue_per_employee.items()}
    
        
        return formatted_revenue


