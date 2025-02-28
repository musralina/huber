import os
import openai
import logging
import schedule
import time
import telebot
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
from process_csv import Process
from config import TEMP_FILE, FILE_URL, CSV_FILE, DEAL_STATUSES, CUMULATIVE_JSON
import json
import numpy as np


load_dotenv()

# Configuration
TELEGRAM_BOT_TOKEN = os.getenv('BOT_TOKEN')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
CHAT_ID = "your_chat_id"

# Initialize bot and OpenAI
bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN)
client = openai.OpenAI(api_key=OPENAI_API_KEY)

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def download_and_convert_xlsx():
    """Downloads the Excel file, converts it to CSV, and deletes the temp file."""
    try:
        response = requests.get(FILE_URL)
        response.raise_for_status()
        
        with open(TEMP_FILE, "wb") as f:
            f.write(response.content)
        
        df = pd.read_excel(
            TEMP_FILE,
            converters={
                'updated_at': lambda x: pd.to_datetime(x, unit='D', origin='1899-12-30') 
                if isinstance(x, (int, float)) else x
            }
        )
        df = df.iloc[1:]  # Remove the first row    
        df.to_csv(CSV_FILE, index=False, encoding='utf-8')
        logging.info("File successfully downloaded and converted to CSV.")
    
    except requests.RequestException as e:
        logging.error(f"Error downloading file: {e}")
    
    except Exception as e:
        logging.error(f"Error processing file: {e}")
    
    finally:
        if os.path.exists(TEMP_FILE):
            os.remove(TEMP_FILE)
            logging.info(f"Temporary file {TEMP_FILE} deleted.")

def get_chat_id():
    """Gets the chat ID by retrieving updates from Telegram."""
    updates = bot.get_updates()
    if updates and updates[-1].message:
        chat_id = updates[-1].message.chat.id
        logging.info(f"Retrieved Chat ID: {chat_id}")
        return chat_id
    else:
        logging.error("No chat updates found!")
        return None

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

def send_report_day():
    """Processes CSV, generates a report, and sends it to Telegram."""
    chat_id = get_chat_id()
    if not chat_id:
        logging.error("Cannot send report. Chat ID not found.")
        return
    
    download_and_convert_xlsx()
    
    df = Process.read_csv_file(CSV_FILE)
    if df is None:
        logging.error("Failed to read CSV file. Skipping report generation.")
        return
    
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    df['updated_at'] = pd.to_datetime(df['updated_at'])
    df_day = df[df['updated_at'].dt.strftime('%Y-%m-%d') == yesterday]
    df_day = df_day[df_day['contact_responsible_user_id'] != "Муратова Рината"]
    logging.info("Processing CSV file...")
    
    total_revenue, margin = Process.calculate_total_revenue(df_day)
    total_revenue_per_employee = Process.calculate_revenue_per_employee(df_day)
    deal_counts = Process.count_deal_stages(df_day)
    df_day = df_day[df_day['contact_responsible_user_id'] != "Биржа заявок"]
    employee_activity = Process.calculate_employee_activity(df_day)
    
    successful_deals = df_day[df_day['status_id'].isin(DEAL_STATUSES['successful'])].groupby("contact_responsible_user_id").agg({"price": "sum", "status_id": "count"}).rename(columns={"status_id": "successful_deals"})
    
    logging.info("Generating report...")
    data_summary = {
        "total_revenue": total_revenue,
        "margin": margin,
        "total_revenue_per_employee": total_revenue_per_employee,
        "deal_counts": deal_counts,
        "employee_activity": employee_activity,
        "successful_deals": successful_deals,
    }
    report = generate_report(data_summary)
    
    logging.info("Sending report to Telegram...")
    bot.send_message(chat_id, f"Ежедневный отчёт:\n{report}")
    save_cumulative_json(df_day, yesterday, total_revenue, margin, total_revenue_per_employee, deal_counts, employee_activity)  # Updated function call


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


def save_cumulative_json(df_day, date, total_revenue, margin, total_revenue_per_employee, deal_counts, employee_activity):
    """Saves cumulative daily reports into a JSON file with Cyrillic support."""
    successful_details = Process.get_deals_details(df_day, DEAL_STATUSES['successful'], "successful_deals")
    failed_details = Process.get_deals_details(df_day, DEAL_STATUSES['failed'], "failed_deals")

    # Convert all data to native Python types
    new_entry = {
        'updated_at': date,
        'total_revenue': convert_to_python_types(total_revenue),
        'margin': convert_to_python_types(margin),
        'total_revenue_per_employee': convert_to_python_types(total_revenue_per_employee),
        'deal_counts': convert_to_python_types(deal_counts),
        'employee_activity': convert_to_python_types(employee_activity),
        **convert_to_python_types(successful_details),
        **convert_to_python_types(failed_details)
    }

    # Read existing data or initialize empty list
    existing_data = []
    if os.path.exists(CUMULATIVE_JSON):
        try:
            # Check if file is not empty
            if os.path.getsize(CUMULATIVE_JSON) > 0:
                with open(CUMULATIVE_JSON, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
                # Ensure existing_data is a list
                if not isinstance(existing_data, list):
                    logging.warning("Corrupted JSON structure. Reinitializing.")
                    existing_data = []
            else:
                logging.info("JSON file is empty. Starting fresh.")
        except json.JSONDecodeError as e:
            logging.error(f"JSON decode error: {e}. Reinitializing file.")
            existing_data = []

    # Append new entry
    existing_data.append(new_entry)

    # Write back with UTF-8 and proper formatting
    with open(CUMULATIVE_JSON, 'w', encoding='utf-8') as f:
        json.dump(existing_data, f, indent=4, ensure_ascii=False)

    logging.info(f"Updated cumulative JSON for {date}")


def generate_historical_data():
    try:
        download_and_convert_xlsx()
        df = Process.read_csv_file(CSV_FILE)
        if df is None:
            logging.error("Failed to read CSV file in historical data.")
            return
        
        # Process dates
        df['updated_at'] = pd.to_datetime(df['updated_at'], errors='coerce')
        df = df.dropna(subset=['updated_at'])
        df['date'] = df['updated_at'].dt.strftime('%Y-%m-%d')  # Now a string column
        
        # Filter out excluded users
        df = df[df['contact_responsible_user_id'] != "Муратова Рината"]
        
        # Process each unique date string
        for date_str in df['date'].unique():  # date_str is already a string
            df_date = df[df['date'] == date_str]
            
            # Calculate metrics
            total_revenue, margin = Process.calculate_total_revenue(df_date)
            total_revenue_per_employee = Process.calculate_revenue_per_employee(df_date)
            deal_counts = Process.count_deal_stages(df_date)
            df_date_filtered = df_date[df_date['contact_responsible_user_id'] != "Биржа заявок"]
            employee_activity = Process.calculate_employee_activity(df_date_filtered)
            
            # Save using the date string directly
            save_cumulative_json(
                df_date, 
                date_str,  # Pass the string, no need for .strftime()
                total_revenue, 
                margin, 
                total_revenue_per_employee, 
                deal_counts, 
                employee_activity
            )
        
        logging.info("Historical data processing completed")
    
    except Exception as e:
        logging.error(f"Error processing historical data: {e}")

    
# generate_historical_data()    
send_report_day()
schedule.every().day.at("00:00").do(send_report_day)
logging.info("Bot started. Waiting for scheduled tasks...")
while True:
    schedule.run_pending()
    time.sleep(60)
