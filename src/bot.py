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
from config import TEMP_FILE, FILE_URL, CSV_FILE, DEAL_STATUSES

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
        
        df = pd.read_excel(TEMP_FILE)
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
    4. Лучший и худший сотрудник считается по количеству успешных сделок, при равном количестве учитывать сумму сделок. 
    Если сумма и количества у всех равны, то нет худшего сутрудника, они показали одинаковые результаты. Наихудший сотрдник показал наименьшее количество сделок.
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

send_report_day()
schedule.every().day.at("00:00").do(send_report_day)
logging.info("Bot started. Waiting for scheduled tasks...")
while True:
    schedule.run_pending()
    time.sleep(60)
