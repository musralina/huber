import os

import openai
import logging
import schedule
import time
import telebot
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()
from process_csv import Process

# Configuration
TELEGRAM_BOT_TOKEN = os.getenv('BOT_TOKEN')
#OPENAI_API_KEY = os.getenv('BOT_TOKEN')
CHAT_ID = "your_chat_id"
OPENAI_API_KEY=os.getenv('OPENAI_API_KEY')

# Initialize bot and OpenAI
bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN)
client = openai.OpenAI(api_key=OPENAI_API_KEY)

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

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
    prompt = f"Analyze the following data summary and provide insights: {data_summary}"
    response = client.chat.completions.create(
        model="gpt-4",
        messages=[{"role": "system", "content": "You are a marketing specialist assistant. Answer always short and only in Russian language"},
                  {"role": "user", "content": prompt}]
    )
    return response.choices[0].message.content.strip()

def send_report_day():
    """Processes CSV, generates a report, and sends it to Telegram."""
    chat_id = get_chat_id()
    if not chat_id:
        logging.error("Cannot send report. Chat ID not found.")
        return
    
    df_day = Process.read_csv_file()
    if df_day is None:
        logging.error("Failed to read CSV file. Skipping report generation.")
        return  # Stop execution
    
    logging.info("Processing CSV file...")
    total_revenue = Process.calculate_total_revenue(df_day)
    total_revenue_per_employee = Process.calculate_revenue_per_employee(df_day)
    
    logging.info("Generating report...")
    data_summary = f"Total revenue: {total_revenue} and Revenue per employee: {total_revenue_per_employee}"
    report = generate_report(data_summary)
    
    logging.info("Sending report to Telegram...")
    bot.send_message(chat_id, f"Ежедневный отчёт:\n{report}")


# Send report immediately for testing
send_report_day()

# Schedule the report every day at 00:00
schedule.every().day.at("00:00").do(send_report_day)

logging.info("Bot started. Waiting for scheduled tasks...")

# Run the scheduler loop
while True:
    schedule.run_pending()
    time.sleep(60)
