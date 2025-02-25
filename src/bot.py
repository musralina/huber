import os
import openai
import logging
import schedule
import time
import telebot
from datetime import datetime, timedelta
import pytz
from dotenv import load_dotenv
load_dotenv()
from process_csv import Process


ALMATY_TZ = pytz.timezone("Asia/Almaty")
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
        messages=[{"role": "system", "content": "You are a marketing specialist assistant. Calculate the best and the worse employee, based on their generated revenue as well. Answer always only in Russian language based on provided information for each employee, provide all numbers. "},
                  {"role": "user", "content": prompt}]
    )
    return response.choices[0].message.content.strip()

def send_report_day():
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%d.%m.%Y")
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
    total_revenue, margin = Process.calculate_total_revenue(df_day)
    total_revenue_per_employee = Process.calculate_revenue_per_employee(df_day)
    deal_counts = Process.count_deal_stages(df_day)
    employee_activity = Process.calculate_employee_activity(df_day)

    logging.info("Generating report...")
    data_summary = f"""Total revenue: {total_revenue}, с маржинальностью в 20% равную {margin} and Revenue per employee: {total_revenue_per_employee}. Количество успешных и проваленных сделок: {deal_counts}, Сводная активность сотрудников:{employee_activity} """
    report = generate_report(data_summary)
    
    logging.info("Sending report to Telegram...")
    bot.send_message(chat_id, f"Ежедневный отчёт за {yesterday}:\n{report}")


def schedule_task():
    now = datetime.now(ALMATY_TZ)
    schedule_time = "06:00"

    # Convert the scheduled time to Almaty timezone
    target_time = ALMATY_TZ.localize(datetime.strptime(schedule_time, "%H:%M")).time()

    print(f"Scheduling task at {schedule_time} Almaty time (server time: {now.strftime('%Y-%m-%d %H:%M:%S')})")
    
    schedule.every().day.at(schedule_time).do(send_report_day)

# Send report immediately for testing
send_report_day()
schedule_task()

# Schedule the report every day at 00:00


logging.info("Bot started. Waiting for scheduled tasks...")

# Run the scheduler loop
while True:
    schedule.run_pending()
    time.sleep(30)
