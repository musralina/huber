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
from config import generate_report, convert_to_python_types, TEMP_FILE, FILE_URL, CSV_FILE, DEAL_STATUSES, CUMULATIVE_JSON
import json
import numpy as np
from langchain.chat_models import ChatOpenAI
from langchain.agents import create_json_agent
from langchain.agents.agent_toolkits import JsonToolkit
from langchain.tools.json.tool import JsonSpec
import threading


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
        df = df[df['responsible_user_id'] != "Муратова Рината"] 
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
    df_day = df_day[df_day['responsible_user_id'] != "Муратова Рината"]
    logging.info("Processing CSV file...")
    df_day = df_day[df_day['responsible_user_id'] != "Биржа заявок"]
    total_revenue, margin = Process.calculate_total_revenue(df_day)
    total_revenue_per_employee = Process.calculate_revenue_per_employee(df_day)
    deal_counts = Process.count_deal_stages(df_day)
    
    employee_activity = Process.calculate_employee_activity(df_day)
    
    successful_deals = df_day[df_day['status_id'].isin(DEAL_STATUSES['successful'])].groupby("responsible_user_id").agg({"price": "sum", "status_id": "count"}).rename(columns={"status_id": "successful_deals"})
    
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
        df = df[df['responsible_user_id'] != "Муратова Рината"]
        
        # Process each unique date string
        for date_str in df['date'].unique():  # date_str is already a string
            df_date = df[df['date'] == date_str]
            
            # Calculate metrics
            df_date_filtered = df_date[df_date['responsible_user_id'] != "Биржа заявок"]
            deal_counts = Process.count_deal_stages(df_date_filtered)
            total_revenue, margin = Process.calculate_total_revenue(df_date_filtered)
            total_revenue_per_employee = Process.calculate_revenue_per_employee(df_date_filtered)
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


@bot.message_handler(func=lambda message: True)
def handle_message(message):
    """Handles user questions, retrieves raw data, and processes with OpenAI."""
    try:
        user_question = message.text
        chat_id = message.chat.id
        
        bot.send_message(chat_id, "Обрабатываю ваш запрос...")
        agent = setup_json_agent()
        
        if not agent:
            bot.send_message(chat_id, "Данные временно недоступны. Попробуйте позже.")
            return

        # Step 1: Get raw data using modified question
        retrieval_prompt = f"""
        В JSON-файле содержатся данные за последние 3 месяца.  
            Дата указана в ключе `updated_at` в формате `yyyy-mm-dd`.  
            Для определения и расчёета периода используйте этот ключ.
            В deal_counts содержится информация по всем сделкам за день: Успешным сделкам, Проваленным сделкам и Сделкам в работе.

            - Извлеките **только сырые данные**, без выполнения каких-либо вычислений.  
            - Значения прибыли за указанный день находятся в ключе `total_revenue`.  
            - Если в запросе указан конкретный период, извлеки данные по соответствующим дням и месяцам, учитывая формат `yyyy-mm-dd`.
            Если запрашивается период за последние 10 дней, то нужно считать начиная со вчерашнего дня в формате yyyy-mm-dd, и не раньше вем (yyyy-mm-dd - 10 дней)
            Если есть данные только за меньшее количество дней чем запрашиваемый период, то вытащи за все дни информацию не меньше и не больше запрашиваемого периода.
            Если вчера было 2025-03-01, то нужно отнять от первого марта 2025-го года 10 дней и вытащить информацию только за период с 19-го февраля.

            Верните **только фактические значения** из данных в исходном виде.  
            **Вопрос:** {user_question}

        """
        
        try:
            raw_data = agent.run(retrieval_prompt)
        except Exception as e:
            logging.error(f"Data retrieval failed: {e}")
            bot.send_message(chat_id, "Ошибка при получении данных.")
            return

        # Step 2: Process data with OpenAI
        analysis_prompt = f"""
        Проанализируй данные и ответь на вопрос на русском языке. 
        Всегда прибыль считай в тенге.
        Обязательно выполни все необходимые расчеты. 
        Если в данных есть временные метки, учитывай их при анализе.
        
        Вопрос: {user_question}
        Данные: {raw_data}
        
        Формат ответа:
        - Краткий вывод в начале
        - Подробное объяснение с расчетами
        - Основные выводы в конце
        """
        
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "Ты финансовый аналитик. Отвечай на русском."},
                {"role": "user", "content": analysis_prompt}
            ],
            temperature=0.1
        )
        
        final_answer = response.choices[0].message.content.strip()
        bot.send_message(chat_id, final_answer)

    except Exception as e:
        logging.error(f"Error handling message: {e}")
        bot.send_message(chat_id, "Произошла ошибка. Пожалуйста, попробуйте еще раз.")

def setup_json_agent():
    """Improved JSON agent initialization with data validation"""
    try:
        if not os.path.exists(CUMULATIVE_JSON):
            raise FileNotFoundError("Cumulative JSON file missing")
            
        with open(CUMULATIVE_JSON, "r", encoding='utf-8') as f:
            data = json.load(f)
            
        # Transform data structure
        processed_data = {}
        for entry in data:
            for key, value in entry.items():
                if key not in processed_data:
                    processed_data[key] = []
                processed_data[key].append(value)
                
        # Enhanced data validation
        required_keys = ['total_revenue', 'updated_at', 'margin', 'successful_deals_price', 'total_revenue_per_employee', 'deal_counts', 'employee_activity']
        for key in required_keys:
            if key not in processed_data:
                raise ValueError(f"Missing required key in data: {key}")

        spec = JsonSpec(dict_=processed_data, max_value_length=10000)
        toolkit = JsonToolkit(spec=spec)
        
        return create_json_agent(
            llm=ChatOpenAI(
                temperature=0,
                model="gpt-4-1106-preview",  # Use latest model
                openai_api_key=OPENAI_API_KEY,
                max_tokens=1000
            ),
            toolkit=toolkit,
            max_iterations=500,
            handle_parsing_errors=True
        )
        
    except Exception as e:
        logging.error(f"Agent setup failed: {str(e)}")
        return None


def start_bot():
    """Starts the Telegram bot in polling mode."""
    bot.infinity_polling()

if __name__ == "__main__":
    #generate_historical_data()
    send_report_day()
    schedule.every().day.at("01:00").do(send_report_day)
    
    # Start bot in a separate thread
    bot_thread = threading.Thread(target=start_bot)
    bot_thread.daemon = True
    bot_thread.start()
    
    logging.info("Bot started. Listening for messages and running scheduled tasks...")
    while True:
        schedule.run_pending()
        time.sleep(60)
