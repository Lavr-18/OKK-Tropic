import os
from datetime import datetime, timedelta, timezone, time
from dotenv import load_dotenv
import uuid
import asyncio
from aiogram import Bot
# УДАЛЕНА СТРОКА: from aiogram.utils.exceptions import MessageTextIsEmpty

# --- Импортируем все необходимые функции для отчёта ---
from report_section_1 import get_section_1_report_data
from report_section_2 import get_section_2_report_data
from report_section_3 import get_section_3_report_data
from report_section_4 import get_active_dialogs, get_dialog_messages, to_utc, parse_iso_datetime
from report_section_fio import get_fio_report_data

# Загружаем переменные окружения из .env файла
load_dotenv()

# --- Константы для всего отчета ---
RETAILCRM_BASE_URL = os.getenv("RETAILCRM_BASE_URL")
RETAILCRM_API_TOKEN = os.getenv("RETAILCRM_API_TOKEN")
RETAILCRM_SITE_CODE = os.getenv("RETAILCRM_SITE_CODE")

# Новые константы для UIS API
UIS_BASE_URL = "https://dataapi.uiscom.ru/v2.0"
UIS_API_TOKEN = os.getenv("UIS_API_TOKEN")

# Константы для RetailCRM Bot API (используются в пункте 4)
RETAILCRM_BOT_BASE_URL = "https://mg-s1.retailcrm.pro/api/bot/v1"
RETAILCRM_BOT_API_TOKEN = os.getenv("RETAILCRM_BOT_API_TOKEN")

# --- ОБНОВЛЕННЫЕ КОНСТАНТЫ ДЛЯ TELEGRAM ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
# Добавлена новая переменная для ID темы (подгруппы)
TELEGRAM_TOPIC_ID = os.getenv("TELEGRAM_TOPIC_ID")

# REPORT_UUID может быть использован для уникальной идентификации отчета, если нужно.
REPORT_UUID = str(uuid.uuid4())

# Если токены не найдены, выходим с ошибкой
if not RETAILCRM_API_TOKEN:
    print("Ошибка: Переменная окружения 'RETAILCRM_API_KEY' не установлена.")
    exit(1)
if not UIS_API_TOKEN:
    print("Ошибка: Переменная окружения 'UIS_API_TOKEN' не установлена.")
    exit(1)
if not RETAILCRM_BASE_URL:
    print("Ошибка: Переменная окружения 'RETAILCRM_BASE_URL' не установлена.")
    exit(1)
if not RETAILCRM_SITE_CODE:
    print("Ошибка: Переменная окружения 'RETAILCRM_SITE_CODE' не установлена.")
    exit(1)
if not RETAILCRM_BOT_API_TOKEN:
    print("Ошибка: Переменная окружения 'RETAILCRM_BOT_API_TOKEN' не установлена. Она нужна для пункта 4.")
    exit(1)
if not TELEGRAM_BOT_TOKEN:
    print("Ошибка: Переменная окружения 'TELEGRAM_BOT_TOKEN' не установлена.")
    exit(1)
if not TELEGRAM_CHAT_ID:
    print("Ошибка: Переменная окружения 'TELEGRAM_CHAT_ID' не установлена.")
    exit(1)
# Новая проверка на наличие ID темы
if not TELEGRAM_TOPIC_ID:
    print("Внимание: Переменная окружения 'TELEGRAM_TOPIC_ID' не установлена. Сообщения будут отправлены в общую тему.")


# --- Функции для отчета (оставляем как есть) ---
def get_section_4_report_data(report_date_msk, retailcrm_bot_base_url, bot_api_key):
    """
    Получает данные для пункта 4: Чаты проверены.
    Находит активные диалоги, где последнее сообщение от клиента пришло после 19:00 MSK указанной даты.
    """
    report_lines = []
    report_lines.append("4. Чаты проверены")

    time_19_00_msk = datetime.combine(report_date_msk, time(19, 0))
    time_19_00_utc = to_utc(time_19_00_msk).replace(tzinfo=timezone.utc)

    print(f"Отладка (main): Ищем сообщения, пришедшие после {time_19_00_utc} UTC ({report_date_msk} 19:00 MSK).")

    all_active_dialogs = get_active_dialogs(retailcrm_bot_base_url, bot_api_key, max_dialogs=50)

    if all_active_dialogs is None:
        report_lines.append("Не удалось получить данные о чатах.")
        return report_lines

    dialogs_with_new_messages_after_19_00 = []

    for dialog in all_active_dialogs:
        chat_id = dialog.get('chatId')
        if chat_id is None:
            chat_id = dialog.get('chat_id')
            if chat_id is None:
                continue

        messages = get_dialog_messages(retailcrm_bot_base_url, bot_api_key, chat_id, limit=5)

        found_recent_message = False
        for message in messages:
            sender_type = message.get('sender', {}).get('type')
            created_at_str = message.get('createdAt')

            if sender_type == 'customer' and created_at_str:
                try:
                    message_time_utc = parse_iso_datetime(created_at_str)
                    if message_time_utc >= time_19_00_utc:
                        dialogs_with_new_messages_after_19_00.append(dialog)
                        found_recent_message = True
                        break
                except ValueError as e:
                    print(f"Ошибка парсинга даты '{created_at_str}' в чате {chat_id}: {e}")

    count_awaiting_response = len(dialogs_with_new_messages_after_19_00)
    report_lines.append(f"Поступили после 19:00: {count_awaiting_response} чата ожидают ответа")

    return report_lines


# --- Функция для разбиения длинного текста на части ---
def split_message(text, limit=4096):
    """
    Рекурсивно разбивает длинный текст на части, не превышающие лимит Telegram (4096),
    стараясь разделять по переводу строки.
    """
    if len(text) <= limit:
        return [text]

    # Ищем последнюю новую строку перед лимитом
    split_pos = text.rfind('\n', 0, limit)
    if split_pos == -1 or split_pos < limit * 0.9:
        # Если не нашли или нашли слишком близко к началу, обрезаем по лимиту
        split_pos = limit

    part = text[:split_pos]
    remainder = text[split_pos:].lstrip('\n')  # Удаляем лишние переносы строки в начале остатка

    return [part] + split_message(remainder, limit)


# --- Асинхронная функция для отправки сообщения в Telegram ---
async def send_telegram_message_async(text, bot_token, chat_id, topic_id=None):
    """
    Отправляет текстовое сообщение в указанный чат Telegram асинхронно,
    автоматически разбивая его на части, если оно слишком длинное.
    """
    bot = Bot(token=bot_token)
    message_parts = split_message(text)

    try:
        for i, part in enumerate(message_parts):
            if not part.strip():  # Пропускаем пустые части
                continue

            # Добавляем нумерацию страниц в заголовок, если частей больше одной
            if len(message_parts) > 1:
                # Временно используем ** для выделения вместо HTML-тегов, чтобы избежать проблем с закрытием тегов при разбиении.
                header = ''
                part = header + part

            await bot.send_message(chat_id=chat_id, text=part, parse_mode='HTML', message_thread_id=topic_id)
            # Небольшая задержка, чтобы избежать лимитов Telegram при отправке нескольких частей
            await asyncio.sleep(0.5)

        print(f"Сообщение успешно отправлено в Telegram ({len(message_parts)} частей).")
    except Exception as e:
        # Общий перехват ошибок. Ошибка MessageTextIsEmpty теперь будет поймана здесь.
        print(f"Ошибка при отправке сообщения в Telegram: {e}")
    finally:
        await bot.session.close()  # Обязательно закрываем сессию


# --- Главная функция для сборки и отправки отчета ---
def main():
    REPORT_DATE_MSK = (datetime.now() - timedelta(days=1)).date()

    # --- Формируем первое сообщение ---
    message_1_lines = []

    # Заголовок отчета добавляем только один раз в начале
    message_1_lines.append(f"Отчет ОКК {REPORT_DATE_MSK.strftime('%d.%m.%Y')}")
    message_1_lines.append("---")

    # Пункт 1: Проверка невыполненных задач
    section_1_output = get_section_1_report_data(REPORT_DATE_MSK, RETAILCRM_BASE_URL, RETAILCRM_API_TOKEN)
    message_1_lines.extend(section_1_output)
    message_1_lines.append("")

    # Пункт 2: Звонки UIS
    section_2_output = get_section_2_report_data(
        report_date_msk=REPORT_DATE_MSK,
        uis_base_url=UIS_BASE_URL,
        uis_api_token=UIS_API_TOKEN,
        retailcrm_base_url=RETAILCRM_BASE_URL,
        retailcrm_api_key=RETAILCRM_API_TOKEN
    )
    message_1_lines.extend(section_2_output)
    message_1_lines.append("")

    # Пункт 3: Количество заказов, просроченных обработку
    section_3_output = get_section_3_report_data(REPORT_DATE_MSK)
    message_1_lines.extend(section_3_output)
    message_1_lines.append("")

    # # Пункт 4: Чаты проверены (передаем необходимые параметры для Bot API)
    # section_4_output = get_section_4_report_data(
    #     report_date_msk=REPORT_DATE_MSK,
    #     retailcrm_bot_base_url=RETAILCRM_BOT_BASE_URL,
    #     bot_api_key=RETAILCRM_BOT_API_TOKEN
    # )
    # message_1_lines.extend(section_4_output)

    message_1 = "\n".join(message_1_lines)

    # Отправляем первое сообщение асинхронно
    print("Отправка первого сообщения в Telegram...")
    # Обновлен вызов функции для передачи ID темы
    asyncio.run(send_telegram_message_async(
        message_1, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, topic_id=TELEGRAM_TOPIC_ID
    ))

    # --- Формируем второе сообщение (проверка ФИО) ---
    message_2_lines = [
        "Проверка оформления ФИО"
    ]

    section_fio_output = get_fio_report_data()
    message_2_lines.extend(section_fio_output)

    message_2 = "\n".join(message_2_lines)

    # Отправляем второе сообщение асинхронно
    print("Отправка второго сообщения в Telegram...")
    # Обновлен вызов функции для передачи ID темы
    asyncio.run(send_telegram_message_async(
        message_2, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, topic_id=TELEGRAM_TOPIC_ID
    ))


if __name__ == "__main__":
    main()
