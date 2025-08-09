import os
import requests
from datetime import datetime, timedelta, timezone, time
from dotenv import load_dotenv

# --- Вспомогательные функции для работы с датами и временем ---

# Смещение часового пояса Москвы относительно UTC
MSK_OFFSET = timedelta(hours=3)


def to_utc(dt_msk):
    """Конвертирует объект datetime из MSK в UTC."""
    return dt_msk - MSK_OFFSET


def format_datetime_for_api(dt_object):
    """Форматирует объект datetime в строковый формат RetailCRM API (UTC)."""
    if dt_object.tzinfo is None:
        dt_object = dt_object.replace(tzinfo=timezone.utc)
    return dt_object.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso_datetime(iso_string):
    """Парсит строку ISO 8601 в объект datetime с учетом UTC."""
    # Обработка 'Z' и смещений
    if iso_string.endswith('Z'):
        dt = datetime.strptime(iso_string[:-1], "%Y-%m-%dT%H:%M:%S.%f")
    else:
        # Пробуем без .%f, если там нет миллисекунд или они не всегда
        try:
            dt = datetime.strptime(iso_string, "%Y-%m-%dT%H:%M:%S.%fZ")
        except ValueError:
            dt = datetime.strptime(iso_string, "%Y-%m-%dT%H:%M:%SZ")
    return dt.replace(tzinfo=timezone.utc)


# --- Функции для получения данных из RetailCRM Bot API ---

def get_dialog_messages(retailcrm_bot_base_url, bot_api_key, chat_id, limit=10):
    """
    Получает последние сообщения для конкретного чата.
    Используем эндпоинт /messages и фильтруем по chat_id.
    """
    url = f"{retailcrm_bot_base_url}/messages"
    headers = {
        "X-Bot-Token": bot_api_key,
        "Content-Type": "application/json"
    }
    params = {
        "chatId": chat_id,
        "limit": limit,
        "order": "desc"
    }

    try:
        messages_response = requests.get(url, params=params, headers=headers, timeout=5)
        messages_response.raise_for_status()
        messages_data = messages_response.json()
        if not isinstance(messages_data, list):
            print(f"Предупреждение: Ответ API для сообщений не является списком. Ответ: {messages_data}")
            return []
        return messages_data
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при получении сообщений для чата {chat_id}: {e}")
        return []
    except Exception as e:
        print(f"Непредвиденная ошибка при получении сообщений для чата {chat_id}: {e}")
        return []


def get_active_dialogs(retailcrm_bot_base_url, bot_api_key, max_dialogs=50):
    """
    Получает список активных диалогов, ограничиваясь max_dialogs.
    """
    print(f"Получение до {max_dialogs} последних активных диалогов...")

    url = f"{retailcrm_bot_base_url}/dialogs"
    headers = {
        "X-Bot-Token": bot_api_key,
        "Content-Type": "application/json"
    }
    params = {
        "active": "true",
        "limit": 100
    }

    all_active_dialogs = []
    page = 1
    while True:
        params["page"] = page
        try:
            print(f"Отладка: Отправка запроса к API для активных диалогов (стр. {page})...")
            response = requests.get(url, params=params, headers=headers, timeout=15)
            print("Отладка: Ответ от API получен.")
            response.raise_for_status()

            data = response.json()
            if not isinstance(data, list):
                print(f"Предупреждение: Ответ API для диалогов не является списком. Получен ответ: {data}")
                return None

            dialogs = data
            all_active_dialogs.extend(dialogs)

            if len(all_active_dialogs) >= max_dialogs or not dialogs:
                break

            page += 1

        except requests.exceptions.Timeout:
            print(f"Ошибка: Запрос активных диалогов превысил таймаут (15 секунд). URL: {url}")
            return None
        except requests.exceptions.ConnectionError as e:
            print(f"Ошибка соединения: Не удалось установить соединение с сервером активных диалогов. {e}")
            return None
        except requests.exceptions.RequestException as e:
            print(f"Ошибка при получении активных диалогов: {e}")
            print(f"Ответ API: {e.response.text if e.response else 'Нет ответа'}")
            return None
        except Exception as e:
            print(f"Непредвиденная ошибка при получении активных диалогов: {e}")
            return None

    return all_active_dialogs[:max_dialogs]


# --- Основная логика выполнения скрипта при прямом запуске ---
if __name__ == "__main__":
    load_dotenv()

    print("Запуск report_section_4.py в тестовом режиме.")

    REPORT_DATE_MSK_TEST = datetime.now().date()

    RETAILCRM_BOT_BASE_URL_TEST = "https://mg-s1.retailcrm.pro/api/bot/v1"

    BOT_API_KEY_TEST = os.getenv("RETAILCRM_BOT_API_TOKEN")

    if not BOT_API_KEY_TEST:
        print(
            "Ошибка: Переменная окружения RETAILCRM_BOT_API_TOKEN не установлена для автономного запуска report_section_4.py.")
        print("Пожалуйста, убедитесь, что ваш файл .env настроен правильно.")
    else:
        # Рассчитываем время 19:00 MSK сегодняшнего дня для фильтрации сообщений
        time_19_00_msk_today = datetime.combine(REPORT_DATE_MSK_TEST, time(19, 0))  # Возвращено на 19
        time_19_00_utc_today = to_utc(time_19_00_msk_today).replace(tzinfo=timezone.utc)
        print(f"Отладка: Будем искать сообщения, пришедшие после {time_19_00_utc_today} UTC (19:00 MSK сегодня).")

        # Шаг 1: Получаем до 50 последних активных диалогов
        all_active_dialogs = get_active_dialogs(RETAILCRM_BOT_BASE_URL_TEST, BOT_API_KEY_TEST, max_dialogs=50)

        if all_active_dialogs is None:
            print("Не удалось получить список активных диалогов для Пункта 4.")
        else:
            print(f"Отладка: Получено {len(all_active_dialogs)} активных диалогов для детальной проверки.")

            # Шаг 2: Проверяем каждое сообщение на соответствие условию
            dialogs_with_new_messages_after_19_00 = []  # Обновлено имя переменной

            for dialog in all_active_dialogs:
                chat_id = dialog.get('chatId')
                if chat_id is None:
                    chat_id = dialog.get('chat_id')
                    if chat_id is None:
                        print(f"Предупреждение: Пропущен диалог без 'chatId' или 'chat_id': {dialog}")
                        continue

                messages = get_dialog_messages(RETAILCRM_BOT_BASE_URL_TEST, BOT_API_KEY_TEST, chat_id, limit=5)

                found_recent_message = False
                for message in messages:
                    sender_type = message.get('sender', {}).get('type')
                    created_at_str = message.get('createdAt')

                    if sender_type == 'customer' and created_at_str:
                        try:
                            message_time_utc = parse_iso_datetime(created_at_str)
                            if message_time_utc >= time_19_00_utc_today:  # Сравнение с 19:00 UTC
                                dialogs_with_new_messages_after_19_00.append(dialog)  # Обновлено имя переменной
                                found_recent_message = True
                                break
                        except ValueError as e:
                            print(f"Ошибка парсинга даты '{created_at_str}' в чате {chat_id}: {e}")

            count_awaiting_response = len(dialogs_with_new_messages_after_19_00)  # Обновлено имя переменной
            print("---")
            print("4. Чаты проверены")
            print(
                f"Поступили сегодня после 19:00: {count_awaiting_response} чата ожидают ответа (из последних 50 активных)")  # Обновлено сообщение вывода