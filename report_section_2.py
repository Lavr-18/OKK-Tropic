import requests
from datetime import datetime, timedelta, UTC, timezone
import json
import time
import uuid
import os
from dotenv import load_dotenv


# --- Вспомогательные функции для работы с датами и временем ---
def get_report_datetimes_msk(report_date_msk_date):
    """
    Возвращает объекты datetime для начала (08:00 МСК) и конца (19:00 МСК)
    указанного рабочего дня в МСК.
    UIS API get.calls_report использует строки с МСК временем.
    """
    MSK_TZ = timezone(timedelta(hours=3))
    start_of_workday_msk = datetime.combine(report_date_msk_date, datetime.min.time().replace(hour=8), tzinfo=MSK_TZ)
    end_of_workday_msk = datetime.combine(report_date_msk_date, datetime.min.time().replace(hour=19), tzinfo=MSK_TZ)
    return start_of_workday_msk, end_of_workday_msk


# --- Функция для получения данных звонков из UIS ---
def get_uis_call_history(uis_base_url, uis_api_token, date_from_str, date_to_str):
    """
    Выполняет запрос к UIS API get.calls_report.
    Использует JSON-RPC 2.0 протокол.
    """
    url = uis_base_url
    all_calls = []

    payload_base = {
        "id": "1",
        "jsonrpc": "2.0",
        "method": "get.calls_report",
        "params": {
            "access_token": uis_api_token,
            "date_from": date_from_str,
            "date_till": date_to_str
        }
    }

    try:
        response = requests.post(url, json=payload_base)
        response.raise_for_status()
        result = response.json()

        if "error" in result:
            print(
                f"Ошибка от UIS API ({payload_base['method']}): {result['error'].get('message', 'Неизвестная ошибка')}")
            return None

        api_data = result.get("result", {}).get("data", [])
        if not isinstance(api_data, list):
            raise ValueError("Неверный формат данных: 'result.data' должен быть списком.")

        all_calls.extend(api_data)

    except requests.exceptions.RequestException as e:
        print(f"Ошибка при получении данных звонков из UIS ({payload_base['method']}): {e}")
        if response is not None and response.text:
            print(f"Тело ответа API UIS: {response.text}")
        return None
    except json.JSONDecodeError:
        print(f"Ошибка: Некорректный JSON ответ от UIS API ({payload_base['method']}).")
        if response is not None and response.text:
            print(f"Тело ответа API UIS: {response.text}")
        return None
    except ValueError as e:
        print(f"Ошибка обработки данных от UIS API: {e}")
        return None

    return all_calls


# --- ОБНОВЛЕННАЯ Функция для проверки чатов в RetailCRM (пока игнорируем 404) ---
def check_retailcrm_chat_response(phone_number, missed_call_time_unix, retailcrm_base_url, retailcrm_api_key):
    """
    Проверяет, было ли взаимодействие в чате RetailCRM для данного номера
    после пропущенного звонка.
    Возвращает True, если найдено соответствующее взаимодействие, иначе False.
    Новая логика: сначала ищет заказ по номеру телефона (через filter[customer]), затем сообщения чата для клиента этого заказа.
    Если заказ не найден, пробует найти клиента по номеру телефона (через filter[name]) и ищет сообщения чата для этого клиента.
    """
    if not retailcrm_base_url or not retailcrm_api_key:
        print("Отладка RetailCRM: Отсутствуют RetailCRM API URL или ключ. Проверка чата пропущена.")
        return False

    customer_id = None
    # Добавим небольшой запас времени до пропущенного звонка для поиска
    # заказов и чатов, чтобы учесть активности, начавшиеся сразу
    check_from_time_dt = datetime.fromtimestamp(missed_call_time_unix - 300)  # 5 минут до
    check_from_time_str = check_from_time_dt.strftime("%Y-%m-%d %H:%M:%S")

    print(f"Отладка RetailCRM: Поиск ответа для номера {phone_number} после {check_from_time_str}")

    # --- 1. Поиск заказа по номеру телефона клиента (через filter[customer]) ---
    try:
        orders_url = f"{retailcrm_base_url}/orders"
        params_orders = {
            "apiKey": retailcrm_api_key,
            "filter[customer]": phone_number,  # <-- ИСПОЛЬЗУЕМ filter[customer] с номером телефона
            "filter[createdAtFrom]": check_from_time_str
        }
        print(f"Отладка RetailCRM: Попытка найти заказ по filter[customer]={phone_number}")
        response = requests.get(orders_url, params=params_orders)
        response.raise_for_status()
        data = response.json()

        if data.get('success') and data.get('orders'):
            customer_id = data['orders'][0]['customer']['id']
            print(f"Отладка RetailCRM: Найден заказ с клиентом {customer_id} для номера {phone_number}.")
        else:
            print(f"Отладка RetailCRM: Заказы для номера {phone_number} не найдены через filter[customer].")

    except requests.exceptions.RequestException as e:
        print(f"Ошибка при поиске заказов в RetailCRM по номеру {phone_number} (orders.list, filter[customer]): {e}")
        if response is not None and response.text:
            print(f"Тело ответа RetailCRM (orders.list): {response.text}")
    except json.JSONDecodeError:
        print(f"Ошибка: Некорректный JSON ответ от RetailCRM (orders.list, filter[customer]).")
        if response is not None and response.text:
            print(f"Тело ответа RetailCRM (orders.list): {response.text}")

    # --- 2. Если заказ не найден, пробуем найти клиента по имени (по номеру телефона через filter[name]) ---
    if not customer_id:
        try:
            customers_url = f"{retailcrm_base_url}/customers"
            params_customers = {
                "apiKey": retailcrm_api_key,
                "filter[name]": phone_number  # <-- ИСПОЛЬЗУЕМ filter[name] с номером телефона
            }
            print(f"Отладка RetailCRM: Попытка найти клиента по filter[name]={phone_number}")
            response = requests.get(customers_url, params=params_customers)
            response.raise_for_status()
            data = response.json()

            if data.get('success') and data.get('customers'):
                customer_id = data['customers'][0]['id']
                print(f"Отладка RetailCRM: Найден клиент {customer_id} для номера {phone_number} через filter[name].")
            else:
                print(f"Отладка RetailCRM: Клиент с номером {phone_number} не найден через filter[name].")

        except requests.exceptions.RequestException as e:
            print(f"Ошибка при поиске клиента в RetailCRM по номеру {phone_number} (customers.list, filter[name]): {e}")
            if response is not None and response.text:
                print(f"Тело ответа RetailCRM (customers.list): {response.text}")
        except json.JSONDecodeError:
            print(f"Ошибка: Некорректный JSON ответ от RetailCRM (customers.list, filter[name]).")
            if response is not None and response.text:
                print(f"Тело ответа RetailCRM (customers.list): {response.text}")

    if not customer_id:
        print(f"Отладка RetailCRM: Не удалось найти клиента/заказ для номера {phone_number} ни одним способом.")
        return False  # Если customer_id не был найден ни одним способом, выходим

    # --- 3. Поиск сообщений чата (customer-messages) для найденного клиента ---
    # !!! ВНИМАНИЕ: Этот раздел может вызывать 404 ошибку из-за отсутствия метода API.
    # !!! Пока мы его оставляем, но имейте в виду, что он может не работать.
    try:
        messages_url = f"{retailcrm_base_url}/customer-messages"
        params_messages = {
            "apiKey": retailcrm_api_key,
            "filter[customerIds][0]": customer_id,
            "filter[createdAtFrom]": check_from_time_str
        }

        print(f"Отладка RetailCRM: Поиск сообщений для клиента {customer_id} с {check_from_time_str}")
        response = requests.get(messages_url, params=params_messages)
        response.raise_for_status()
        data = response.json()

        if data.get('success') and data.get('customerMessages'):
            print(
                f"Отладка RetailCRM: Найдены сообщения чата для клиента {customer_id} ({phone_number}) после пропущенного звонка.")
            return True
        else:
            print(
                f"Отладка RetailCRM: Не найдено сообщений чата для клиента {customer_id} ({phone_number}) после пропущенного звонка.")
            return False

    except requests.exceptions.RequestException as e:
        print(f"Ошибка при поиске сообщений чата в RetailCRM для клиента {customer_id}: {e}")
        if response is not None and response.text:
            print(f"Тело ответа RetailCRM (customer-messages.list): {response.text}")
        return False
    except json.JSONDecodeError:
        print(f"Ошибка: Некорректный JSON ответ от RetailCRM (customer-messages.list).")
        if response is not None and response.text:
            print(f"Тело ответа RetailCRM (customer-messages.list): {response.text}")
        return False


def get_section_2_report_data(report_date_msk, uis_base_url, uis_api_token, retailcrm_base_url, retailcrm_api_key):
    """
    Формирует данные для второго пункта отчета (звонки UIS).
    Теперь включает проверку чатов RetailCRM через поиск заказов.
    """
    print("Получение данных для пункта 2: Пропущенных, Абонентов, Перезвонов и Не перезвонивших...")

    start_report_msk, end_report_msk = get_report_datetimes_msk(report_date_msk)

    date_from_str = start_report_msk.strftime("%Y-%m-%d %H:%M:%S")
    date_to_str = end_report_msk.strftime("%Y-%m-%d %H:%M:%S")

    all_calls = get_uis_call_history(uis_base_url, uis_api_token, date_from_str, date_to_str)

    if all_calls is None:
        return [
            "2. Пропущенных - Ошибка получения данных UIS",
            "Абонентов - Ошибка получения данных UIS",
            "Количество перезвонов более 5 минут - Ошибка получения данных UIS",
            "Не перезвонили/не написали - Ошибка получения данных UIS"
        ]

    print(f"Отладка: Получено {len(all_calls)} всего звонков из UIS.")

    in_calls = []
    out_calls = []

    missed_calls_count = 0
    unique_callers = set()
    missed_call_details = []

    for call in all_calls:
        direction = call.get('direction')
        is_lost = call.get('is_lost')
        phone_number = call.get('contact_phone_number')
        start_time_str = call.get('start_time')
        # Получаем длительность звонка, по умолчанию 0, если поля нет
        call_duration = call.get('call_session_duration', 0)

        if direction == 'in':
            in_calls.append(call)
            if phone_number:
                unique_callers.add(phone_number)

            # !!! ОБНОВЛЕННАЯ ЛОГИКА ДЛЯ "ПРОПУЩЕННЫХ ЗВОНКОВ"
            if is_lost is True and call_duration > 10:  # Пропущенный входящий звонок с длительностью > 10 секунд
                missed_calls_count += 1
                if phone_number and start_time_str:
                    start_time_dt = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                    missed_call_details.append({
                        "phone_number": phone_number,
                        "start_time_unix": int(start_time_dt.timestamp())
                    })
        elif direction == 'out':
            out_calls.append(call)

    print(f"Отладка: Из них {len(in_calls)} входящих и {len(out_calls)} исходящих.")

    # --- Подсчет перезвонов более 5 минут и не перезвонивших/не написавших ---
    late_callbacks_count = 0
    responded_to_missed_numbers = set()  # Номера пропущенных, на которые был любой ответ

    # Сначала собираем все исходящие звонки по номерам и времени
    outgoing_calls_by_number = {}
    for call in out_calls:
        phone_number = call.get('contact_phone_number')
        start_time_str = call.get('start_time')

        if phone_number and start_time_str:
            start_time_dt = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            start_time_unix = int(start_time_dt.timestamp())

            if phone_number not in outgoing_calls_by_number:
                outgoing_calls_by_number[phone_number] = []
            outgoing_calls_by_number[phone_number].append(start_time_unix)

    for missed_call in missed_call_details:
        missed_phone = missed_call['phone_number']
        missed_time_unix = missed_call['start_time_unix']

        found_any_response = False  # Флаг для любого ответа (звонок ИЛИ чат)

        # 1. Проверка обратного звонка
        found_callback_call = False
        first_callback_call_time_diff = float('inf')

        if missed_phone in outgoing_calls_by_number:
            for out_time_unix in outgoing_calls_by_number[missed_phone]:
                if out_time_unix > missed_time_unix:
                    found_callback_call = True
                    found_any_response = True
                    time_diff = out_time_unix - missed_time_unix
                    if time_diff < first_callback_call_time_diff:
                        first_callback_call_time_diff = time_diff

        # 2. Проверка ответа в чате RetailCRM (пока игнорируем ошибки)
        # Если chat_responded == True, это означает, что ответ был через чат.
        chat_responded = check_retailcrm_chat_response(
            missed_phone,
            missed_time_unix,
            retailcrm_base_url,
            retailcrm_api_key
        )
        if chat_responded:
            found_any_response = True

        if found_any_response:
            responded_to_missed_numbers.add(missed_phone)
            # Логика для late_callbacks_count относится только к звонкам.
            if found_callback_call and first_callback_call_time_diff > 300: # 300 секунд = 5 минут
                late_callbacks_count += 1

    # Учитываем, что responded_to_missed_numbers может содержать номера, отвеченные через чат.
    # Если проверка чата не работает, то это будет только прозвон.
    unresponded_calls_count = missed_calls_count - len(responded_to_missed_numbers)

    report_lines = [
        f"2. Пропущенных - {missed_calls_count}",
        f"Абонентов - {len(unique_callers)}",
        f"Количество перезвонов более 5 минут - {late_callbacks_count}",
        f"Не перезвонили/не написали - {unresponded_calls_count}"
    ]
    return report_lines


# Тестовый запуск модуля
if __name__ == "__main__":
    load_dotenv()

    UIS_BASE_URL_TEST = "https://dataapi.uiscom.ru/v2.0"
    UIS_API_TOKEN_TEST = os.getenv("UIS_API_TOKEN")
    REPORT_DATE_MSK_TEST = datetime(2025, 7, 10).date()

    RETAILCRM_BASE_URL_TEST = os.getenv("RETAILCRM_BASE_URL")
    RETAILCRM_API_KEY_TEST = os.getenv("RETAILCRM_API_TOKEN")

    if not UIS_API_TOKEN_TEST or not RETAILCRM_API_KEY_TEST or not RETAILCRM_BASE_URL_TEST:
        print(
            "Ошибка: Не все переменные окружения (UIS_API_TOKEN, RETAILCRM_API_TOKEN, RETAILCRM_BASE_URL) установлены для автономного запуска report_section_2.py.")
    else:
        print("Запуск report_section_2.py в тестовом режиме.")
        report_output = get_section_2_report_data(
            report_date_msk=REPORT_DATE_MSK_TEST,
            uis_base_url=UIS_BASE_URL_TEST,
            uis_api_token=UIS_API_TOKEN_TEST,
            retailcrm_base_url=RETAILCRM_BASE_URL_TEST,
            retailcrm_api_key=RETAILCRM_API_KEY_TEST
        )
        for line in report_output:
            print(line)