import os
import requests
from dotenv import load_dotenv
import json
from datetime import datetime, timedelta, timezone
import re

# --- 1. Загрузка переменных окружения ---
# Убедитесь, что в вашем файле .env есть следующие переменные:
# RETAILCRM_BASE_URL (например, https://your_account.retailcrm.ru)
# RETAILCRM_API_TOKEN
# RETAILCRM_SITE_CODE (например, your-site-code)
# UIS_API_TOKEN
load_dotenv()

RETAILCRM_BASE_URL = os.getenv("RETAILCRM_BASE_URL")
RETAILCRM_API_TOKEN = os.getenv("RETAILCRM_API_TOKEN")
RETAILCRM_SITE_CODE = os.getenv("RETAILCRM_SITE_CODE")

UIS_BASE_URL = os.getenv(
    "UIS_BASE_URL") or "https://dataapi.uiscom.ru/v2.0"  # Устанавливаем значение по умолчанию, если переменная не найдена
UIS_API_TOKEN = os.getenv("UIS_API_TOKEN")  # Загружаем UIS_API_TOKEN

# --- Коррекция RETAILCRM_BASE_URL, если он содержит /api/v5 ---
# Если переменная RETAILCRM_BASE_URL загружена с суффиксом /api/v5,
# удаляем его, чтобы избежать дублирования в запросах.
if RETAILCRM_BASE_URL and RETAILCRM_BASE_URL.endswith('/api/v5'):
    RETAILCRM_BASE_URL = RETAILCRM_BASE_URL.rsplit('/api/v5', 1)[0]
    print(f"Отладка: RETAILCRM_BASE_URL скорректирован до: {RETAILCRM_BASE_URL}")

# --- 2. Константы и сопоставления ---

# Сопоставление "способов оформления" с символьными кодами orderMethod из API RetailCRM
ORDER_METHOD_MAPPINGS = {
    "в один клик": "one-click",
    "задать вопрос": "zadat-vopros",
    "заказать услугу": "zakazat-uslugu",
    "через корзину": "shopping-cart",
    "хотите увидеть фото": "khotite-uvidet-foto",
}


# --- 3. Вспомогательные функции для работы с датами и временем ---

def get_report_datetimes_msk(report_date_msk_date):
    """
    Возвращает начало и конец рабочего дня (9:00:00 - 19:59:59) для заданной даты в МСК.
    Все даты и время будут в Московском часовом поясе.
    """
    MSK_TZ = timezone(timedelta(hours=3))
    start_of_workday_msk = datetime.combine(report_date_msk_date, datetime.min.time().replace(hour=9), tzinfo=MSK_TZ)
    # Конец рабочего дня (до 20:00, то есть до 19:59:59)
    end_of_workday_msk = datetime.combine(report_date_msk_date, datetime.min.time().replace(hour=20),
                                          tzinfo=MSK_TZ) - timedelta(seconds=1)
    return start_of_workday_msk, end_of_workday_msk


def normalize_phone_number(phone_number):
    """
    Нормализует телефонный номер, оставляя только цифры.
    Удаляет все нецифровые символы.
    """
    if not phone_number:
        return None
    # Удаляем все, кроме цифр
    normalized_number = re.sub(r'\D', '', phone_number)
    # Если номер начинается с 8, заменяем на 7 (для российских номеров)
    if normalized_number.startswith('8') and len(normalized_number) == 11:
        normalized_number = '7' + normalized_number[1:]
    # Если номер начинается с 9 и имеет 10 цифр, добавляем 7
    elif normalized_number.startswith('9') and len(normalized_number) == 10:
        normalized_number = '7' + normalized_number
    return normalized_number


# --- 4. Функции для работы с RetailCRM API ---

def get_orders_list(
        base_url,
        api_key,
        site_code,
        start_date=None,
        end_date=None,
        order_methods=None,
        page=1,
        limit=100  # Default limit for pagination
):
    """
    Получает список заказов из RetailCRM API с возможностью фильтрации по дате создания и способу оформления.
    Обрабатывает пагинацию для получения всех заказов.
    """
    url = f"{base_url}/api/v5/orders"
    all_orders = []

    while True:
        params = {
            "apiKey": api_key,
            "site": site_code,
            "page": page,
            "limit": limit  # Use the provided limit, which should be valid (20, 50, or 100)
        }

        if start_date:
            params["filter[createdAtFrom]"] = start_date.isoformat(sep=' ', timespec='seconds')
        if end_date:
            params["filter[createdAtTo]"] = end_date.isoformat(sep=' ', timespec='seconds')

        if order_methods:
            for i, method in enumerate(order_methods):
                params[f"filter[orderMethod][{i}]"] = method

        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            if not data.get('success'):
                print(f"Ошибка от RetailCRM API (orders.list): {data.get('errorMsg', 'Неизвестная ошибка')}")
                # Если есть конкретные ошибки, выведем их
                if data.get('errors'):
                    print(f"Подробности ошибок: {data['errors']}")
                return None

            orders_batch = data.get('orders', [])
            if not orders_batch:
                break  # Нет больше заказов

            all_orders.extend(orders_batch)

            pagination = data.get('pagination', {})
            current_page = pagination.get('currentPage')
            total_page_count = pagination.get('totalPageCount')

            if current_page >= total_page_count:
                break  # Достигнута последняя страница
            page += 1

        except requests.exceptions.RequestException as e:
            print(f"Ошибка при получении заказов из RetailCRM (orders.list): {e}")
            if response is not None and response.text:
                print(f"Тело ответа RetailCRM (orders.list): {response.text}")
            return None
        except json.JSONDecodeError:
            print(f"Ошибка: Некорректный JSON ответ от RetailCRM (orders.list).")
            if response is not None and response.text:
                print(f"Тело ответа RetailCRM (orders.list): {response.text}")
            return None
        except Exception as e:
            print(f"Неизвестная ошибка при получении заказов из RetailCRM: {e}")
            return None
    return all_orders


# --- 5. Функция для работы с UIS API ---

def get_uis_call_history(uis_base_url, uis_api_token, date_from_str, date_to_str):
    """
    Получает историю звонков из UIS API за указанный период.
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


# --- 6. Основная логика скрипта для пункта 3 ---

def get_section_3_report_data(report_date_msk_date):
    """
    Рассчитывает количество просроченных заказов согласно новым критериям
    и возвращает список строк отчета, включая детальные логи.
    """
    print("Получение данных для пункта 3: Количество заказов, просроченных обработку...")

    # Проверка наличия всех необходимых переменных окружения
    if not all([RETAILCRM_BASE_URL, RETAILCRM_API_TOKEN, RETAILCRM_SITE_CODE, UIS_API_TOKEN]):
        return [
            "3. Количество заказов, просроченных обработку - Ошибка: Не все необходимые переменные окружения для API установлены (RetailCRM URL, токены, код сайта, UIS токен)."]

    MSK_TZ = timezone(timedelta(hours=3))
    start_report_msk, end_report_msk = get_report_datetimes_msk(report_date_msk_date)

    retailcrm_date_from_str = start_report_msk.isoformat(sep=' ', timespec='seconds')
    retailcrm_date_to_str = end_report_msk.isoformat(sep=' ', timespec='seconds')

    uis_date_from_str = start_report_msk.strftime("%Y-%m-%d %H:%M:%S")
    uis_date_to_str = (end_report_msk + timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")

    target_order_methods_codes = list(ORDER_METHOD_MAPPINGS.values())

    retailcrm_orders = get_orders_list(
        RETAILCRM_BASE_URL,
        RETAILCRM_API_TOKEN,
        RETAILCRM_SITE_CODE,
        start_date=start_report_msk,
        end_date=end_report_msk + timedelta(seconds=1),
        order_methods=target_order_methods_codes
    )
    if retailcrm_orders is None:
        return ["3. Количество заказов, просроченных обработку - Ошибка получения данных RetailCRM."]

    print(f"Отладка: Получено {len(retailcrm_orders)} заказов из RetailCRM с целевыми способами оформления.")

    all_uis_calls = get_uis_call_history(UIS_BASE_URL, UIS_API_TOKEN, uis_date_from_str, uis_date_to_str)
    if all_uis_calls is None:
        return ["3. Количество заказов, просроченных обработку - Ошибка получения данных UIS."]

    outgoing_calls_details = []
    for call in all_uis_calls:
        if call.get('direction') == 'out':
            phone_number = call.get('contact_phone_number')
            start_time_str = call.get('start_time')
            if phone_number and start_time_str:
                normalized_phone = normalize_phone_number(phone_number)
                if normalized_phone:
                    try:
                        call_dt = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                        outgoing_calls_details.append({
                            'phone_number': normalized_phone,
                            'timestamp': int(call_dt.timestamp())
                        })
                    except ValueError:
                        print(
                            f"Предупреждение: Неверный формат даты/времени UIS API: {start_time_str}. Звонок пропущен из анализа.")
                        continue

    print(f"Отладка: Получено {len(outgoing_calls_details)} исходящих звонков из UIS.")

    delayed_orders_count = 0
    total_relevant_orders = 0
    orders_outside_working_hours_count = 0

    report_output_lines = []  # Список для сбора всех строк отчета этого раздела
    individual_order_details_lines = []  # Список для детальных логов по каждому заказу

    # 3. Проверка просроченности для каждого заказа
    for order in retailcrm_orders:
        order_id = order.get('id')
        created_at_str = order.get('createdAt')

        # Получаем внешний номер заказа для отчета
        external_order_number = order.get('number')
        if not external_order_number:
            # Если по какой-то причине номера нет, используем внутренний ID как запасной вариант
            external_order_number = order_id

        customer_phone_number = None
        if order.get('phone'):
            customer_phone_number = order.get('phone')
        elif order.get('customer', {}).get('phones'):
            if isinstance(order['customer']['phones'], list) and len(order['customer']['phones']) > 0:
                customer_phone_number = order['customer']['phones'][0].get('number')

        normalized_customer_phone = normalize_phone_number(customer_phone_number)

        if not (created_at_str and normalized_customer_phone):
            print(
                f"Отладка: Пропуск заказа {order_id} из-за отсутствия даты создания ({created_at_str}) или нормализованного номера клиента ({normalized_customer_phone}).")
            continue

        try:
            if '+' in created_at_str or (
                    len(created_at_str) > 19 and (created_at_str[19] == '+' or created_at_str[19] == '-')):
                order_created_at_dt = datetime.strptime(created_at_str, "%Y-%m-%d %H:%M:%S%z")
            else:
                order_created_at_dt = datetime.strptime(created_at_str, "%Y-%m-%d %H:%M:%S").replace(
                    tzinfo=timezone.utc)
        except ValueError:
            print(
                f"Предупреждение: Неверный формат даты создания заказа {order_id}: {created_at_str}. Заказ пропущен из анализа.")
            continue

        order_created_at_msk = order_created_at_dt.astimezone(MSK_TZ)

        if not (start_report_msk <= order_created_at_msk <= end_report_msk):
            orders_outside_working_hours_count += 1
            print(
                f"Отладка: Заказ {order_id} создан вне рабочего времени ({order_created_at_msk.strftime('%Y-%m-%d %H:%M:%S')}). Пропуск из основного анализа.")
            continue

        total_relevant_orders += 1

        order_created_at_timestamp = int(order_created_at_msk.timestamp())
        contact_deadline_timestamp = order_created_at_timestamp + 600

        contact_made_in_time = False
        earliest_contact_time = None

        for call in outgoing_calls_details:
            call_phone = call['phone_number']
            call_timestamp = call['timestamp']

            if call_phone == normalized_customer_phone and call_timestamp >= order_created_at_timestamp:
                if earliest_contact_time is None or call_timestamp < earliest_contact_time:
                    earliest_contact_time = call_timestamp

                if call_timestamp <= contact_deadline_timestamp:
                    contact_made_in_time = True
                    break

        # Добавляем ссылку на заказ
        order_link = f"{RETAILCRM_BASE_URL}/orders/{order_id}/edit"

        if not contact_made_in_time:
            individual_order_details_lines.append(
                f"Заказ {external_order_number} ({order_link}): Создан в {order_created_at_msk.strftime('%Y-%m-%d %H:%M:%S')}. "
                f"Первый контакт: {datetime.fromtimestamp(earliest_contact_time, tz=MSK_TZ).strftime('%Y-%m-%d %H:%M:%S') if earliest_contact_time else 'Нет контакта'}. "
                f"Статус: ПРОСРОЧЕН.")
            delayed_orders_count += 1

        else:
            individual_order_details_lines.append(
                f"Заказ {external_order_number} ({order_link}): Создан в {order_created_at_msk.strftime('%Y-%m-%d %H:%M:%S')}. "
                f"Первый контакт: {datetime.fromtimestamp(earliest_contact_time, tz=MSK_TZ).strftime('%Y-%m-%d %H:%M:%S')}. "
                f"Статус: ОБРАБОТАН ВОВРЕМЯ.")

    # Сначала добавляем сводные строки
    report_output_lines.append(
        f"3. Количество заказов, просроченных обработку - {delayed_orders_count} / {total_relevant_orders}")
    report_output_lines.append(
        f"Количество заказов, созданных в нерабочее время (не с 09:00 до 20:00) - {orders_outside_working_hours_count}")

    # Затем добавляем детальные логи по каждому заказу
    report_output_lines.extend(individual_order_details_lines)

    return report_output_lines


# --- 7. Тестовый скрипт для вывода данных заказа ---

def test_dump_order_data(report_date_msk_date, num_orders_to_dump=5):
    """
    Получает несколько последних заказов за указанную дату и выводит их полные данные
    для отладки.
    """
    print(f"\nЗапуск тестового скрипта для вывода данных заказов RetailCRM за {report_date_msk_date}...")

    if not all([RETAILCRM_BASE_URL, RETAILCRM_API_TOKEN, RETAILCRM_SITE_CODE]):
        print("Ошибка: Не все необходимые переменные окружения для RetailCRM установлены.")
        return

    MSK_TZ = timezone(timedelta(hours=3))
    start_of_day_msk = datetime.combine(report_date_msk_date, datetime.min.time(), tzinfo=MSK_TZ)
    end_of_day_msk = datetime.combine(report_date_msk_date, datetime.max.time(), tzinfo=MSK_TZ)

    valid_limit = 20
    orders = get_orders_list(
        RETAILCRM_BASE_URL,
        RETAILCRM_API_TOKEN,
        RETAILCRM_SITE_CODE,
        start_date=start_of_day_msk,
        end_date=end_of_day_msk,
        limit=valid_limit
    )

    if orders is None:
        print("Не удалось получить заказы для тестового вывода.")
        return

    if not orders:
        print(f"Не найдено заказов за {report_date_msk_date}.")
        return

    orders_to_display = orders[:num_orders_to_dump]

    print(f"Получено {len(orders)} заказов из API. Выводим первые {len(orders_to_display)}.")
    for i, order in enumerate(orders_to_display):
        print(f"\n--- Данные заказа {i + 1} (ID: {order.get('id')}) ---")
        print(f"  createdAt: {order.get('createdAt')}")

        customer_phone_number_for_print = None
        if order.get('phone'):
            customer_phone_number_for_print = order.get('phone')
        elif order.get('customer', {}).get('phones'):
            if isinstance(order['customer']['phones'], list) and len(order['customer']['phones']) > 0:
                customer_phone_number_for_print = order['customer']['phones'][0].get('number')
        print(f"  customer.phone (extracted): {customer_phone_number_for_print}")
        print(
            f"  customer.phone (normalized for comparison): {normalize_phone_number(customer_phone_number_for_print)}")

        print(f"  orderMethod: {order.get('orderMethod')}")
        print("  Полные данные заказа:")
        print(json.dumps(order, indent=2, ensure_ascii=False))


def test_dump_uis_call_data(report_date_msk_date, num_calls_to_dump=10):
    """
    Получает несколько последних звонков из UIS API за указанную дату и выводит их полные данные
    для отладки, включая нормализованные номера.
    """
    print(f"\nЗапуск тестового скрипта для вывода данных звонков UIS за {report_date_msk_date}...")

    if not UIS_API_TOKEN:
        print("Ошибка: Не установлен UIS_API_TOKEN.")
        return

    MSK_TZ = timezone(timedelta(hours=3))
    start_of_day_msk = datetime.combine(report_date_msk_date, datetime.min.time(), tzinfo=MSK_TZ)
    end_of_day_msk = datetime.combine(report_date_msk_date, datetime.max.time(), tzinfo=MSK_TZ)

    uis_date_from_str = start_of_day_msk.strftime("%Y-%m-%d %H:%M:%S")
    uis_date_to_str = (end_of_day_msk + timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")

    uis_calls = get_uis_call_history(UIS_BASE_URL, UIS_API_TOKEN, uis_date_from_str, uis_date_to_str)

    if uis_calls is None:
        print("Не удалось получить звонки для тестового вывода.")
        return

    if not uis_calls:
        print(f"Не найдено звонков за {report_date_msk_date}.")
        return

    calls_to_display = uis_calls[:num_calls_to_dump]

    print(f"Получено {len(uis_calls)} звонков из API. Выводим первые {len(calls_to_display)}.")
    for i, call in enumerate(calls_to_display):
        print(f"\n--- Данные звонка {i + 1} ---")
        print(f"  start_time: {call.get('start_time')}")
        print(f"  direction: {call.get('direction')}")
        print(f"  contact_phone_number: {call.get('contact_phone_number')}")
        print(f"  contact_phone_number (normalized): {normalize_phone_number(call.get('contact_phone_number'))}")
        print("  Полные данные звонка:")
        print(json.dumps(call, indent=2, ensure_ascii=False))


# --- 8. Точка входа для запуска скриптов ---

if __name__ == "__main__":
    # !!! Установите желаемую дату отчета в формате datetime.date(ГОД, МЕСЯЦ, ДЕНЬ) !!!
    REPORT_DATE_MSK_TEST = datetime(2025, 8, 7).date()

    # --- Выберите режим работы: ---
    # Закомментируйте/раскомментируйте нужную строку для выполнения:

    # 1. Запустить основной отчет (Пункт 3):
    report_output = get_section_3_report_data(report_date_msk_date=REPORT_DATE_MSK_TEST)
    for line in report_output:
        print(line)

    # 2. Запустить тестовый скрипт для вывода данных заказа RetailCRM:
    test_dump_order_data(report_date_msk_date=REPORT_DATE_MSK_TEST, num_orders_to_dump=1)

    # 3. Запустить тестовый скрипт для вывода данных звонков UIS:
    test_dump_uis_call_data(report_date_msk_date=REPORT_DATE_MSK_TEST, num_calls_to_dump=10)