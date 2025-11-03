import os
import requests
from dotenv import load_dotenv
import json
from datetime import datetime, timedelta, timezone
import re

# --- 1. Загрузка переменных окружения ---
load_dotenv()

RETAILCRM_BASE_URL = os.getenv("RETAILCRM_BASE_URL")
RETAILCRM_API_TOKEN = os.getenv("RETAILCRM_API_TOKEN")
RETAILCRM_SITE_CODE = os.getenv("RETAILCRM_SITE_CODE")

UIS_BASE_URL = os.getenv(
    "UIS_BASE_URL") or "https://dataapi.uiscom.ru/v2.0"
UIS_API_TOKEN = os.getenv("UIS_API_TOKEN")

if RETAILCRM_BASE_URL and RETAILCRM_BASE_URL.endswith('/api/v5'):
    RETAILCRM_BASE_URL = RETAILCRM_BASE_URL.rsplit('/api/v5', 1)[0]
    print(f"Отладка: RETAILCRM_BASE_URL скорректирован до: {RETAILCRM_BASE_URL}")

# --- 2. Константы и сопоставления ---

MSK_TZ = timezone(timedelta(hours=3))

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
    Возвращает начало и конец дня для заданной даты в МСК.
    """
    start_of_day_msk = datetime.combine(report_date_msk_date, datetime.min.time(), tzinfo=MSK_TZ)
    end_of_day_msk = datetime.combine(report_date_msk_date, datetime.max.time(), tzinfo=MSK_TZ)
    return start_of_day_msk, end_of_day_msk


def get_working_datetimes_msk(report_date_msk_date):
    """
    Возвращает начало и конец рабочего дня (9:00:00 - 20:59:59) для заданной даты в МСК.
    """
    start_of_workday = datetime.combine(report_date_msk_date, datetime.min.time().replace(hour=9), tzinfo=MSK_TZ)
    # Обновлено: Рабочее время до 21:00, т.е. конец в 20:59:59
    end_of_workday = datetime.combine(report_date_msk_date, datetime.min.time().replace(hour=20, minute=59, second=59),
                                      tzinfo=MSK_TZ)
    return start_of_workday, end_of_workday


def get_next_working_day_start_msk(current_date):
    """
    Определяет дату и время начала следующего рабочего дня (09:00:00 МСК).
    """
    next_day = current_date + timedelta(days=1)
    # Если следующий день выходной (суббота - 5, воскресенье - 6), переходим на понедельник
    if next_day.weekday() >= 5:
        next_day += timedelta(days=7 - next_day.weekday())
    return datetime.combine(next_day, datetime.min.time().replace(hour=9), tzinfo=MSK_TZ)


def normalize_phone_number(phone_number):
    """
    Нормализует телефонный номер, оставляя только цифры.
    """
    if not phone_number:
        return None
    normalized_number = re.sub(r'\D', '', phone_number)
    if normalized_number.startswith('8') and len(normalized_number) == 11:
        normalized_number = '7' + normalized_number[1:]
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
        limit=100
):
    """
    Получает список заказов из RetailCRM API с возможностью фильтрации по дате создания и способу оформления.
    Обрабатывает пагинацию для получения всех заказов.
    """
    url = f"{base_url}/api/v5/orders"
    all_orders = []
    response = None

    while True:
        params = {
            "apiKey": api_key,
            "site": site_code,
            "page": page,
            "limit": limit
        }

        if start_date:
            params["filter[createdAtFrom]"] = start_date.isoformat(sep=' ', timespec='seconds')
        if end_date:
            params["filter[createdAtTo]"] = end_date.isoformat(sep=' ', timespec='seconds')

        if order_methods:
            params["filter[orderMethod][]"] = order_methods

        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            if not data.get('success'):
                print(f"Ошибка от RetailCRM API (orders.list): {data.get('errorMsg', 'Неизвестная ошибка')}")
                if data.get('errors'):
                    print(f"Подробности ошибок: {data['errors']}")
                return None

            orders_batch = data.get('orders', [])
            if not orders_batch:
                break

            all_orders.extend(orders_batch)

            pagination = data.get('pagination', {})
            current_page = pagination.get('currentPage')
            total_page_count = pagination.get('totalPageCount')

            if current_page >= total_page_count:
                break
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
    response = None

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

    if not all([RETAILCRM_BASE_URL, RETAILCRM_API_TOKEN, RETAILCRM_SITE_CODE, UIS_API_TOKEN]):
        return [
            "3. Количество заказов, просроченных обработку - Ошибка: Не все необходимые переменные окружения для API установлены."]

    start_report_msk, end_report_msk = get_report_datetimes_msk(report_date_msk_date)
    start_work_day_msk, end_work_day_msk = get_working_datetimes_msk(report_date_msk_date)

    # Переменная для нерабочего времени (после 21:00)
    non_work_time_start = datetime.combine(report_date_msk_date, datetime.min.time().replace(hour=21), tzinfo=MSK_TZ)

    # Расширяем временной диапазон для получения заказов:
    # Получаем заказы, начиная с 21:00 предыдущего дня.
    previous_day_end = start_report_msk - timedelta(seconds=1)
    start_time_for_api = previous_day_end.replace(hour=21, minute=0, second=0)

    all_orders = []
    for method_name, method_code in ORDER_METHOD_MAPPINGS.items():
        print(f"Отладка: Получаем заказы для способа оформления: '{method_name}' ({method_code})")

        # Получаем заказы с 21:00 предыдущего дня до конца отчетного дня
        orders_for_method = get_orders_list(
            base_url=RETAILCRM_BASE_URL,
            api_key=RETAILCRM_API_TOKEN,
            site_code=RETAILCRM_SITE_CODE,
            start_date=start_time_for_api,
            end_date=end_report_msk + timedelta(seconds=1),
            order_methods=[method_code]
        )
        if orders_for_method:
            all_orders.extend(orders_for_method)

    orders_by_id = {order['id']: order for order in all_orders}
    all_orders_unique = list(orders_by_id.values())

    print(
        f"Отладка: Получено {len(all_orders_unique)} уникальных заказов из RetailCRM с целевыми способами оформления.")

    uis_date_from_str = start_time_for_api.strftime("%Y-%m-%d %H:%M:%S")
    uis_date_to_str = (end_report_msk + timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")

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
                        call_dt_msk = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=MSK_TZ)
                        outgoing_calls_details.append({
                            'phone_number': normalized_phone,
                            'datetime_msk': call_dt_msk
                        })
                    except ValueError:
                        print(
                            f"Предупреждение: Неверный формат даты/времени UIS API: {start_time_str}. Звонок пропущен из анализа.")
                        continue

    print(f"Отладка: Получено {len(outgoing_calls_details)} исходящих звонков из UIS.")

    overdue_count = 0
    total_relevant_orders = 0
    orders_in_work_time_count = 0
    orders_outside_work_time_count = 0

    report_output_lines = []
    overdue_details = []  # Новый список для хранения только просроченных деталей

    for order in all_orders_unique:
        order_number = order.get('number')
        order_id = order.get('id')
        created_at_str = order.get('createdAt')

        customer_phone_number = None
        if order.get('phone'):
            customer_phone_number = order.get('phone')
        elif order.get('customer', {}).get('phones'):
            if isinstance(order['customer']['phones'], list) and len(order['customer']['phones']) > 0:
                customer_phone_number = order['customer']['phones'][0].get('number')

        normalized_customer_phone = normalize_phone_number(customer_phone_number)

        # Пропускаем заказы без номера или без номера телефона клиента
        if not (order_number and created_at_str and normalized_customer_phone):
            continue

        try:
            order_created_at_dt = datetime.strptime(created_at_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=MSK_TZ)
        except ValueError:
            print(
                f"Предупреждение: Неверный формат даты создания заказа {order_number}: {created_at_str}. Заказ пропущен из анализа.")
            continue

        # Проверка, что заказ попадает в отчетный период (весь день или после 21:00 предыдущего дня)
        previous_day_non_work_time = datetime.combine(report_date_msk_date - timedelta(days=1),
                                                      datetime.min.time().replace(hour=21), tzinfo=MSK_TZ)
        is_relevant_order = (start_report_msk <= order_created_at_dt <= end_report_msk) or \
                            (
                                    order_created_at_dt >= previous_day_non_work_time and order_created_at_dt < start_work_day_msk)

        if not is_relevant_order:
            continue  # Заказ вне анализируемого окна

        total_relevant_orders += 1

        # Расчет дедлайна и подсчет рабочего/нерабочего времени
        contact_deadline_dt = None

        if start_work_day_msk <= order_created_at_dt <= end_work_day_msk:
            # Заказ создан в рабочее время (9:00 - 20:59:59) -> дедлайн +10 минут
            contact_deadline_dt = order_created_at_dt + timedelta(minutes=10)
            orders_in_work_time_count += 1
        elif order_created_at_dt >= non_work_time_start:  # Создан после 21:00 в день отчета
            # Заказ создан после 21:00 (т.е. после non_work_time_start в день report_date_msk_date)
            contact_deadline_dt = get_next_working_day_start_msk(order_created_at_dt.date()) + timedelta(hours=2)
            orders_outside_work_time_count += 1
        elif order_created_at_dt >= previous_day_non_work_time and order_created_at_dt < start_work_day_msk:
            # Заказ создан после 21:00 предыдущего дня, но до 09:00 текущего дня
            contact_deadline_dt = start_work_day_msk + timedelta(hours=2)
            orders_outside_work_time_count += 1
        else:
            # Не должен срабатывать, если is_relevant_order верен
            continue

        if not contact_deadline_dt:
            continue

        first_contact_time = None
        for call in outgoing_calls_details:
            # Ищем первый звонок после создания заказа
            if call['phone_number'] == normalized_customer_phone and call['datetime_msk'] >= order_created_at_dt:
                if first_contact_time is None or call['datetime_msk'] < first_contact_time:
                    first_contact_time = call['datetime_msk']

        is_overdue = first_contact_time is None or first_contact_time > contact_deadline_dt

        # --- ИЗМЕНЕНИЕ 1 и 2: Форматирование и фильтрация ---
        if is_overdue:
            overdue_count += 1

            contact_info = first_contact_time.strftime('%Y-%m-%d %H:%M:%S') if first_contact_time else 'Нет контакта'
            deadline_info = contact_deadline_dt.strftime('%Y-%m-%d %H:%M:%S')
            order_link = f"{RETAILCRM_BASE_URL}/orders/{order_id}/edit"

            # ИЗМЕНЕНИЕ: Форматируем номер заказа как HTML-ссылку
            order_link_formatted = f'<a href="{order_link}">Заказ {order_number}</a>'

            # ИЗМЕНЕНИЕ: Используем HTML-ссылку и убираем прямую ссылку в конце
            overdue_details.append(
                f"{order_link_formatted} (ID {order_id}): Создан в {created_at_str}. Дедлайн: {deadline_info}. Первый контакт: {contact_info}. Статус: ПРОСРОЧЕН."
            )

    # --- Формирование отчета ---

    # Расчет количества выполненных заказов для итоговой строки (общее - просроченные)
    # completed_count = total_relevant_orders - overdue_count

    # ИЗМЕНЕНИЕ: Суммарная строка в формате "просрочено/всего"
    report_output_lines.append(
        f"3. Количество заказов, просроченных обработку - {overdue_count}/{total_relevant_orders}")

    # Обновленное описание часов
    report_output_lines.append(
        f"Количество заказов, созданных в нерабочее время (до 09:00 и после 21:00) - {orders_outside_work_time_count}")
    report_output_lines.append(
        f"Количество заказов, созданных в рабочее время (с 09:00 до 21:00) - {orders_in_work_time_count}")

    # Добавление только просроченных деталей
    report_output_lines.extend(overdue_details)

    return report_output_lines


# --- 7. Тестовый скрипт для вывода данных заказа ---

def test_dump_order_data(report_date_msk_date, num_orders_to_dump=1):
    print(f"\nЗапуск тестового скрипта для вывода данных заказа RetailCRM за {report_date_msk_date}...")

    if not all([RETAILCRM_BASE_URL, RETAILCRM_API_TOKEN, RETAILCRM_SITE_CODE]):
        print("Ошибка: Не все необходимые переменные окружения для RetailCRM установлены.")
        return

    start_of_day_msk = datetime.combine(report_date_msk_date, datetime.min.time(), tzinfo=MSK_TZ)
    end_of_day_msk = datetime.combine(report_date_msk_date, datetime.max.time(), tzinfo=MSK_TZ)

    orders = get_orders_list(
        base_url=RETAILCRM_BASE_URL,
        api_key=RETAILCRM_API_TOKEN,
        site_code=RETAILCRM_SITE_CODE,
        start_date=start_of_day_msk,
        end_date=end_of_day_msk + timedelta(seconds=1)
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
    print(f"\nЗапуск тестового скрипта для вывода данных звонков UIS за {report_date_msk_date}...")

    if not UIS_API_TOKEN:
        print("Ошибка: Не установлен UIS_API_TOKEN.")
        return

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
    REPORT_DATE_MSK_TEST = datetime.now().date()

    report_output = get_section_3_report_data(report_date_msk_date=REPORT_DATE_MSK_TEST)
    for line in report_output:
        print(line)

    # 2. Запустить тестовый скрипт для вывода данных заказа RetailCRM:
    test_dump_order_data(report_date_msk_date=REPORT_DATE_MSK_TEST, num_orders_to_dump=1)

    # 3. Запустить тестовый скрипт для вывода данных звонков UIS:
    test_dump_uis_call_data(report_date_msk_date=REPORT_DATE_MSK_TEST, num_calls_to_dump=10)
