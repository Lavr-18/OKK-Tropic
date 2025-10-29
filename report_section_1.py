import os
import requests
from datetime import datetime, timedelta, timezone
from collections import defaultdict

# --- Вспомогательные функции для работы с датами и временем ---

# Смещение часового пояса Москвы относительно UTC
MSK_OFFSET = timedelta(hours=3)

# Коды статусов заказов, которые относятся к группам "Новый" и "Согласование"
TARGET_ORDER_STATUSES = {
    # Группа "Новый"
    "new", "gotovo-k-soglasovaniiu", "soglasovat-sostav", "agree-absence", "novyi-predoplachen", "novyi-oplachen",
    # Группа "Согласование"
    "availability-confirmed", "client-confirmed", "offer-analog", "ne-dozvonilis", "perezvonit-pozdnee",
    "otpravili-varianty-na-pochtu", "otpravili-varianty-v-vatsap", "ready-to-wait", "waiting-for-arrival",
    "klient-zhdet-foto-s-zakupki", "vizit-v-shourum", "ozhidaet-oplaty", "gotovim-kp", "kp-gotovo-k-zashchite",
    "soglasovanie-kp", "proekt-visiak", "soglasovano", "oplacheno", "prepayed", "soglasovan-ozhidaet-predoplaty",
    "vyezd-biologa-oplachen", "vyezd-biologa-zaplanirovano", "predoplata-poluchena", "oplata-ne-proshla",
    "proverka-nalichiia", "obsluzhivanie-zaplanirovano", "obsluzhivanie-soglasovanie", "predoplachen-soglasovanie",
    "servisnoe-obsluzhivanie-oplacheno", "zakaz-obrabotan-soglasovanie", "vyezd-biologa-soglasovanie"
}


def to_msk(dt_utc):
    """Конвертирует объект datetime из UTC в MSK."""
    # Важно: для точности при работе с наивными объектами, лучше явно установить UTC
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=timezone.utc)
    return dt_utc.astimezone(timezone(MSK_OFFSET)).replace(tzinfo=None)


def to_utc(dt_msk):
    """Конвертирует объект datetime из MSK в UTC."""
    dt_with_msk_tz = dt_msk.replace(tzinfo=timezone(MSK_OFFSET))
    return dt_with_msk_tz.astimezone(timezone.utc).replace(tzinfo=None)  # Возвращаем наивный объект для совместимости


def get_report_timeframes_utc(report_date_msk_date_obj):
    """
    Рассчитывает UTC временные рамки для отчетного дня.
    Для отчета в 22:00 МСК, отчетный период заканчивается в 22:00:00 МСК.
    """
    # Начало отчетного дня в MSK (00:00:00)
    start_of_report_day_msk = datetime.combine(report_date_msk_date_obj, datetime.min.time())

    # ИСПРАВЛЕНИЕ: Конец отчетного периода в MSK (22:00:00)
    end_of_report_day_msk = datetime.combine(report_date_msk_date_obj,
                                             datetime.min.time().replace(hour=22, second=0, microsecond=0))

    # Крайний срок проверки контролером (01:00 MSK следующего дня)
    controller_deadline_msk_date = report_date_msk_date_obj + timedelta(days=1)
    controller_deadline_msk = datetime.combine(controller_deadline_msk_date, datetime.min.time().replace(hour=1))

    # Конвертируем все в UTC
    start_of_report_day_utc = to_utc(start_of_report_day_msk).replace(tzinfo=timezone.utc)
    end_of_report_day_utc = to_utc(end_of_report_day_msk).replace(tzinfo=timezone.utc)
    controller_deadline_utc = to_utc(controller_deadline_msk).replace(tzinfo=timezone.utc)

    return start_of_report_day_utc, end_of_report_day_utc, controller_deadline_utc


def format_datetime_for_api(dt_object):
    """Форматирует объект datetime в строковый формат RetailCRM API (UTC)."""
    # Убеждаемся, что объект UTC и имеет информацию о часовом поясе
    if dt_object.tzinfo is None:
        dt_object = dt_object.replace(tzinfo=timezone.utc)
    return dt_object.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_api_datetime(dt_string):
    """Парсит строку даты/времени RetailCRM API в объект datetime с UTC."""
    if dt_string is None:
        return None
    try:
        # Попытка парсинга с секундами (наиболее полный формат)
        return datetime.strptime(dt_string, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        try:
            # Попытка парсинга с 'T' и 'Z' (ISO 8601, часто используется в API)
            return datetime.strptime(dt_string.split('.')[0].rstrip('Z'), "%Y-%m-%dT%H:%M:%S").replace(
                tzinfo=timezone.utc)
        except ValueError:
            try:
                # Попытка парсинга без секунд (YYYY-MM-DD HH:MM)
                return datetime.strptime(dt_string, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
            except ValueError:
                return None


# --- Новая функция: Получение статуса заказа ---
def get_order_status(order_id, api_url, api_key, site):
    """
    Синхронно получает статус заказа по его ID.
    Возвращает код статуса заказа (строка) или None в случае ошибки/отсутствия.
    """
    order_url = f"{api_url}/orders/{order_id}?by=id&site={site}&apiKey={api_key}"

    try:
        response = requests.get(order_url)
        response.raise_for_status()
        data = response.json()

        if data.get('success') and 'order' in data:
            return data['order'].get('status')

        if data.get('errorMsg') == 'Order not found':
            return None

        if not data.get('success'):
            print(f"Ошибка API при получении заказа {order_id}: {data.get('errorMsg', 'Неизвестная ошибка')}")

    except requests.exceptions.RequestException as e:
        print(f"Ошибка сети/HTTP при запросе статуса заказа {order_id}: {e}")
    except Exception as e:
        print(f"Неожиданная ошибка при обработке запроса заказа {order_id}: {e}")

    return None


# --- Функции для получения данных из RetailCRM ---

def get_managers(retailcrm_base_url, api_key):
    # ... (Оставляем функцию get_managers без изменений, она корректна)
    url = f"{retailcrm_base_url}/users"
    params = {"apiKey": api_key, "filter[isManager]": 1, "filter[active]": 1, "limit": 100}

    all_managers_from_api = {}
    page = 1
    while True:
        params["page"] = page
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            for user in data.get('users', []):
                full_name = f"{user.get('firstName', '')} {user.get('lastName', '')}".strip()
                if not full_name:
                    full_name = user.get('email', f"Manager {user.get('id', 'Unknown')}")
                all_managers_from_api[user['id']] = full_name

            if 'pagination' not in data or data['pagination']['currentPage'] >= data['pagination']['totalPageCount']:
                break
            page += 1
        except requests.exceptions.RequestException as e:
            print(f"Ошибка при получении списка менеджеров: {e}")
            return None
        except Exception as e:
            print(f"Непредвиденная ошибка при получении менеджеров: {e}")
            return None

    print(f"Отладка: Получено {len(all_managers_from_api)} активных менеджеров из API.")
    return all_managers_from_api


def get_tasks_due_in_period(retailcrm_base_url, api_key, start_utc, end_utc):
    # ... (Оставляем функцию get_tasks_due_in_period без изменений, она корректна)
    url = f"{retailcrm_base_url}/tasks"
    params = {
        "apiKey": api_key,
        "filter[dateFrom]": format_datetime_for_api(start_utc),
        "filter[dateTo]": format_datetime_for_api(end_utc),
        "limit": 100
    }

    tasks_in_period = []
    page = 1
    while True:
        params["page"] = page
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            tasks = data.get('tasks', [])
            tasks_in_period.extend(tasks)

            if not tasks or (
                    'pagination' in data and data['pagination']['currentPage'] >= data['pagination']['totalPageCount']):
                break
            page += 1
        except requests.exceptions.RequestException as e:
            print(f"Ошибка при получении задач со сроком выполнения в период ({start_utc} - {end_utc}): {e}")
            return None
        except Exception as e:
            print(f"Непредвиденная ошибка при получении задач со сроком выполнения в период: {e}")
            return None
    print(f"Отладка: Получено {len(tasks_in_period)} задач со сроком выполнения в период отчета.")
    return tasks_in_period


def get_section_1_report_data(report_date_msk_date_obj, retailcrm_base_url, api_key, site):
    """
    Формирует данные для Пункта 1 отчета ОКК, включая фильтрацию по статусу заказа.
    """
    print("Получение данных для отчета по невыполненным задачам (Пункт 1) с фильтрацией по статусу...")

    # Получаем список всех активных менеджеров
    managers = get_managers(retailcrm_base_url, api_key)
    if managers is None:
        return ["1. Проверка невыполненных задач: 0\nНе удалось получить список менеджеров."]

    # Рассчитываем временные рамки
    (start_of_report_day_utc, end_of_report_day_utc, _) = get_report_timeframes_utc(report_date_msk_date_obj)

    # --- Подсчет "поставлено" (задачи со сроком выполнения в отчетный день) ---
    # В этом месте end_of_report_day_utc уже содержит 22:00:00 МСК (19:00:00 UTC)
    tasks_due_today = get_tasks_due_in_period(
        retailcrm_base_url, api_key, start_of_report_day_utc, end_of_report_day_utc
    )
    if tasks_due_today is None:
        return [
            "1. Проверка невыполненных задач: 0\nНе удалось получить задачи со сроком выполнения в отчетный период."]

    # Инициализация счетчиков для каждого менеджера
    manager_report_data = defaultdict(lambda: {"поставлено": 0, "выполнено": 0, "перенесено": 0})

    # Вычисляем начало и конец следующего дня в UTC для определения "перенесенных" задач
    next_day_date_msk = report_date_msk_date_obj + timedelta(days=1)
    start_of_next_day_msk = datetime.combine(next_day_date_msk, datetime.min.time())
    start_of_next_day_utc = to_utc(start_of_next_day_msk).replace(tzinfo=timezone.utc)

    tasks_processed_count = 0
    tasks_filtered_out_count = 0

    print("\n--- Отладка: Обработка и фильтрация задач по статусу заказа ---")

    # Сначала фильтруем, затем считаем
    for task in tasks_due_today:
        task_id = task.get('id')
        performer_id = task.get('performer')
        order_info = task.get('order', {})
        order_id = order_info.get('id')

        # 1. Фильтрация по статусу заказа
        if order_id:
            order_status = get_order_status(order_id, retailcrm_base_url, api_key, site)

            if order_status not in TARGET_ORDER_STATUSES:
                tasks_filtered_out_count += 1
                continue  # Пропускаем задачу, если статус нецелевой
        else:
            # Задачи без заказа всегда пропускаются (т.к. отчет по заказам)
            tasks_filtered_out_count += 1
            continue

        # 2. Обработка статистики (только для прошедших фильтр задач)
        tasks_processed_count += 1

        manager_name_full = managers.get(performer_id, "Неизвестный менеджер")

        if performer_id in managers:
            manager_report_data[performer_id]["поставлено"] += 1

            # a) Подсчет "выполнено"
            if task.get('complete') is True:
                manager_report_data[performer_id]["выполнено"] += 1

            # b) Подсчет "перенесено"
            elif task.get('complete') is False:
                task_due_datetime_dt = parse_api_datetime(task.get('datetime'))

                # Проверяем, что срок ИЗНАЧАЛЬНО был на отчетный день
                # Поскольку end_of_report_day_utc теперь 22:00, это условие не изменится
                if task_due_datetime_dt and task_due_datetime_dt.date() == start_of_report_day_utc.date():
                    next_task_datetime_str = task.get('nextTime')

                    if next_task_datetime_str and task.get('datetime') != next_task_datetime_str:
                        next_task_datetime_dt = parse_api_datetime(next_task_datetime_str)

                        # Если перенесено на следующий день (или позже)
                        if next_task_datetime_dt and next_task_datetime_dt.date() > report_date_msk_date_obj:
                            manager_report_data[performer_id]["перенесено"] += 1

    print(f"Отладка: Всего задач, срок которых в отчетный день: {len(tasks_due_today)}")
    print(f"Отладка: Проигнорировано (нет заказа/нецелевой статус): {tasks_filtered_out_count}")
    print(f"Отладка: Обработано (целевые задачи): {tasks_processed_count}")

    # Формируем строки для отчета (старый формат)
    report_lines = []

    # Общее количество задач, которые были поставлены (прошедшие фильтр)
    total_tasks_assigned = sum(data['поставлено'] for data in manager_report_data.values())
    report_lines.append(f"1. Проверка невыполненных задач: {total_tasks_assigned}")

    # Сортируем менеджеров по именам для отчета
    sorted_manager_ids = sorted(managers.keys(), key=lambda x: managers[x])

    for manager_id in sorted_manager_ids:
        manager_name = managers[manager_id]
        data = manager_report_data[manager_id]

        if data['поставлено'] > 0:  # Выводим только менеджеров, у которых были поставленные задачи

            # Учитываем все, что не выполнено и не перенесено (просрочено)
            unaccounted_tasks = data['поставлено'] - data['выполнено']

            # Восклицательный знак, если есть невыполненные задачи (даже если перенесены)
            # Чтобы соответствовать вашему примеру, где "Вера - поставлено 16/выполнено 10 (перенесенных было 1)❗️"
            # Проще поставить ❗️ если 'поставлено' > 'выполнено'
            exclamation = "❗️" if data['поставлено'] > data['выполнено'] else ""

            # Имя сотрудника берем только первое слово (имя)
            first_name = manager_name.split(' ')[0]

            report_lines.append(
                f"{first_name} - поставлено {data['поставлено']}/выполнено {data['выполнено']} (перенесенных было {data['перенесено']}){exclamation}"
            )

    # Если задач 0, все равно выводим заголовок:
    if total_tasks_assigned == 0:
        report_lines.append(
            f"Нет задач по актуальным заказам со сроком выполнения {report_date_msk_date_obj.strftime('%d.%m.%Y')}")

    return report_lines


# Если этот файл запущен напрямую, выполним только его функцию (для тестирования)
if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()

    # !!! Установите желаемую дату отчета в формате datetime.date(ГОД, МЕСЯЦ, ДЕНЬ) !!!
    # ИСПРАВЛЕНИЕ: Теперь тестируем текущий день
    REPORT_DATE_MSK_TEST = datetime.now().date()  # Текущий день

    RETAILCRM_BASE_URL_TEST = os.getenv("RETAILCRM_BASE_URL")
    API_KEY_TEST = os.getenv("RETAILCRM_API_TOKEN")
    SITE_CODE_TEST = os.getenv("RETAILCRM_SITE_CODE")

    if not API_KEY_TEST or not RETAILCRM_BASE_URL_TEST or not SITE_CODE_TEST:
        print(
            "Ошибка: Не все переменные окружения установлены для автономного запуска report_section_1.py.")
    else:
        print("Запуск report_section_1.py в тестовом режиме.")
        # В синхронном режиме требуется передать 4 аргумента, включая site
        report_output = get_section_1_report_data(
            report_date_msk_date_obj=REPORT_DATE_MSK_TEST,
            retailcrm_base_url=RETAILCRM_BASE_URL_TEST,
            api_key=API_KEY_TEST,
            site=SITE_CODE_TEST
        )
        for line in report_output:
            print(line)
