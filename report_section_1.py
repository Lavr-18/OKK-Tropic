import os
import requests
from datetime import datetime, timedelta, timezone

# --- Вспомогательные функции для работы с датами и временем ---

# Смещение часового пояса Москвы относительно UTC
MSK_OFFSET = timedelta(hours=3)


def to_msk(dt_utc):
    """Конвертирует объект datetime из UTC в MSK."""
    return dt_utc + MSK_OFFSET


def to_utc(dt_msk):
    """Конвертирует объект datetime из MSK в UTC."""
    return dt_msk - MSK_OFFSET


def get_report_timeframes_utc(report_date_msk_date_obj):
    """
    Рассчитывает UTC временные рамки для отчетного дня и крайнего срока проверки контролером.
    report_date_msk_date_obj: объект date (например, datetime.date(2025, 7, 14))
    """
    # Начало отчетного дня в MSK (00:00:00)
    start_of_report_day_msk = datetime.combine(report_date_msk_date_obj, datetime.min.time())
    # Конец отчетного дня в MSK (23:59:59)
    end_of_report_day_msk = datetime.combine(report_date_msk_date_obj,
                                             datetime.max.time().replace(second=59, microsecond=999999))

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
    """Парсит строку даты/времени RetailCRM API в объект datetime с UTC.
    Обрабатывает различные форматы, включая те, что без секунд."""
    if dt_string is None:
        return None
    try:
        # Попытка парсинга с секундами (наиболее полный формат)
        # RetailCRM может возвращать формат "YYYY-MM-DD HH:MM:SS" без T и Z
        return datetime.strptime(dt_string, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        try:
            # Попытка парсинга с 'T' и 'Z' (ISO 8601, часто используется в API)
            return datetime.strptime(dt_string, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        except ValueError:
            try:
                # Новая попытка парсинга без секунд (YYYY-MM-DD HH:MM)
                return datetime.strptime(dt_string, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
            except ValueError:
                print(f"Предупреждение: Некорректный формат даты/времени API: {dt_string}. Возвращено None.")
                return None


# --- Функции для получения данных из RetailCRM ---

def get_managers(retailcrm_base_url, api_key):
    """
    Получает список всех активных менеджеров из RetailCRM.
    Возвращает словарь {manager_id: full_name}.
    """
    url = f"{retailcrm_base_url}/users"
    # Запрашиваем всех активных менеджеров
    params = {"apiKey": api_key, "filter[isManager]": 1, "filter[active]": 1, "limit": 100}

    all_managers_from_api = {}
    page = 1
    while True:
        params["page"] = page
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()  # Вызывает исключение для HTTP ошибок (4xx или 5xx)
            data = response.json()

            for user in data.get('users', []):
                full_name = f"{user.get('firstName', '')} {user.get('lastName', '')}".strip()
                if not full_name:  # Если имя/фамилия отсутствуют
                    full_name = user.get('email', f"Manager {user.get('id', 'Unknown')}")
                all_managers_from_api[user['id']] = full_name

            # Проверяем пагинацию
            if 'pagination' not in data or data['pagination']['currentPage'] >= data['pagination']['totalPageCount']:
                break  # Нет информации о пагинации или достигнута последняя страница
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
    """
    Получает задачи, срок выполнения (deadline) которых приходится на указанный UTC-период.
    """
    url = f"{retailcrm_base_url}/tasks"
    params = {
        "apiKey": api_key,
        "filter[dateFrom]": format_datetime_for_api(start_utc),  # Использование dateFrom (дата выполнения)
        "filter[dateTo]": format_datetime_for_api(end_utc),      # Использование dateTo (дата выполнения)
        "limit": 100  # Максимальное количество элементов на странице
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


def get_section_1_report_data(report_date_msk_date_obj, retailcrm_base_url, api_key):
    """
    Формирует данные для Пункта 1 отчета ОКК:
    "Маша - задач поставлено 20, перенесено (тобишь не выполнила) - 10"
    """
    print("Получение данных для отчета по невыполненным задачам (Пункт 1)...")

    # Получаем список всех активных менеджеров
    managers = get_managers(retailcrm_base_url, api_key)
    if managers is None:
        return ["Не удалось получить список менеджеров."]

    # Рассчитываем временные рамки для отчетного дня (срок выполнения)
    # и крайнего срока проверки контролером.
    (start_of_report_day_utc,
     end_of_report_day_utc,
     controller_deadline_utc) = get_report_timeframes_utc(report_date_msk_date_obj)

    print(f"Отладка: Отчетный день (МСК): {report_date_msk_date_obj.strftime('%Y-%m-%d')}")
    print(f"Отладка: Начало отчетного дня (UTC): {start_of_report_day_utc}")
    print(f"Отладка: Конец отчетного дня (UTC): {end_of_report_day_utc}")
    print(f"Отладка: Крайний срок проверки контролером (UTC): {controller_deadline_utc}")

    # --- Подсчет "поставлено" (задачи со сроком выполнения в отчетный день) ---
    tasks_due_today = get_tasks_due_in_period( # Используем новую функцию
        retailcrm_base_url, api_key, start_of_report_day_utc, end_of_report_day_utc
    )
    if tasks_due_today is None:
        return ["Не удалось получить задачи со сроком выполнения в отчетный период."]

    # Инициализация счетчиков для каждого менеджера
    manager_report_data = {
        manager_id: {"поставлено": 0, "выполнено": 0, "перенесено": 0}
        for manager_id in managers
    }

    # Вычисляем начало и конец следующего дня в UTC для определения "перенесенных" задач
    next_day_date_msk = report_date_msk_date_obj + timedelta(days=1)
    start_of_next_day_msk = datetime.combine(next_day_date_msk, datetime.min.time())
    end_of_next_day_msk = datetime.combine(next_day_date_msk, datetime.max.time().replace(second=59, microsecond=999999))
    start_of_next_day_utc = to_utc(start_of_next_day_msk).replace(tzinfo=timezone.utc)
    end_of_next_day_utc = to_utc(end_of_next_day_msk).replace(tzinfo=timezone.utc)

    print("\n--- Отладка: Обработка каждой задачи со сроком выполнения в отчетный день ---")
    for task in tasks_due_today:
        task_id = task.get('id')
        performer_id = task.get('performer')
        manager_name = managers.get(performer_id, "Неизвестный менеджер")
        complete_status = task.get('complete')
        created_at_str = task.get('createdAt')
        task_due_datetime_str = task.get('datetime') # Срок выполнения задачи (DateTime в RetailCRM, соответствует полю 'datetime')

        created_at_dt = parse_api_datetime(created_at_str)
        task_due_datetime_dt = parse_api_datetime(task_due_datetime_str)

        print(f"  Задача ID: {task_id}")
        print(f"    Менеджер: {manager_name} (ID: {performer_id})")
        print(f"    Создана (API): {created_at_str} (UTC: {created_at_dt})")
        print(f"    Срок выполнения (API): {task_due_datetime_str} (UTC: {task_due_datetime_dt})")
        print(f"    Статус 'complete': {complete_status}")


        if performer_id in managers:
            manager_report_data[performer_id]["поставлено"] += 1
            print(f"    -> Зачислена в 'поставлено' для {manager_name} (срок выполнения в отчетный день).")

            if complete_status is True:
                manager_report_data[performer_id]["выполнено"] += 1
                print(f"    -> Зачислена в 'выполнено' для {manager_name}.")
            else:  # Задача не выполнена
                if task_due_datetime_dt:
                    # Если срок выполнения задачи попадает на следующий день
                    if start_of_next_day_utc <= task_due_datetime_dt <= end_of_next_day_utc:
                        manager_report_data[performer_id]["перенесено"] += 1
                        print(f"    -> Зачислена в 'перенесено' для {manager_name} (срок перенесен на следующий день).")
                    else:
                        print(f"    -> Задача не выполнена, но НЕ 'перенесена' (срок не на следующий день).")
                else:
                    print(f"    -> Задача не выполнена, срок выполнения НЕ указан или некорректен.")
        else:
            print(f"    -> Пропущена, менеджер {performer_id} не найден в списке активных или не является менеджером.")
        print("---")

    print("\nОтладка: Задачи 'поставлено', 'выполнено' и 'перенесено' подсчитаны.")

    # Формируем строки для отчета
    report_lines = []

    # Общее количество задач, которые были поставлены
    total_tasks_assigned = sum(data['поставлено'] for data in manager_report_data.values())
    report_lines.append(f"1. Проверка невыполненных задач: {total_tasks_assigned}")


    # Сортируем менеджеров по именам для отчета
    sorted_manager_ids = sorted(managers.keys(), key=lambda x: managers[x])

    for manager_id in sorted_manager_ids:
        manager_name = managers[manager_id]
        data = manager_report_data[manager_id]

        if data['поставлено'] > 0:  # Выводим только менеджеров, у которых были поставленные задачи
            exclamation = ""
            # Восклицательный знак, если есть задачи, которые не были выполнены и не были перенесены
            unaccounted_tasks = data['поставлено'] - data['выполнено'] - data['перенесено']
            if unaccounted_tasks > 0:
                exclamation = "❗"

            report_lines.append(
                f"{manager_name.split(' ')[0]} - поставлено {data['поставлено']}/выполнено {data['выполнено']} (перенесенных было {data['перенесено']}){exclamation}"
            )
    return report_lines


# Если этот файл запущен напрямую, выполним только его функцию (для тестирования)
if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()
    # !!! Установите желаемую дату отчета в формате datetime.date(ГОД, МЕСЯЦ, ДЕНЬ) !!!
    # Например, для отчета за 17 июля 2025 года:
    REPORT_DATE_MSK_TEST = datetime(2025, 7, 21).date() # Используем дату из вашего примера

    RETAILCRM_BASE_URL_TEST = os.getenv("RETAILCRM_BASE_URL")
    API_KEY_TEST = os.getenv("RETAILCRM_API_TOKEN")

    if not API_KEY_TEST or not RETAILCRM_BASE_URL_TEST:
        print(
            "Ошибка: Не все переменные окружения (RETAILCRM_API_TOKEN, RETAILCRM_BASE_URL) установлены для автономного запуска report_section_1.py.")
        print("Пожалуйста, убедитесь, что ваш файл .env настроен правильно.")
    else:
        print("Запуск report_section_1.py в тестовом режиме.")
        report_output = get_section_1_report_data(REPORT_DATE_MSK_TEST, RETAILCRM_BASE_URL_TEST, API_KEY_TEST)
        for line in report_output:
            print(line)