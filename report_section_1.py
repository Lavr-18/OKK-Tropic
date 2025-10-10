import os
import requests
import aiohttp
import asyncio
from datetime import datetime, timedelta, timezone

# --- Вспомогательные функции для работы с датами и временем ---

# Смещение часового пояса Москвы относительно UTC
MSK_OFFSET = timedelta(hours=3)

# Коды статусов заказов, которые относятся к группам "Новый" и "Согласование"
# Берем только ключевые коды статусов, которые должны быть актуальны для активной работы.
# Полный список статусов, которые вы привели в группах "New" и "Approval".
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
    # Удаляем информацию о часовом поясе, чтобы вернуть "наивный" объект времени по Москве
    return (dt_utc + MSK_OFFSET).replace(tzinfo=None)


def to_utc(dt_msk):
    """Конвертирует объект datetime из MSK в UTC."""
    # Устанавливаем часовой пояс MSK, а затем конвертируем в UTC
    dt_with_msk_tz = dt_msk.replace(tzinfo=timezone(MSK_OFFSET))
    return dt_with_msk_tz.astimezone(timezone.utc)


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
    # Формат API RetailCRM, который вы используете для фильтрации
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
            return datetime.strptime(dt_string.split('.')[0].rstrip('Z'), "%Y-%m-%dT%H:%M:%S").replace(
                tzinfo=timezone.utc)
        except ValueError:
            try:
                # Попытка парсинга без секунд (формат в ответе get task)
                return datetime.strptime(dt_string, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
            except ValueError:
                print(f"Ошибка парсинга даты/времени: {dt_string}. Возвращено None.")
                return None


# --- Основная логика отчета ---

async def get_order_status_group_async(session, order_id, api_url, api_key, site):
    """
    Асинхронно получает статус заказа по его ID.
    Возвращает статус заказа (строка) или None в случае ошибки/отсутствия.
    """
    order_url = f"{api_url}/orders/{order_id}?by=id&site={site}&apiKey={api_key}"

    try:
        async with session.get(order_url) as response:
            data = await response.json()
            if data.get('success') and 'order' in data:
                # Возвращаем код статуса из поля 'status'
                return data['order'].get('status')

            # Обработка случая, когда заказ не найден или API вернул ошибку
            if data.get('errorMsg') == 'Order not found':
                # Это может произойти, если заказ удален или имеет другую проблему.
                # Для целей отчета мы можем просто игнорировать такие задачи.
                return None

                # Логируем другие ошибки API
            if not data.get('success'):
                print(f"Ошибка API при получении заказа {order_id}: {data.get('errorMsg', 'Неизвестная ошибка')}")

    except aiohttp.ClientError as e:
        print(f"Ошибка сети при запросе статуса заказа {order_id}: {e}")
    except Exception as e:
        print(f"Неожиданная ошибка при обработке запроса заказа {order_id}: {e}")

    return None  # Возвращаем None при любых ошибках или не-успешном результате


async def process_task_async(session, task_id, api_url, api_key, site, start_of_report_day_utc):
    """
    Асинхронно обрабатывает одну задачу: получает детали, проверяет время и статус заказа.
    Возвращает словарь с деталями задачи или None, если задача не подходит.
    """
    task_url = f"{api_url}/tasks/{task_id}?apiKey={api_key}"

    try:
        async with session.get(task_url) as response:
            task_data = await response.json()

            if not task_data.get('success') or 'task' not in task_data:
                # print(f"Не удалось получить детали задачи {task_id}: {task_data.get('errorMsg', 'Неизвестная ошибка')}")
                return None

            task = task_data['task']

            # 1. Проверка, что задача не завершена
            if task.get('complete') is True:
                return None

            # 2. Проверка даты задачи (задача должна быть просрочена или на отчетный день)
            # В RetailCRM 'datetime' - это запланированная дата и время (MSK).
            task_datetime_str = task.get('datetime')
            if not task_datetime_str:
                return None  # Игнорируем задачи без даты

            task_datetime_msk_naive = datetime.strptime(task_datetime_str, "%Y-%m-%d %H:%M")
            task_datetime_utc = to_utc(task_datetime_msk_naive).replace(tzinfo=timezone.utc)

            # Проверяем, что задача не была просрочена до начала отчетного дня
            # или ее срок истекает в течение отчетного дня.
            # Мы хотим включить все задачи, срок которых был раньше, чем 00:00:00 (MSK) отчетного дня,
            # а также задачи, срок которых приходится на отчетный день.
            if task_datetime_utc >= start_of_report_day_utc:
                # Если задача на отчетный день или позже - игнорируем
                return None

            # 3. Проверка статуса заказа
            order_info = task.get('order')
            if order_info and 'id' in order_info:
                order_id = order_info['id']
                order_status = await get_order_status_group_async(session, order_id, api_url, api_key, site)

                # Фильтрация по статусу заказа
                if order_status not in TARGET_ORDER_STATUSES:
                    # print(f"Задача {task_id} проигнорирована: статус заказа {order_id} = '{order_status}' не в целевых группах.")
                    return None

                # Добавляем номер заказа для отчета
                order_number = order_info.get('number', str(order_id))
            else:
                # Если задача не привязана к заказу, она игнорируется,
                # так как мы хотим учитывать только те, что относятся к заказам.
                return None

            # 4. Формирование результата, если все проверки пройдены
            return {
                'id': task_id,
                'text': task.get('text', 'Нет текста'),
                'datetime_msk': to_msk(task_datetime_utc).strftime("%d.%m.%Y %H:%M"),  # Время задачи в MSK
                'order_number': order_number
            }

    except aiohttp.ClientError as e:
        print(f"Ошибка сети при запросе задачи {task_id}: {e}")
    except Exception as e:
        print(f"Неожиданная ошибка при обработке задачи {task_id}: {e}")

    return None


async def get_overdue_tasks_section(api_url, api_key, site, report_date_msk_date_obj):
    """
    Асинхронно получает список просроченных задач, привязанных к заказам со статусами
    из групп "Новый" и "Согласование".
    """
    start_of_report_day_utc, _, _ = get_report_timeframes_utc(report_date_msk_date_obj)

    # Дата в UTC, на которую нам нужно получить список *просроченных* задач.
    # RetailCRM API фильтрует по `datetime` (дате, до которой задача должна быть выполнена).
    # Мы хотим получить задачи, срок выполнения которых истек *до* начала отчетного дня.

    # 00:00 MSK отчетного дня в UTC - это максимальный срок для отбора просроченных задач
    filter_date_utc = format_datetime_for_api(start_of_report_day_utc)

    # API-запрос на получение списка задач
    list_tasks_url = (
        f"{api_url}/tasks?apiKey={api_key}&filter[status]=not-completed"
        f"&filter[dateTo]={filter_date_utc}&limit=100"  # Фильтр для просроченных (истекших до 00:00 MSK)
    )

    all_task_ids = []
    page = 1

    # Используем aiohttp для всех асинхронных запросов
    async with aiohttp.ClientSession() as session:
        while True:
            current_url = f"{list_tasks_url}&page={page}"
            try:
                async with session.get(current_url) as response:
                    data = await response.json()

                    if not data.get('success') or not data.get('tasks'):
                        break

                    # Собираем ID задач
                    all_task_ids.extend([task['id'] for task in data['tasks']])

                    if len(data['tasks']) < 100:
                        break  # Если меньше лимита, значит, это последняя страница
                    page += 1
                    await asyncio.sleep(0.5)  # Небольшая задержка между страницами

            except aiohttp.ClientError as e:
                print(f"Ошибка сети при запросе списка задач (страница {page}): {e}")
                break
            except Exception as e:
                print(f"Неожиданная ошибка при получении списка задач: {e}")
                break

        # Параллельная обработка всех найденных задач
        tasks_to_process = [
            process_task_async(session, task_id, api_url, api_key, site, start_of_report_day_utc)
            for task_id in all_task_ids
        ]

        # Запускаем все запросы одновременно. limit=20 для управления нагрузкой на API.
        results = await asyncio.gather(*tasks_to_process)

    # Фильтруем None (задачи, которые не прошли проверку)
    overdue_tasks = [task for task in results if task is not None]

    # Сортировка по дате (самые старые просроченные - в начале)
    overdue_tasks.sort(key=lambda x: datetime.strptime(x['datetime_msk'], "%d.%m.%Y %H:%M"))

    # Формирование отчета
    if not overdue_tasks:
        return ""

    report_parts = [
        "<b>🔴 ПРОСРОЧЕННЫЕ ЗАДАЧИ ПО АКТУАЛЬНЫМ ЗАКАЗАМ</b>",
        f"*(Учитываются заказы в статусах групп 'Новый' и 'Согласование')*",
        ""
    ]

    for task in overdue_tasks:
        report_parts.append(
            f"❗️ <b>Заказ {task['order_number']}</b> (Срок: {task['datetime_msk']})\n"
            f"   - {task['text']}"
        )

    return "\n".join(report_parts)

# Пример использования (для тестирования)
# import datetime
# async def main():
#     # Замените на реальные данные для теста
#     API_URL = "https://tropichouse.retailcrm.ru/api/v5"
#     API_KEY = "ВАШ_КЛЮЧ"
#     SITE = "tropichouse"
#
#     # Дата отчета - вчера (предполагая, что скрипт запускается сегодня)
#     report_date = datetime.date.today() - timedelta(days=1)
#
#     report = await get_overdue_tasks_section(API_URL, API_KEY, SITE, report_date)
#     print(report)

# if __name__ == "__main__":
#     # Для запуска асинхронного кода в скрипте (если он не запускается из другого асинхронного контекста)
#     # asyncio.run(main())
#     pass