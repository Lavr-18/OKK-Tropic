import os
import requests
from dotenv import load_dotenv
import json
from datetime import datetime, timedelta, timezone
from openai import OpenAI

# --- 1. Загрузка переменных окружения ---
load_dotenv()

RETAILCRM_BASE_URL = os.getenv("RETAILCRM_BASE_URL")
RETAILCRM_API_TOKEN = os.getenv("RETAILCRM_API_TOKEN")
RETAILCRM_SITE_CODE = os.getenv("RETAILCRM_SITE_CODE")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Инициализация клиента OpenAI
openai_client = None
if OPENAI_API_KEY:
    try:
        openai_client = OpenAI(api_key=OPENAI_API_KEY)
    except Exception as e:
        # Это сообщение должно выводиться только если скрипт запускается напрямую
        if __name__ == "__main__":
            print(f"Ошибка при инициализации OpenAI клиента: {e}")
            print("Проверка клиентов через OpenAI API будет недоступна.")
else:
    if __name__ == "__main__":
        print("Внимание: OPENAI_API_KEY не установлен. Проверка клиентов через OpenAI API будет недоступна.")

# --- Коррекция RETAILCRM_BASE_URL, если он содержит /api/v5 ---
if RETAILCRM_BASE_URL and RETAILCRM_BASE_URL.endswith('/api/v5'):
    RETAILCRM_BASE_URL = RETAILCRM_BASE_URL.rsplit('/api/v5', 1)[0]

# --- Словарь для перевода причин ошибок с английского на русский ---
ERROR_TRANSLATIONS = {
    "too short": "Поле слишком короткое",
    "too long": "Поле слишком длинное",
    "contains digits": "Содержит только цифры",
    "no letters": "Не содержит букв",
    "contains spaces": "Поле 'Имя' содержит пробелы (возможно, это ФИО)",
    "not a real name": "Не похоже на реальное имя/фамилию",
    "not a real last name": "Не похоже на реальную фамилию",
    "not a real patronymic": "Не похоже на реальное отчество",  # Добавил для точности
    "typo or grammatical error": "Содержит опечатку или грамматическую ошибку",
    "nickname": "Является кличкой",
    "meaningless characters": "Содержит бессмысленные символы",
    "url": "Является URL",
    "email": "Является email-адресом",
    "initials/abbreviation": "Является инициалами/аббревиатурой",
    "generic word": "Содержит общее слово",
    "test value": "Является тестовым значением",
    "OK": "ОК"
}


def get_russian_error_message(english_reason):
    """
    Возвращает русскую формулировку ошибки по английской причине.
    """
    clean_reason = english_reason.strip().lower().rstrip('.')
    return ERROR_TRANSLATIONS.get(clean_reason, f"Неизвестная ошибка: {english_reason}")


# --- 2. Вспомогательные функции ---

def get_yesterday_msk_range():
    """
    Возвращает начало и конец вчерашнего дня в Московском часовом поясе.
    """
    MSK_TZ = timezone(timedelta(hours=3))
    current_datetime = datetime.now(MSK_TZ)
    yesterday_date = (current_datetime - timedelta(days=1)).date()
    start_of_yesterday = datetime.combine(yesterday_date, datetime.min.time(), tzinfo=MSK_TZ)
    end_of_yesterday = datetime.combine(yesterday_date, datetime.max.time(), tzinfo=MSK_TZ)
    return start_of_yesterday, end_of_yesterday


def get_retailcrm_orders(base_url, api_key, site_code, start_date, end_date):
    """
    Получает список заказов из RetailCRM API за указанный период.
    Обрабатывает пагинацию.
    """
    url = f"{base_url}/api/v5/orders"
    all_orders = []
    page = 1
    limit = 100

    while True:
        params = {
            "apiKey": api_key,
            "site": site_code,
            "page": page,
            "limit": limit,
            "filter[createdAtFrom]": start_date.isoformat(sep=' ', timespec='seconds'),
            "filter[createdAtTo]": end_date.isoformat(sep=' ', timespec='seconds')
        }
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            if not data.get('success'):
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
            return None
        except json.JSONDecodeError:
            return None
        except Exception as e:
            return None
    return all_orders


def check_text_with_openai(text, field_name, is_last_name_empty=False):
    """
    Проверяет переданный текст (имя, фамилия, отчество) на наличие некорректных записей.
    """
    if not openai_client:
        return True, "API check skipped: API key not configured."

    if not text or not isinstance(text, str) or not text.strip():
        return False, "empty or incorrect type"

    cleaned_text = text.strip()
    if cleaned_text.lower() == "спам":
        return True, "OK"
    if len(cleaned_text) < 2:
        return False, "too short"
    if len(cleaned_text) > 70:
        return False, "too long"
    if cleaned_text.isdigit():
        return False, "contains digits"
    if not any(char.isalpha() for char in cleaned_text):
        return False, "no letters"
    if field_name == "Имя" and " " in cleaned_text:
        return False, "contains spaces"

    field_name_en = ""
    additional_instruction = ""
    if field_name == "Имя":
        field_name_en = "first name"
        if is_last_name_empty:
            additional_instruction = " It can be a first name or a last name if no last name is provided."
        else:
            additional_instruction = " It must be a first name, not a last name."
    elif field_name == "Фамилия":
        field_name_en = "last name"
    elif field_name == "Отчество":
        field_name_en = "patronymic"
    else:
        field_name_en = "name part"

    system_prompt = "You are an assistant verifying the correctness of Russian first names, last names, and patronymics. Pay close attention to typos and grammatical errors in Russian words, and recognize and accept transliterated Russian names."
    user_prompt = (
        f"Evaluate '{cleaned_text}' as a {field_name_en}.{additional_instruction} Identify typos, grammar issues, meaningless characters, URLs, emails, nicknames. "
        f"It must resemble a **real Russian name, last name, or patronymic (or their transliteration)**. "
        f"**Do not accept initials ('VA', 'DM', 'A.V.'), abbreviations, or generic words ('client', 'test')**. "
        f"'Rodion', 'Vyatkin', 'Раздольская' (as a last name) are valid. Respond 'OK' or briefly describe the single most relevant issue (max 5 words) "
        f"using one of these phrases: 'not a real name', 'not a real last name', 'not a real patronymic', 'typo or grammatical error', 'nickname', 'meaningless characters', 'url', 'email', 'initials/abbreviation', 'generic word', 'test value'."
    )
    try:
        response = openai_client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            max_tokens=20,
            temperature=0.1
        )
        ai_response_content = response.choices[0].message.content.strip()
        if ai_response_content.upper() == "OK":
            return True, "OK"
        else:
            return False, ai_response_content
    except Exception as e:
        return True, "OpenAI API check error (skipped)."


# --- 3. Основная функция для отчёта ---
def get_fio_report_data():
    """
    Проверяет имена и фамилии клиентов во вчерашних заказах и возвращает отчёт в виде списка строк.
    Эта функция будет вызываться из main.py
    """
    report_lines = []

    if not all([RETAILCRM_BASE_URL, RETAILCRM_API_TOKEN, RETAILCRM_SITE_CODE]):
        report_lines.append("Ошибка: Не все необходимые переменные окружения для RetailCRM установлены.")
        return report_lines

    start_date, end_date = get_yesterday_msk_range()

    orders = get_retailcrm_orders(RETAILCRM_BASE_URL, RETAILCRM_API_TOKEN, RETAILCRM_SITE_CODE, start_date, end_date)

    if orders is None:
        report_lines.append("Не удалось получить заказы из RetailCRM. Проверка отменена.")
        return report_lines

    if not orders:
        report_lines.append("--- Сводка по проверке ---")
        report_lines.append(f"Всего проверено заказов: 0")
        report_lines.append(f"Заказов с проблемами оформления ФИО: 0")
        return report_lines

    problem_details = []
    checked_customer_ids = set()

    for order in orders:
        order_id = order.get('id')
        customer = order.get('customer')

        if not customer:
            continue

        customer_id = customer.get('id')
        if customer_id in checked_customer_ids:
            continue

        first_name = customer.get('firstName')
        last_name = customer.get('lastName')
        patronymic = customer.get('patronymic')

        errors_for_current_customer = []

        is_first_name_valid, reason_fn_en = check_text_with_openai(first_name, "Имя",
                                                                   is_last_name_empty=(not last_name))
        if not is_first_name_valid and reason_fn_en.strip().upper() != "OK":
            reason_fn_ru = get_russian_error_message(reason_fn_en)
            errors_for_current_customer.append(f"Имя ('{first_name}') - Ошибка: {reason_fn_ru}")

        if last_name:
            is_last_name_valid, reason_ln_en = check_text_with_openai(last_name, "Фамилия")
            if not is_last_name_valid and reason_ln_en.strip().upper() != "OK":
                reason_ln_ru = get_russian_error_message(reason_ln_en)
                errors_for_current_customer.append(f"Фамилия ('{last_name}') - Ошибка: {reason_ln_ru}")

        if patronymic:
            is_patronymic_valid, reason_pat_en = check_text_with_openai(patronymic, "Отчество")
            if not is_patronymic_valid and reason_pat_en.strip().upper() != "OK":
                reason_pat_ru = get_russian_error_message(reason_pat_en)
                errors_for_current_customer.append(f"Отчество ('{patronymic}') - Ошибка: {reason_pat_ru}")

        if errors_for_current_customer:
            problem_details.append({
                "order_id": order_id,
                "customer_id": customer_id,
                "full_name": f"{first_name or ''} {last_name or ''} {patronymic or ''}".strip(),
                "errors": errors_for_current_customer
            })

        checked_customer_ids.add(customer_id)

    # --- Формирование отчета ---
    total_orders = len(orders)
    problem_count = len(problem_details)

    report_lines.append("--- Сводка по проверке ---")
    report_lines.append(f"Всего проверено заказов: {total_orders}")
    report_lines.append(f"Заказов с проблемами оформления ФИО: {problem_count}")

    if problem_count > 0:
        report_lines.append("")
        report_lines.append("--- Детализация проблемных заказов ---")
        for problem in problem_details:
            report_lines.append("")
            report_lines.append(
                f"Заказ ID: {problem['order_id']} (ссылка: {RETAILCRM_BASE_URL}/orders/{problem['order_id']}/edit)")
            report_lines.append(
                f"  Клиент ID: {problem['customer_id']} (ссылка: {RETAILCRM_BASE_URL}/customers/{problem['customer_id']}/edit)")
            report_lines.append(f"  ФИО: {problem['full_name']}")
            report_lines.append("  Проблемы:")
            for error in problem['errors']:
                report_lines.append(f"    - {error}")

    return report_lines


# --- Точка входа для отладки, если скрипт запускается отдельно ---
if __name__ == "__main__":
    report_lines = get_fio_report_data()
    for line in report_lines:
        print(line)