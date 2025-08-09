# Было:
# FROM python:3.10-slim
# Стало:
FROM python:3.11-slim

# Остальная часть файла остается без изменений
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["python", "main.py"]