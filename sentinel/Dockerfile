FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# DB lives on a mounted volume — override with DB_PATH env var
ENV DB_PATH=/data/sentinel.db

RUN useradd -m sentinel && mkdir -p /data && chown sentinel /data
USER sentinel

EXPOSE 8000

CMD ["python", "main.py"]
