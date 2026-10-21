FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY operator.py .

RUN useradd -m widget-op
USER widget-op

CMD ["kopf", "run", "operator.py", "--namespace", "demo-operator-ns", "--liveness=http://0.0.0.0:8080/healthz"]
