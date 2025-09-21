FROM python:3.12-slim
WORKDIR /app

COPY requirements.txt .
COPY consumer.py producer.py app.py .
RUN pip install --no-cache-dir -r requirements.txt
# Default command (you can override this in docker-compose)
CMD ["python3.12", "producer.py"]
