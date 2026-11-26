FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY run.py ./

# Primary and replica SQLite files live here; mount a volume to persist them.
RUN mkdir -p /app/data
VOLUME ["/app/data"]

ENV PRIMARY_DB=/app/data/primary.db \
    REPLICA_DB=/app/data/replica.db \
    HOST=0.0.0.0 \
    PORT=8000

EXPOSE 8000

CMD ["python", "run.py"]
