# =======================================================================
# EDF Pipeline web app — Flask dashboard (trigger DAG, progress, logs,
# topology, infra/storage health, Grafana embeds).
# =======================================================================

FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV FLASK_HOST=0.0.0.0 \
    PYTHONUNBUFFERED=1

EXPOSE 5000

CMD ["python", "app.py"]
