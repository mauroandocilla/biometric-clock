# Dockerfile
FROM python:3.12-slim

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Dependencias del sistema (mdbtools para MDB)
RUN apt-get update \
 && apt-get install -y --no-install-recommends mdbtools \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
EXPOSE 8000
# Dockerfile (línea CMD)
CMD ["bash","-lc","gunicorn --worker-class gthread --workers 1 --threads 4 --timeout 600 --graceful-timeout 30 --keep-alive 5 --access-logfile - --error-logfile - --log-level info -b 0.0.0.0:${PORT:-8000} wsgi:app"]