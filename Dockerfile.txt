# Dockerfile
FROM python:3.11-slim

ENV PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt ./
RUN pip install --upgrade pip && pip install -r requirements.txt

# copy the rest (assets/, report/, app.py, etc.)
COPY . .

# Spaces will set PORT. Default 7860 for local.
ENV PORT=7860
CMD ["bash", "-lc", "gunicorn app:app.server --bind 0.0.0.0:$PORT --workers 2 --timeout 120"]
