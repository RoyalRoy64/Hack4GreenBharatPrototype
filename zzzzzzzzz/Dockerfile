# Dockerfile
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

# install system deps needed for pymupdf and optional gemini client
RUN apt-get update && apt-get install -y build-essential cmake libgl1 libglib2.0-0 wget && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip
RUN pip install -r /app/requirements.txt

COPY . /app

ENV GEMINI_API_KEY=""

EXPOSE 8000

CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8000"]