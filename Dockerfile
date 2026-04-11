FROM python:3.11-slim

RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p data && wget -q -O data/b3.db 'https://www.dropbox.com/scl/fi/h9p6dkp2wy91bmpa91d8u/b3.db?rlkey=ec23sb9j2mkmyqnwez1me5p48&st=wuqbnp72&dl=1'

EXPOSE 8000

CMD uvicorn main:app --host 0.0.0.0 --port 8000
