FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends wget && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Download databases from Dropbox at build time
RUN mkdir -p data && \
    wget -q -O data/b3.db \
    "https://www.dropbox.com/scl/fi/h9p6dkp2wy91bmpa91d8u/b3.db?rlkey=ec23sb9j2mkmyqnwez1me5p48&st=wuqbnp72&dl=1" && \
    wget -q -O data/dividendos.db \
    "https://www.dropbox.com/scl/fi/1je6v5ewqk5fjubr3m0vy/dividendos.db?rlkey=pfbhmudlo4vy8kaasl73byxah&dl=1"

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
