FROM python:3.11-slim
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    librdkafka-dev \
    libsnappy-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . . 
#copies everything from your faust-processor/ folder on your host machine into /app inside the container.

CMD ["faust", "-A", "app", "worker", "-l", "info"]