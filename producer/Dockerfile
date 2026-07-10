#tells docker which base image to use
FROM python:3.12-slim 

WORKDIR /app
# Install system deps needed by confluent-kafka (librdkafka)
RUN apt-get update && apt-get install  -y --no-install-recommends \
    gcc \
    librdkafka-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*
#After installation, Docker removes cached package lists.

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

#This copies your entire project into the container.
COPY . .

# --reload lets you edit main.py on your host and see changes instantly
#uvicorn is an ASGI web server that runs Python web applications such as FastAPI.
#--reload automatically restarts whenever there is change in the code files
#main:app  runs 'app' present inside main.py
#starts your FastAPI application using the Uvicorn web server
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]