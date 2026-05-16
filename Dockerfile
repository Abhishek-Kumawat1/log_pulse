FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Generate self-signed SSL cert if not present
RUN apt-get update && apt-get install -y --no-install-recommends openssl && \
    rm -rf /var/lib/apt/lists/* && \
    openssl req -x509 -newkey rsa:2048 \
      -keyout server.key -out server.crt \
      -days 365 -nodes -subj "/CN=localhost"

EXPOSE 5000 8080
