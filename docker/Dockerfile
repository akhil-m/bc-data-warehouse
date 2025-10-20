FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ ./src/

# Copy and setup entrypoint
COPY docker/run_statscan.sh ./run_statscan.sh
RUN chmod +x run_statscan.sh

CMD ["./run_statscan.sh"]
