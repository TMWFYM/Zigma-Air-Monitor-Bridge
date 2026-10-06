ARG BUILD_FROM=python:3.11-alpine
FROM ${BUILD_FROM}

WORKDIR /app

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY zigma_bridge.py /app/
COPY run.sh /app/
RUN chmod +x /app/run.sh

ENV PYTHONUNBUFFERED=1

CMD ["/app/run.sh"]
