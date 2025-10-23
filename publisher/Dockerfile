FROM python:3.11-slim

WORKDIR /app

RUN adduser --disabled-password --gecos '' publisher && \
    chown -R publisher:publisher /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY publisher.py ./

USER publisher

CMD ["python", "publisher.py"]