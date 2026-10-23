FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
# Bake the seeded dataset + streaming checkpoints into the image so the
# dashboard is fully self-contained (deterministic: same seed, same data).
RUN python data/make_demo.py && \
    PYTHONPATH=src python -m clickstream.stream \
        --topic data/clickstream_30min.jsonl --out out --replay-speed 0

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
