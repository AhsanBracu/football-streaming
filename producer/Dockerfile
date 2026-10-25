# Small Python image with the two libraries the producer needs.
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY replay.py .
# -u: print progress immediately instead of buffering it
ENTRYPOINT ["python", "-u", "replay.py"]
