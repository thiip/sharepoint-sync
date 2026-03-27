FROM python:3.12-slim
ARG CACHE_BUST=2026-03-27-v4
LABEL build.date="2026-03-27" build.version="3"
WORKDIR /app
RUN pip install --no-cache-dir requests openpyxl
COPY sync.py .
EXPOSE 8080
CMD ["python", "-u", "sync.py"]
