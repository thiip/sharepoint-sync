FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir requests openpyxl
COPY sync.py .
EXPOSE 8080
CMD ["python", "-u", "sync.py"]
