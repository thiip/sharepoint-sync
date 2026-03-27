FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir requests openpyxl
COPY sync.py entrypoint.sh ./
RUN chmod +x entrypoint.sh
EXPOSE 8080
CMD ["/app/entrypoint.sh"]
