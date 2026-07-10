FROM python:3.12-slim
WORKDIR /app

ENV PYTHONPATH=/app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY inciweb_pipeline/ inciweb_pipeline/

ENTRYPOINT ["python", "-m", "inciweb_pipeline"]
