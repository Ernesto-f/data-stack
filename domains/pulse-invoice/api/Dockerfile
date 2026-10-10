FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY api/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY api/*.py /app/
RUN mkdir -p /app/pipelines
COPY docetl/pipelines/nfse-local-extraction.yaml /app/pipelines/nfse-local-extraction.yaml

EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
