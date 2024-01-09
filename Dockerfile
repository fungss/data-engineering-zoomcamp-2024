FROM python:3.9
RUN apt-get update && apt-get install wget
RUN pip install pandas sqlalchemy psycopg2-binary
COPY ./app/ingest-to-postgres.py ./app/ingest-to-postgres.py
RUN mkdir -p /app/data
WORKDIR /app
ENTRYPOINT ["python", "ingest-to-postgres.py"]