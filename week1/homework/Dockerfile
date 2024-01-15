FROM python:3.10.13

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2 pyarrow

WORKDIR /app
COPY get_data.py get_data.py

ENTRYPOINT ["python", "get_data.py"]