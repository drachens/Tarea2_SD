FROM python:3.9

COPY . /usr/src/app
COPY ../consumer/miembros.csv /usr/src/app/miembros.csv
COPY ../consumer/ventas.csv /usr/src/app/ventas.csv

WORKDIR /usr/src/app


RUN pip install -r requirements.txt


CMD tail -f /dev/null