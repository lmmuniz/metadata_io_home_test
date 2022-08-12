# syntax=docker/dockerfile:1
FROM python:3.7.6-buster
WORKDIR /code
#ENV API_KEY=550959973c2f88d5e307e26e90f233c7

COPY requirements.txt requirements.txt

RUN pip3 install --upgrade pip

RUN pip3 install -r requirements.txt

COPY . .

CMD ["python", "main.py"]