# syntax=docker/dockerfile:1

FROM python:3.9.2

WORKDIR /app

COPY Pipfile Pipfile
RUN pip install pipenv
RUN pipenv --python 3.9.2
COPY Pipfile* /tmp
RUN cd /tmp && pipenv lock --keep-outdated --requirements > requirements.txt
RUN pip install -r /tmp/requirements.txt
COPY . / tmp/twitter-positivity-stream

EXPOSE 5000

COPY . .

CMD ["luigi_datamodel.py"]
ENTRYPOINT ["python3"]
