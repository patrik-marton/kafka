FROM docker-private.infra.cloudera.com/cloudera_thirdparty/python:3.12-slim

RUN pip install --target=/app api4jenkins==2.0.3 requests==2.32.3 PyGithub --index-url https://pypi.infra.cloudera.com/api/pypi/cloudera/simple

COPY . /app
WORKDIR /app

CMD ["python", "/app/main.py"]