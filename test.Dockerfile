FROM python:3.9

RUN pip install poetry
RUN pip install pytest

COPY . /app

WORKDIR /app

RUN poetry config virtualenvs.create false
RUN poetry install --only main
# RUN pip install -r requiremets.txt

ENV PYTHONPATH=${PYTHONPATH}:${PWD} 

CMD pytest ./tests