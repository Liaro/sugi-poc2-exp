# TODO: slimに変更する
FROM python:3.9.2 as builder

WORKDIR /root

COPY ./poetry.lock ./pyproject.toml /root/

RUN pip install poetry
RUN poetry export --without-hashes -f requirements.txt > requirements.txt

FROM python:3.9.2

WORKDIR /root

COPY --from=builder /root/requirements.txt .

RUN pip install -U pip
RUN pip install -r requirements.txt

COPY ./ /root/