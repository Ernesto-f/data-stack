# Imagem Python simples.
FROM python:3.12-slim


# Tudo que fizermos a seguir acontece em /app.
WORKDIR /app


# Primeiro copia somente requirements.
COPY requirements.txt /app/requirements.txt


# Instala as bibliotecas Python.
RUN python -m pip install \
    --no-cache-dir \
    -r /app/requirements.txt


# Copia o projeto.
COPY . /app