# syntax=docker/dockerfile:1.7

# Exemplo para fixar a versão durante o build:
# docker build --build-arg N8N_VERSION=2.30.5 -t meu-n8n-ai:2.30.5 .
ARG N8N_VERSION=latest
ARG PYTHON_IMAGE=python:3.12-alpine

# A imagem oficial atual do n8n é mínima. Usamos um estágio separado
# para copiar o n8n para uma base Alpine extensível com Python e pip.
FROM docker.n8n.io/n8nio/n8n:${N8N_VERSION} AS n8n

FROM ${PYTHON_IMAGE} AS runtime

USER root

# Ferramentas de runtime úteis para automações.
# .build-deps existe apenas durante a instalação dos pacotes Python.
RUN apk add --no-cache \
        bash \
        ca-certificates \
        curl \
        ffmpeg \
        git \
        jq \
        openssh-client \
        tzdata \
        libffi \
        libstdc++ \
        openssl \
    && apk add --no-cache --virtual .build-deps \
        build-base \
        cargo \
        libffi-dev \
        linux-headers \
        openssl-dev \
        python3-dev

# Ambiente Python isolado para não alterar os pacotes do sistema.
ENV VIRTUAL_ENV=/opt/venv
ENV PATH="${VIRTUAL_ENV}/bin:/usr/local/bin:/usr/bin:/bin"
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PIP_NO_CACHE_DIR=1

RUN python -m venv "${VIRTUAL_ENV}" \
    && pip install --no-cache-dir --upgrade pip setuptools wheel

# Alterações no requirements.txt invalidam apenas esta parte do cache.
COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    && rm -f /tmp/requirements.txt \
    && apk del .build-deps \
    && rm -rf /root/.cache /tmp/*

# Copia o n8n oficial, Node.js, entrypoint e o usuário "node".
COPY --from=n8n / /

RUN mkdir -p /home/node/.n8n \
    && chown -R node:node /home/node "${VIRTUAL_ENV}"

WORKDIR /home/node
USER node

EXPOSE 5678

ENTRYPOINT ["tini", "--", "/docker-entrypoint.sh"]
