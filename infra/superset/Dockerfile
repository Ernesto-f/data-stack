FROM apache/superset:6.1.0

# (Opcional) Instalar só o driver do Postgres.
# A imagem já traz suporte, mas manter explícito ajuda.
USER root
RUN pip install --no-cache-dir psycopg2-binary
USER superset