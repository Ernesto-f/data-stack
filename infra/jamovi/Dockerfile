# syntax=docker/dockerfile:1

FROM jamovi/jamovi:28.1

ENV JAMOVI_HOME=/usr/lib/jamovi
ENV PYTHONPATH=/usr/lib/jamovi/server
ENV JAMOVI_SESSION_EXPIRES=0
ENV JAMOVI_ALLOW_ARBITRARY_CODE=false

# Sem senha por padrão.
# Para produção, recomendo definir uma chave no docker run/compose.
ENV JAMOVI_ACCESS_KEY=""

EXPOSE 41337 41338 41339

ENTRYPOINT ["/bin/sh", "-c"]

CMD ["/usr/bin/python3 -u -m jamovi.server 41337 --if=*"]