FROM quay.io/keycloak/keycloak:26.7.0

LABEL org.opencontainers.image.title="Keycloak"
LABEL org.opencontainers.image.description="Keycloak 26 com PostgreSQL para ambiente local"
LABEL org.opencontainers.image.version="26.7.0"

WORKDIR /opt/keycloak

EXPOSE 8080

ENTRYPOINT ["/opt/keycloak/bin/kc.sh"]

CMD ["start-dev"]