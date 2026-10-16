FROM maven:3.9.9-eclipse-temurin-21 AS action-token-provider-build

WORKDIR /build
COPY action-token-provider/pom.xml action-token-provider/pom.xml
COPY action-token-provider/src action-token-provider/src
RUN mvn -q -f action-token-provider/pom.xml package -DskipTests

FROM quay.io/keycloak/keycloak:26.7.0

LABEL org.opencontainers.image.title="Keycloak"
LABEL org.opencontainers.image.description="Keycloak 26 com PostgreSQL para ambiente local"
LABEL org.opencontainers.image.version="26.7.0"

WORKDIR /opt/keycloak

COPY --from=action-token-provider-build /build/action-token-provider/target/pulse-action-token-provider.jar /opt/keycloak/providers/

EXPOSE 8080

ENTRYPOINT ["/opt/keycloak/bin/kc.sh"]

CMD ["start-dev"]
