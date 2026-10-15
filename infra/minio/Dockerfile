FROM minio/minio:RELEASE.2025-09-07T16-13-09Z

USER root

ARG MC_VERSION=RELEASE.2025-08-13T08-35-41Z

RUN microdnf install -y curl || true

RUN curl -fsSL https://dl.min.io/client/mc/release/linux-amd64/archive/mc.${MC_VERSION} \
    -o /usr/local/bin/mc \
    && chmod +x /usr/local/bin/mc \
    && mc --version

# Copia scripts para os paths que o entrypoint usa
COPY entrypoint.sh /entrypoint.sh
COPY create-bucket.sh /usr/local/bin/create-bucket.sh

RUN chmod +x /entrypoint.sh /usr/local/bin/create-bucket.sh

WORKDIR /data

EXPOSE 9000 9001

ENTRYPOINT ["/entrypoint.sh"]
