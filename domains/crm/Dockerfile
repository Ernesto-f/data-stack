FROM odoo:19.0

USER root

ARG OCA_SERVER_AUTH_COMMIT=be44314fac3efd784051466e2fa425cdff03c1fd

# hadolint ignore=DL3008
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && git clone --filter=blob:none --no-checkout https://github.com/OCA/server-auth.git /tmp/server-auth \
    && git -C /tmp/server-auth checkout "${OCA_SERVER_AUTH_COMMIT}" -- auth_oidc \
    && mkdir -p /mnt/extra-addons \
    && cp -a /tmp/server-auth/auth_oidc /mnt/extra-addons/auth_oidc \
    && cp /tmp/server-auth/LICENSE /mnt/extra-addons/OCA-server-auth-LICENSE \
    && rm -rf /tmp/server-auth \
    && apt-get purge -y --auto-remove git \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir "python-jose[cryptography]==3.5.0"

COPY addons/pulse_crm_sso /mnt/extra-addons/pulse_crm_sso

USER odoo
