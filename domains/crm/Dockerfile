FROM odoo:18

USER root

ARG OCA_SERVER_AUTH_COMMIT=b75e5653e8116daf7fb03e73fe84190e14e03e23

# hadolint ignore=DL3008
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && git clone --filter=blob:none --no-checkout https://github.com/OCA/server-auth.git /tmp/server-auth \
    && git -C /tmp/server-auth checkout "${OCA_SERVER_AUTH_COMMIT}" -- auth_oidc LICENSE \
    && mkdir -p /mnt/extra-addons \
    && cp -a /tmp/server-auth/auth_oidc /mnt/extra-addons/auth_oidc \
    && cp /tmp/server-auth/LICENSE /mnt/extra-addons/OCA-server-auth-LICENSE \
    && rm -rf /tmp/server-auth \
    && apt-get purge -y --auto-remove git \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir --break-system-packages "python-jose[cryptography]==3.5.0"

# The SALUS ERP source is maintained as a separate checkout under salus-demo.
# The tenant-specific SSO addon remains part of this repository.
COPY salus-demo/salus-erp/addons /mnt/extra-addons
COPY salus-demo/salus-erp/config/odoo.image.conf /etc/odoo/odoo.conf
COPY crm/addons/pulse_crm_sso /mnt/extra-addons/pulse_crm_sso

USER odoo
