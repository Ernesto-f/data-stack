import os
import re

from odoo.exceptions import UserError


TENANT_KEY_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]{1,29}$")


def post_init_hook(env):
    tenant_key = os.environ.get("CRM_TENANT_KEY", "")
    client_id = os.environ.get("CRM_OIDC_CLIENT_ID", "")
    client_secret = os.environ.get("CRM_OIDC_CLIENT_SECRET", "")
    if not TENANT_KEY_PATTERN.fullmatch(tenant_key):
        raise UserError("CRM_TENANT_KEY must be a valid lowercase tenant slug.")
    if client_id != f"pulse-crm-{tenant_key}" or not client_secret:
        raise UserError("CRM OIDC client settings do not match this tenant.")

    provider = env.ref("pulse_crm_sso.provider_keycloak")
    provider.write(
        {
            "client_id": client_id,
            "client_secret": client_secret,
            "name": f"Pulse SSO ({tenant_key})",
            "enabled": True,
        }
    )
    env["ir.config_parameter"].sudo().set_param("pulse_crm.tenant_key", tenant_key)
