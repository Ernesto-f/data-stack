from odoo import models
from odoo.exceptions import AccessDenied


class AuthOauthProvider(models.Model):
    _inherit = "auth.oauth.provider"

    def _decode_id_token(self, access_token, id_token, kid):
        claims = super()._decode_id_token(access_token, id_token, kid)
        pulse_provider = self.env.ref(
            "pulse_crm_sso.provider_keycloak", raise_if_not_found=False
        )
        if pulse_provider and self.id == pulse_provider.id:
            expected_issuer = (
                "https://portal.izatacore.com/keylogin/realms/pulse"
            )
            if claims.get("iss") != expected_issuer:
                raise AccessDenied()
        return claims
