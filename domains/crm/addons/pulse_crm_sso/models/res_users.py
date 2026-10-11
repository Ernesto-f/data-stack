from odoo import api, models
from odoo.exceptions import AccessDenied


class ResUsers(models.Model):
    _inherit = "res.users"

    @api.model
    def _auth_oauth_signin(self, provider, validation, params):
        pulse_provider = self.env.ref(
            "pulse_crm_sso.provider_keycloak", raise_if_not_found=False
        )
        if not pulse_provider or provider != pulse_provider.id:
            return super()._auth_oauth_signin(provider, validation, params)

        subject = validation.get("sub") or validation.get("user_id")
        email = validation.get("email")
        groups = validation.get("groups") or []
        tenant_key = self.env["ir.config_parameter"].sudo().get_param(
            "pulse_crm.tenant_key"
        )
        tenant_group = f"/pulse/tenants/{tenant_key}/crm" if tenant_key else None
        if (
            not subject
            or not email
            or validation.get("email_verified") is not True
            or not isinstance(groups, list)
            or "/pulse/home/crm" not in groups
            or tenant_group not in groups
        ):
            raise AccessDenied()

        users = self.sudo().with_context(active_test=False)
        linked = users.search(
            [("oauth_provider_id", "=", provider), ("oauth_uid", "=", subject)],
            limit=2,
        )
        if len(linked) > 1:
            raise AccessDenied()
        if linked:
            user = linked
            if (
                not user.active
                or user.share
                or (user.email or "").strip().casefold() != email.strip().casefold()
            ):
                raise AccessDenied()
            user.write({"oauth_access_token": params.get("access_token")})
            self._sync_crm_role(user, tenant_key, groups)
            return user.login

        candidates = users.search(
            [
                ("email", "=ilike", email.strip()),
                ("active", "=", True),
                ("share", "=", False),
            ],
            limit=2,
        )
        if len(candidates) > 1:
            raise AccessDenied()
        role_group = self._crm_role_group(tenant_key, groups)
        if candidates:
            user = candidates
            if user.oauth_provider_id or user.oauth_uid:
                raise AccessDenied()
            user.write(
                {
                    "oauth_provider_id": provider,
                    "oauth_uid": subject,
                    "oauth_access_token": params.get("access_token"),
                }
            )
        else:
            name = validation.get("name") or validation.get("preferred_username") or email
            user = users.with_context(no_reset_password=True).create(
                {
                    "name": name,
                    "login": email,
                    "email": email,
                    "active": True,
                    "oauth_provider_id": provider,
                    "oauth_uid": subject,
                    "oauth_access_token": params.get("access_token"),
                    "groups_id": [(4, role_group.id)],
                }
            )
        self._sync_crm_role(user, tenant_key, groups)
        return user.login

    def _crm_role_group(self, tenant_key, groups):
        role_xmlid = (
            "sales_team.group_sale_manager"
            if f"/pulse/tenants/{tenant_key}/crm-admin" in groups
            else "sales_team.group_sale_salesman"
        )
        return self.env.ref(role_xmlid)

    def _sync_crm_role(self, user, tenant_key, groups):
        manager = self.env.ref("sales_team.group_sale_manager")
        all_leads = self.env.ref("sales_team.group_sale_salesman_all_leads")
        salesperson = self.env.ref("sales_team.group_sale_salesman")
        desired = self._crm_role_group(tenant_key, groups)
        commands = [(3, group.id) for group in (manager, all_leads, salesperson)]
        commands.append((4, desired.id))
        user.write({"groups_id": commands})
