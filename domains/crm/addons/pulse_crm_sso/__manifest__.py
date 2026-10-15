{
    "name": "Pulse CRM SSO",
    "summary": "Restrict CRM OpenID login to provisioned Pulse users",
    "version": "18.0.1.0.0",
    "license": "AGPL-3",
    "depends": ["auth_oidc", "crm"],
    "data": ["data/oauth_provider.xml"],
    "post_init_hook": "post_init_hook",
    "installable": True,
    "application": False,
}
