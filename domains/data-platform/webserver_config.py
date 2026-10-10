"""Airflow FAB auth manager configuration for the shared Pulse Keycloak realm."""

from __future__ import annotations

import os

from airflow.providers.fab.auth_manager.security_manager.override import (
    FabAirflowSecurityManagerOverride,
)
from flask_appbuilder.const import AUTH_OAUTH


OIDC_ISSUER = "https://portal.izatacore.com/keylogin/realms/pulse"
OIDC_API = OIDC_ISSUER + "/protocol/openid-connect"

AUTH_TYPE = AUTH_OAUTH
AUTH_USER_REGISTRATION = True
AUTH_USER_REGISTRATION_ROLE = "Viewer"
AUTH_ROLES_SYNC_AT_LOGIN = True
AUTH_ROLES_MAPPING = {
    "pulse_airflow_admin": ["Admin"],
    "pulse_airflow_user": ["User"],
    "pulse_airflow_viewer": ["Viewer"],
}

OAUTH_PROVIDERS = [
    {
        "name": "keycloak",
        "icon": "fa-key",
        "token_key": "access_token",
        "remote_app": {
            "client_id": os.environ["AIRFLOW_OIDC_CLIENT_ID"],
            "client_secret": os.environ["AIRFLOW_OIDC_CLIENT_SECRET"],
            "server_metadata_url": OIDC_ISSUER + "/.well-known/openid-configuration",
            "api_base_url": OIDC_API + "/",
            "request_token_url": None,
            "access_token_url": OIDC_API + "/token",
            "authorize_url": OIDC_API + "/auth",
            "client_kwargs": {"scope": "openid profile email groups"},
        },
    }
]


class PulseSecurityManager(FabAirflowSecurityManagerOverride):
    def get_oauth_user_info(self, provider, response):
        if provider != "keycloak":
            return super().get_oauth_user_info(provider, response)

        remote = self.appbuilder.sm.oauth_remotes[provider]
        user_info = remote.get("userinfo").json()
        groups = user_info.get("groups") or []
        if isinstance(groups, str):
            groups = [groups]

        if "/pulse/admin" in groups or "/pulse/home/airflow-admin" in groups:
            role_keys = ["pulse_airflow_admin"]
        elif "/pulse/home/airflow" in groups:
            role_keys = ["pulse_airflow_user"]
        else:
            role_keys = ["pulse_airflow_viewer"]

        return {
            "username": user_info.get("preferred_username") or user_info.get("sub"),
            "email": user_info.get("email"),
            "first_name": user_info.get("given_name") or user_info.get("name") or "",
            "last_name": user_info.get("family_name") or "",
            "role_keys": role_keys,
        }


SECURITY_MANAGER_CLASS = PulseSecurityManager
