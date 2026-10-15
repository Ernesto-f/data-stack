"""Server-side Keycloak Admin API operations for tenant memberships."""

from __future__ import annotations

import os
import time

import httpx


class KeycloakError(RuntimeError):
    pass


class KeycloakGroupMissingError(KeycloakError):
    """The requested group path does not exist in the realm."""

    pass


class KeycloakAdmin:
    def __init__(self) -> None:
        self.base = os.getenv("KEYCLOAK_ADMIN_BASE", "http://keycloak:8080/keylogin").rstrip("/")
        self.client_id = os.environ["TENANT_CONTROL_CLIENT_ID"]
        self.client_secret = os.environ["TENANT_CONTROL_CLIENT_SECRET"]
        self.client = httpx.AsyncClient(timeout=12.0)
        self.token: str | None = None
        self.expires_at = 0.0

    async def close(self) -> None:
        await self.client.aclose()

    async def _admin_token(self) -> str:
        if self.token and time.monotonic() < self.expires_at:
            return self.token
        try:
            response = await self.client.post(
                f"{self.base}/realms/pulse/protocol/openid-connect/token",
                data={
                    "grant_type": "client_credentials",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                },
            )
        except httpx.HTTPError as error:
            raise KeycloakError("Keycloak service-account authentication unavailable") from error
        if response.status_code != 200:
            raise KeycloakError("Keycloak service-account authentication failed")
        payload = response.json()
        self.token = payload["access_token"]
        self.expires_at = time.monotonic() + max(30, int(payload.get("expires_in", 60)) - 15)
        return self.token

    async def request(self, method: str, path: str, *, json=None, params=None) -> httpx.Response:
        token = await self._admin_token()
        try:
            response = await self.client.request(
                method,
                f"{self.base}/admin/realms/pulse/{path.lstrip('/')}",
                headers={"Authorization": f"Bearer {token}"},
                json=json,
                params=params,
            )
        except httpx.HTTPError as error:
            raise KeycloakError("Keycloak Admin API unavailable") from error
        if response.status_code >= 400:
            raise KeycloakError(f"Keycloak operation failed ({response.status_code})")
        return response

    async def user_by_email(self, email: str) -> dict | None:
        response = await self.request(
            "GET", "users", params={"email": email, "exact": "true"}
        )
        users = response.json()
        if len(users) > 1:
            raise KeycloakError("Multiple Keycloak identities match this email")
        return users[0] if users else None

    async def create_user(self, email: str, display_name: str) -> dict:
        first, _, last = display_name.strip().partition(" ")
        response = await self.request(
            "POST",
            "users",
            json={
                "username": email,
                "email": email,
                "emailVerified": False,
                "enabled": True,
                "firstName": first or email,
                "lastName": last,
                "requiredActions": ["VERIFY_EMAIL", "UPDATE_PASSWORD"],
            },
        )
        location = response.headers.get("Location", "")
        user_id = location.rsplit("/", 1)[-1] if location else ""
        if not user_id:
            user = await self.user_by_email(email)
            user_id = (user or {}).get("id", "")
        if not user_id:
            raise KeycloakError("Keycloak created an identity but returned no ID")
        return {"id": user_id, "email": email, "emailVerified": False}

    async def group_id(self, group_path: str) -> str:
        parent_id = None
        for name in group_path.strip("/").split("/"):
            path = f"groups/{parent_id}/children" if parent_id else "groups"
            response = await self.request(
                "GET", path, params={"first": 0, "max": 1000, "briefRepresentation": "false"}
            )
            matches = [item for item in response.json() if item.get("name") == name]
            if len(matches) != 1:
                raise KeycloakGroupMissingError(f"Required Keycloak group is missing: {group_path}")
            parent_id = matches[0]["id"]
        if not parent_id:
            raise KeycloakError("Invalid empty group path")
        return parent_id

    async def groups_for_user(self, user_id: str) -> set[str]:
        response = await self.request(
            "GET", f"users/{user_id}/groups", params={"first": 0, "max": 1000}
        )
        return {item.get("path", "") for item in response.json() if item.get("path")}

    async def add_user_to_group(self, user_id: str, group_path: str) -> bool:
        if group_path in await self.groups_for_user(user_id):
            return False
        group_id = await self.group_id(group_path)
        await self.request("PUT", f"users/{user_id}/groups/{group_id}")
        return True

    async def remove_user_from_group(self, user_id: str, group_path: str) -> None:
        if group_path not in await self.groups_for_user(user_id):
            return
        group_id = await self.group_id(group_path)
        await self.request("DELETE", f"users/{user_id}/groups/{group_id}")

    async def set_groups(self, user_id: str, desired: set[str], managed: set[str]) -> list[str]:
        existing = await self.groups_for_user(user_id)
        added: list[str] = []
        try:
            for path in sorted(desired - existing):
                if await self.add_user_to_group(user_id, path):
                    added.append(path)
            for path in sorted((existing & managed) - desired):
                await self.remove_user_from_group(user_id, path)
        except KeycloakError:
            for group_path in added:
                try:
                    await self.remove_user_from_group(user_id, group_path)
                except KeycloakError:
                    continue
            raise
        return added

    async def rollback_user_groups(
        self, user_id: str | None, added_groups: list[str], delete_user: bool
    ) -> None:
        if not user_id:
            return
        if delete_user:
            await self.delete_user(user_id)
            return
        for group_path in added_groups:
            try:
                await self.remove_user_from_group(user_id, group_path)
            except KeycloakError:
                continue

    async def send_invitation(self, user_id: str) -> None:
        await self.request(
            "PUT",
            f"users/{user_id}/execute-actions-email",
            params={"lifespan": "86400"},
            json=["VERIFY_EMAIL", "UPDATE_PASSWORD"],
        )

    async def delete_user(self, user_id: str) -> None:
        await self.request("DELETE", f"users/{user_id}")
