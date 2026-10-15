import asyncio

import pytest
from app.keycloak import KeycloakAdmin, KeycloakError


class GroupSyncAdmin(KeycloakAdmin):
    def __init__(self):
        self.groups = {
            "/pulse/tenants/acme/obsolete-a",
            "/pulse/tenants/acme/obsolete-b",
        }
        self.fail_on_remove = "/pulse/tenants/acme/obsolete-b"

    async def groups_for_user(self, _user_id):
        return set(self.groups)

    async def add_user_to_group(self, _user_id, group_path):
        self.groups.add(group_path)
        return True

    async def remove_user_from_group(self, _user_id, group_path):
        if group_path == self.fail_on_remove:
            raise KeycloakError("group service unavailable")
        self.groups.discard(group_path)


def test_set_groups_restores_removed_groups_when_a_later_add_fails():
    admin = GroupSyncAdmin()

    async def run_sync():
        with pytest.raises(KeycloakError):
            await admin.set_groups(
                "member-1",
                set(),
                {
                    "/pulse/tenants/acme/obsolete-a",
                    "/pulse/tenants/acme/obsolete-b",
                },
            )

    asyncio.run(run_sync())
    assert admin.groups == {
        "/pulse/tenants/acme/obsolete-a",
        "/pulse/tenants/acme/obsolete-b",
    }
