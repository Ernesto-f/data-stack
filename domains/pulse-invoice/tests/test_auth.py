from __future__ import annotations

import time
from types import SimpleNamespace

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import HTTPException

from auth import OIDCTokenVerifier


ISSUER = "https://idp.example.invalid/realms/pulse"
ALLOWED_AZP = {"pulse-oauth2-proxy"}


class StaticJWKSClient:
    def __init__(self, public_key):
        self.public_key = public_key

    def get_signing_key_from_jwt(self, token):
        return SimpleNamespace(key=self.public_key)


@pytest.fixture
def keypair():
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return private_key, private_key.public_key()


def token(private_key, **overrides):
    claims = {
        "iss": ISSUER,
        "sub": "synthetic-sub-a",
        "azp": "pulse-oauth2-proxy",
        "iat": int(time.time()),
        "exp": int(time.time()) + 120,
        "groups": ["/pulse/home"],
    }
    claims.update(overrides)
    return jwt.encode(claims, private_key, algorithm="RS256", headers={"kid": "test-key"})


def verifier(public_key):
    return OIDCTokenVerifier(
        issuer=ISSUER,
        allowed_authorized_parties=ALLOWED_AZP,
        jwks_client=StaticJWKSClient(public_key),
    )


def test_verifies_signature_issuer_client_and_subject(keypair):
    private_key, public_key = keypair
    identity = verifier(public_key).verify(token(private_key))

    assert identity.subject == "synthetic-sub-a"
    assert identity.authorized_party == "pulse-oauth2-proxy"
    assert identity.groups == ("/pulse/home",)


def test_rejects_token_from_another_client(keypair):
    private_key, public_key = keypair
    with pytest.raises(HTTPException) as error:
        verifier(public_key).verify(token(private_key, azp="untrusted-client"))
    assert error.value.status_code == 401


def test_rejects_expired_token(keypair):
    private_key, public_key = keypair
    with pytest.raises(HTTPException) as error:
        verifier(public_key).verify(token(private_key, exp=int(time.time()) - 60))
    assert error.value.status_code == 401


def test_rejects_token_signed_by_an_unknown_key(keypair):
    _, expected_public_key = keypair
    other_private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    with pytest.raises(HTTPException) as error:
        verifier(expected_public_key).verify(token(other_private_key))
    assert error.value.status_code == 401
