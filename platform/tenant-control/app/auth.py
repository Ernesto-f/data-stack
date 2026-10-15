"""Verify Keycloak access tokens forwarded by the trusted Portal Nginx."""

from __future__ import annotations

import os
from dataclasses import dataclass

import jwt
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jwt import PyJWKClient, PyJWKClientConnectionError, PyJWKClientError

bearer = HTTPBearer(auto_error=False)


@dataclass(frozen=True)
class Identity:
    subject: str
    authorized_party: str
    groups: tuple[str, ...]
    email: str | None = None
    email_verified: bool = False
    name: str | None = None


class TokenVerifier:
    def __init__(self, issuer: str, jwks_url: str, allowed_azp: set[str]) -> None:
        self.issuer = issuer.rstrip("/")
        self.allowed_azp = allowed_azp
        self.jwks = PyJWKClient(jwks_url, cache_jwk_set=True, lifespan=300, timeout=5)

    def verify(self, token: str) -> Identity:
        try:
            key = self.jwks.get_signing_key_from_jwt(token).key
            claims = jwt.decode(
                token,
                key,
                algorithms=["RS256"],
                issuer=self.issuer,
                options={"verify_aud": False, "require": ["sub", "iss", "exp", "azp"]},
                leeway=15,
            )
        except PyJWKClientConnectionError as error:
            raise HTTPException(503, "Identity provider unavailable") from error
        except (PyJWKClientError, jwt.PyJWTError, TypeError, ValueError) as error:
            raise HTTPException(
                401, "Invalid access token", headers={"WWW-Authenticate": "Bearer"}
            ) from error

        subject = claims.get("sub")
        azp = claims.get("azp")
        if not isinstance(subject, str) or not subject.strip() or azp not in self.allowed_azp:
            raise HTTPException(401, "Untrusted identity or client")
        raw_groups = claims.get("groups") or []
        groups = (raw_groups,) if isinstance(raw_groups, str) else tuple(
            value for value in raw_groups if isinstance(value, str)
        )
        return Identity(
            subject=subject,
            authorized_party=azp,
            groups=groups,
            email=claims.get("email"),
            email_verified=claims.get("email_verified") is True,
            name=claims.get("name") or claims.get("preferred_username"),
        )


async def current_identity(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer),
) -> Identity:
    if credentials is None or credentials.scheme.lower() != "bearer":
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            "Bearer token required",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return request.app.state.token_verifier.verify(credentials.credentials)


def build_verifier() -> TokenVerifier:
    issuer = os.getenv(
        "OIDC_ISSUER_URL", "https://portal.izatacore.com/keylogin/realms/pulse"
    )
    jwks = os.getenv(
        "OIDC_JWKS_URL",
        "http://keycloak:8080/keylogin/realms/pulse/protocol/openid-connect/certs",
    )
    allowed = {
        part.strip()
        for part in os.getenv("OIDC_ALLOWED_AZP", "pulse-oauth2-proxy").split(",")
        if part.strip()
    }
    return TokenVerifier(issuer, jwks, allowed)
