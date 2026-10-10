"""Validate Keycloak access tokens before resolving any invoice tenant."""

from __future__ import annotations

import os
from dataclasses import dataclass

import jwt
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jwt import PyJWKClient, PyJWKClientConnectionError, PyJWKClientError


bearer_scheme = HTTPBearer(auto_error=False)


@dataclass(frozen=True)
class Identity:
    subject: str
    authorized_party: str
    groups: tuple[str, ...]
    username: str | None = None
    email: str | None = None
    email_verified: bool = False


class OIDCTokenVerifier:
    def __init__(
        self,
        issuer: str,
        allowed_authorized_parties: set[str],
        jwks_url: str | None = None,
        jwks_client: PyJWKClient | None = None,
    ) -> None:
        self.issuer = issuer.rstrip("/")
        self.allowed_authorized_parties = allowed_authorized_parties
        self.jwks_client = jwks_client or PyJWKClient(
            jwks_url or f"{self.issuer}/protocol/openid-connect/certs",
            cache_jwk_set=True,
            lifespan=300,
            timeout=5,
        )

    def verify(self, token: str) -> Identity:
        try:
            signing_key = self.jwks_client.get_signing_key_from_jwt(token).key
            claims = jwt.decode(
                token,
                signing_key,
                algorithms=["RS256"],
                issuer=self.issuer,
                options={"verify_aud": False, "require": ["sub", "iss", "exp", "azp"]},
                leeway=15,
            )
        except PyJWKClientConnectionError as error:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Identity provider is unavailable",
            ) from error
        except (PyJWKClientError, jwt.PyJWTError, TypeError, ValueError) as error:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid access token",
                headers={"WWW-Authenticate": "Bearer"},
            ) from error

        subject = claims.get("sub")
        authorized_party = claims.get("azp")
        if not isinstance(subject, str) or not subject.strip():
            raise HTTPException(status_code=401, detail="Invalid access token subject")
        if authorized_party not in self.allowed_authorized_parties:
            raise HTTPException(status_code=401, detail="Token was issued to an untrusted client")

        claim_groups = claims.get("groups") or []
        groups = (claim_groups,) if isinstance(claim_groups, str) else tuple(
            group for group in claim_groups if isinstance(group, str)
        )
        return Identity(
            subject=subject,
            authorized_party=authorized_party,
            groups=groups,
            username=claims.get("preferred_username"),
            email=claims.get("email"),
            email_verified=claims.get("email_verified") is True,
        )


async def get_current_identity(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
) -> Identity:
    if credentials is None or credentials.scheme.lower() != "bearer":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Bearer token required",
            headers={"WWW-Authenticate": "Bearer"},
        )

    verifier: OIDCTokenVerifier = request.app.state.oidc_token_verifier
    return verifier.verify(credentials.credentials)


def build_token_verifier() -> OIDCTokenVerifier:
    issuer = os.getenv(
        "OIDC_ISSUER_URL",
        "https://portal.izatacore.com/keylogin/realms/pulse",
    ).rstrip("/")
    allowed = {
        value.strip()
        for value in os.getenv("OIDC_ALLOWED_AZP", "pulse-oauth2-proxy").split(",")
        if value.strip()
    }
    jwks_url = os.getenv("OIDC_JWKS_URL") or None
    return OIDCTokenVerifier(issuer, allowed, jwks_url)
