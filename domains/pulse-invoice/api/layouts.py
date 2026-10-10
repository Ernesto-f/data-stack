"""Conservative NFS-e layout-family classification for versioned ETL profiles."""

from __future__ import annotations

import re
import unicodedata


def _normalized(text: str) -> str:
    ascii_text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    return re.sub(r"[^A-Z0-9]+", " ", ascii_text.upper()).strip()


def classify_nfse_layout(markdown: str) -> str:
    """Return a known layout id, or an explicit unmapped/unknown sentinel.

    The classifier uses only stable document headings. It deliberately avoids
    inferring a profile from a supplier/customer name or a single tax value.
    """
    text = _normalized(markdown)

    if any(marker in text for marker in (
        "DANFSE",
        "NFS E NACIONAL",
        "DOCUMENTO AUXILIAR DA NOTA FISCAL DE SERVICO ELETRONICA NACIONAL",
    )):
        return "nfse_nacional_unmapped"

    if (
        "EMITENTE" in text
        and "DESTINATARIO" in text
        and "ISS" in text
        and "VALOR" in text
    ):
        return "nfse_nacional_candidate_v1"

    classic_headers = (
        "PRESTADOR DE SERVICOS" in text,
        "TOMADOR DE SERVICOS" in text,
        "NUMERO DA NOTA" in text or "VALOR TOTAL DO SERVICO" in text,
    )
    if sum(classic_headers) == len(classic_headers):
        return "municipal_classic_v1"

    return "unclassified"
