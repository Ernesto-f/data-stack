"""Local-only Docling conversion and reuse of the existing DocETL pipeline."""

from __future__ import annotations

import base64
import json
import os
import shutil
import uuid
from pathlib import Path
from typing import Any

import httpx
import yaml

from layouts import classify_nfse_layout


class LocalExtractionError(RuntimeError):
    pass


class LocalInvoiceExtractor:
    def __init__(
        self,
        docling_url: str,
        docetl_url: str,
        pipeline_path: str,
        shared_dir: str,
        docetl_container_dir: str,
    ) -> None:
        self.docling_url = docling_url.rstrip("/")
        self.docetl_url = docetl_url.rstrip("/")
        self.pipeline_path = Path(pipeline_path)
        self.shared_dir = Path(shared_dir)
        self.docetl_container_dir = docetl_container_dir.rstrip("/")

    async def extract(
        self,
        pdf_content: bytes,
        filename: str,
        source_document_id: int,
        source_sha256: str,
    ) -> dict[str, Any]:
        if not self.pipeline_path.is_file():
            raise LocalExtractionError("The local NFS-e pipeline is not installed")

        try:
            markdown = await self._convert_pdf(pdf_content, filename)
            layout_family = classify_nfse_layout(markdown)
            run_id = uuid.uuid4().hex
            run_dir = self.shared_dir / run_id
            run_dir.mkdir(parents=True, mode=0o700, exist_ok=False)
            try:
                doclet_run_dir = self.docetl_container_dir + "/" + run_id
                input_file = run_dir / "invoice_input.json"
                output_file = run_dir / "invoice_extraction.json"
                pipeline_file = run_dir / "pipeline.yaml"
                input_file.write_text(
                    json.dumps(
                        [{
                            "source_document_id": source_document_id,
                            "source_sha256": source_sha256,
                            "layout_family": layout_family,
                            "markdown": markdown,
                        }],
                        ensure_ascii=False,
                    ),
                    encoding="utf-8",
                )

                pipeline = yaml.safe_load(self.pipeline_path.read_text(encoding="utf-8"))
                pipeline["datasets"]["invoices"]["path"] = doclet_run_dir + "/invoice_input.json"
                pipeline["pipeline"]["output"]["path"] = doclet_run_dir + "/invoice_extraction.json"
                pipeline_file.write_text(yaml.safe_dump(pipeline, sort_keys=False, allow_unicode=True), encoding="utf-8")

                async with httpx.AsyncClient(timeout=180) as client:
                    response = await client.post(
                        self.docetl_url + "/run_pipeline",
                        json={"yaml_config": doclet_run_dir + "/pipeline.yaml"},
                    )
                    response.raise_for_status()
                if not output_file.is_file():
                    raise LocalExtractionError("DocETL completed without an output file")
                output = json.loads(output_file.read_text(encoding="utf-8"))
                if isinstance(output, list):
                    if len(output) != 1:
                        raise LocalExtractionError("Expected one normalized NFS-e output")
                    output = output[0]
                if not isinstance(output, dict) or not isinstance(output.get("values"), dict):
                    raise LocalExtractionError("DocETL returned an invalid normalized result")
                return output
            finally:
                shutil.rmtree(run_dir, ignore_errors=True)
        except LocalExtractionError:
            raise
        except (httpx.HTTPError, OSError, ValueError, KeyError, TypeError) as error:
            # Never include PDF text, OCR output, or provider responses in the API error.
            raise LocalExtractionError("Local Docling/DocETL extraction failed") from error

    async def _convert_pdf(self, content: bytes, filename: str) -> str:
        payload = {
            "options": {
                "to_formats": ["md"],
                "include_images": False,
                "ocr_mode": "never",
            },
            "file_sources": [{
                "base64_string": base64.b64encode(content).decode("ascii"),
                "filename": filename,
            }],
        }
        async with httpx.AsyncClient(timeout=180) as client:
            response = await client.post(
                self.docling_url + "/v1alpha/convert/source",
                json=payload,
            )
            response.raise_for_status()
        result = response.json()
        markdown = result.get("document", {}).get("md_content", "")
        if result.get("status") != "success" or not markdown.strip():
            raise LocalExtractionError("Local Docling did not produce text; scanned-PDF OCR is not enabled in this MVP")
        return markdown


def build_invoice_extractor() -> LocalInvoiceExtractor:
    return LocalInvoiceExtractor(
        docling_url=os.getenv("DOCLING_SERVER_URL", "http://docling-server:8090"),
        docetl_url=os.getenv("DOCETL_API_URL", "http://docetl-backend:8000"),
        pipeline_path=os.getenv(
            "INVOICE_PIPELINE_PATH",
            "/app/pipelines/nfse-local-extraction.yaml",
        ),
        shared_dir=os.getenv("DOCETL_SHARED_DIR", "/doclet-invoice"),
        docetl_container_dir=os.getenv(
            "DOCETL_CONTAINER_DIR",
            "/docetl-data/.docetl/pulse-invoice-api",
        ),
    )
