import json
import re

from fastapi import status

from diffrev.shared.errors import raise_error

HUNK_HEADER_RE = re.compile(r"^@@ -\d+(?:,\d+)? \+\d+(?:,\d+)? @@", re.MULTILINE)

PROVIDER_CHOICES = ("mock", "llm")
# The jobs.max_findings column is INT UNSIGNED; anything beyond that cannot be
# stored. This is a storage limit, not a business cap on findings.
MAX_FINDINGS_COLUMN_LIMIT = 4294967295


def validate_payload(raw, max_payload_bytes):
    if len(raw) > max_payload_bytes:
        raise_error(status.HTTP_413_CONTENT_TOO_LARGE, "payload_too_large",
                    f"Payload exceeds {max_payload_bytes} bytes")

    try:
        data = json.loads(raw)
    except ValueError:
        raise_error(status.HTTP_400_BAD_REQUEST, "invalid_json", "Request body is not valid JSON")

    diff = data.get("diff") if isinstance(data, dict) else None
    if not isinstance(diff, str) or not diff.strip():
        raise_error(status.HTTP_422_UNPROCESSABLE_CONTENT, "invalid_diff",
                    "diff must be a non-empty unified diff string")
    if not HUNK_HEADER_RE.search(diff):
        raise_error(status.HTTP_422_UNPROCESSABLE_CONTENT, "invalid_diff",
                    "diff is not parseable as a unified diff")

    return data


def validate_options(options, default_max_findings=100):
    """Return a safe (provider, max_findings) pair, rejecting bad values with
    422 so a malformed options object can never crash the submission."""
    provider = "mock"
    max_findings = default_max_findings
    if not isinstance(options, dict):
        return provider, max_findings

    requested_provider = options.get("provider")
    if requested_provider is not None:
        if requested_provider not in PROVIDER_CHOICES:
            raise_error(status.HTTP_422_UNPROCESSABLE_CONTENT, "invalid_diff",
                        "options.provider must be 'mock' or 'llm'")
        provider = requested_provider

    requested = options.get("maxFindings")
    if requested is not None:
        if isinstance(requested, bool) or not isinstance(requested, int):
            raise_error(status.HTTP_422_UNPROCESSABLE_CONTENT, "invalid_diff",
                        "options.maxFindings must be a non-negative integer")
        if not 0 <= requested <= MAX_FINDINGS_COLUMN_LIMIT:
            raise_error(status.HTTP_422_UNPROCESSABLE_CONTENT, "invalid_diff",
                        f"options.maxFindings must be between 0 and {MAX_FINDINGS_COLUMN_LIMIT}")
        max_findings = requested

    return provider, max_findings
