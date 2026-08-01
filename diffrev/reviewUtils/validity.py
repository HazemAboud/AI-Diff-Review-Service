import json
import re

from fastapi import status

from diffrev.shared.errors import raise_error

HUNK_HEADER_RE = re.compile(r"^@@ -\d+(?:,\d+)? \+\d+(?:,\d+)? @@", re.MULTILINE)


def validate_payload(raw: bytes, max_payload_bytes: int) -> dict:
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
