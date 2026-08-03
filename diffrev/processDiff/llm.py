import asyncio
import json
import os
import urllib.error
import urllib.request

from diffrev.processDiff.base import BaseProvider, ProviderError

GEMINI_BASE_URL = "https://generativelanguage.googleapis.com/v1beta"
DEFAULT_MODEL = "gemini-flash-latest"

ALLOWED_SEVERITY = ("critical", "high", "medium", "low")
ALLOWED_CATEGORY = ("security", "correctness", "performance", "style")

PROMPT_PREFIX = (
    "You are reviewing a unified diff of source code. "
    "Analyze ONLY the added lines (lines prefixed with +) and report real defects. "
    "Respond with a JSON array of findings. Each finding must be an object "
    "with exactly these fields:\n"
    '- "ruleId": a stable short identifier such as "LLM-001"\n'
    '- "path": the file path from the diff\n'
    '- "line": the line number in the new file\n'
    '- "severity": one of "critical", "high", "medium", "low"\n'
    '- "category": one of "security", "correctness", "performance", "style"\n'
    '- "title": a short description\n'
    '- "evidence": the offending added line\n\n'
    "Look for: SQL injection via string concatenation, shell/command injection, "
    "unsafe eval or deserialization, hardcoded credentials, cryptographic misuse, "
    "insecure auth decisions, swallowed exceptions, resource leaks, race conditions, "
    "null dereferences, unbounded loops, and debug leftovers (console.log, TODO/FIXME). "
    "Be precise: report only findings you are confident are real.\n\n"
    "Diff:\n"
)


def pick(item, keys, default):
    """Return the first non-empty value among `keys`, else `default`."""
    for key in keys:
        value = item.get(key)
        if value is not None and value != "":
            return value
    return default


def parse_finding(item):
    """Turn one model JSON object into a raw finding the base can normalize."""
    rule_id = str(pick(item, ("ruleId", "rule_id"), "LLM-001"))[:20]
    severity = str(pick(item, ("severity",), "medium"))
    if severity not in ALLOWED_SEVERITY:
        severity = "medium"
    category = str(pick(item, ("category",), "style"))
    if category not in ALLOWED_CATEGORY:
        category = "style"
    try:
        line = int(pick(item, ("line",), 0))
    except (TypeError, ValueError):
        line = 0
    return {
        "ruleId": rule_id,
        "path": str(pick(item, ("path", "file"), "unknown")),
        "line": abs(line),
        "severity": severity,
        "category": category,
        "title": str(pick(item, ("title",), "review finding"))[:255],
        "evidence": str(pick(item, ("evidence",), "")),
    }


def strip_code_fences(text):
    """Remove a ```json ... ``` wrapper the model might add."""
    text = text.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        lines = lines[1:] if lines[0].startswith("```") else lines
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines).strip()
    return text


def parse_response(text):
    """Parse the model's JSON into a list of raw findings."""
    if not text or not text.strip():
        raise ProviderError("llm provider returned an empty response")
    text = strip_code_fences(text)
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        raise ProviderError(
            "llm provider returned invalid JSON: " + text[:200]
        )
    if isinstance(data, dict):
        data = data.get("findings", [])
    if not isinstance(data, list):
        raise ProviderError("llm provider response is not a findings list")
    return [parse_finding(item) for item in data if isinstance(item, dict)]


def call_model(prompt, api_key, base_url, model):
    """Synchronous Gemini generateContent call (runs in a worker thread)."""
    url = f"{base_url}/models/{model}:generateContent"
    payload = json.dumps(
        {
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "generationConfig": {"responseMimeType": "application/json"},
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json", "x-goog-api-key": api_key},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as resp:
            body = resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        snippet = exc.read().decode("utf-8", "replace")[:200]
        raise ProviderError(
            f"llm provider failed: HTTP {exc.code} {exc.reason}: {snippet}"
        )
    except OSError as exc:
        raise ProviderError(f"llm provider failed: {exc}")
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        raise ProviderError("llm provider returned a non-JSON API response")
    try:
        parts = data["candidates"][0]["content"]["parts"]
    except (KeyError, IndexError, TypeError):
        raise ProviderError("llm provider response had no candidates")
    return "".join(
        p.get("text", "") for p in parts if isinstance(p, dict)
    )


class LlmProvider(BaseProvider):
    """Review path backed by a real Google Gemini (generateContent) call.

    - Without LLM_API_KEY configured, analyze() raises ProviderError so the
      job fails gracefully with a clear message.
    - With LLM_API_KEY set, every chunk is sent to the Gemini API; a model or
      network failure also fails the job gracefully.

    Configuration:
      LLM_API_KEY   Google Gemini API key (required)
      LLM_BASE_URL  API base URL, defaults to the Gemini v1beta API
      LLM_MODEL     model name, defaults to gemini-flash-latest
    """

    name = "llm"

    async def analyze(self, chunk):
        api_key = os.getenv("LLM_API_KEY")
        if not api_key:
            raise ProviderError(
                "llm provider is not configured: set LLM_API_KEY to enable it"
            )
        base_url = os.getenv("LLM_BASE_URL", GEMINI_BASE_URL).rstrip("/")
        model = os.getenv("LLM_MODEL", DEFAULT_MODEL)
        prompt = PROMPT_PREFIX + chunk
        text = await asyncio.to_thread(call_model, prompt, api_key, base_url, model)
        return parse_response(text)
