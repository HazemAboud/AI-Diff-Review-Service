import re

from diffrev.processDiff.base import BaseProvider
from diffrev.processDiff.diffparse import FileSection, LineRecord, parse_diff

CREDENTIAL_RE = re.compile(
    r"(api[_-]?key|secret|token)\s*[:=]\s*['\"][A-Za-z0-9_\-]{16,}['\"]",
    re.IGNORECASE,
)
CATCH_RE = re.compile(r"\bcatch\b")
SQL_KEYWORDS = ("SELECT", "INSERT", "UPDATE", "DELETE")
INJECTION_PHRASES = ("ignore previous instructions", "disregard all prior", "you are now")


class MockProvider(BaseProvider):
    name = "mock"

    async def review(self, diff: str) -> list[dict]:
        findings: list[dict] = []
        for section in parse_diff(diff):
            findings.extend(_scan_section(section))
        return findings


def _finding(rule_id: str, severity: str, category: str, title: str,
             path: str, line: int, evidence: str) -> dict:
    return {
        "id": f"{rule_id}:{path}:{line}",
        "ruleId": rule_id,
        "path": path,
        "line": line,
        "severity": severity,
        "category": category,
        "title": title,
        "evidence": evidence,
    }


def _scan_section(section: FileSection) -> list[dict]:
    findings: list[dict] = []
    for i, rec in enumerate(section.lines):
        if not rec.added:
            continue
        content = rec.content

        if "eval(" in content:
            findings.append(_finding("MOCK-001", "critical", "security", "eval usage",
                                     section.path, rec.number, content))
        if CREDENTIAL_RE.search(content):
            findings.append(_finding("MOCK-002", "critical", "security", "hardcoded credential",
                                     section.path, rec.number, content))
        if "+" in content and any(kw in content.upper() for kw in SQL_KEYWORDS):
            findings.append(_finding("MOCK-003", "high", "security", "SQL string concatenation",
                                     section.path, rec.number, content))
        if CATCH_RE.search(content) and _is_empty_catch(section.lines, i):
            findings.append(_finding("MOCK-004", "high", "correctness", "swallowed exception",
                                     section.path, rec.number, content))
        if "== null" in content or "!= null" in content:
            findings.append(_finding("MOCK-005", "medium", "correctness", "loose null comparison",
                                     section.path, rec.number, content))
        if "JSON.parse(JSON.stringify(" in content:
            findings.append(_finding("MOCK-006", "medium", "performance", "deep-clone via JSON",
                                     section.path, rec.number, content))
        if "console.log(" in content:
            findings.append(_finding("MOCK-007", "low", "style", "console.log left in",
                                     section.path, rec.number, content))
        if "TODO" in content or "FIXME" in content:
            findings.append(_finding("MOCK-008", "low", "style", "unresolved marker",
                                     section.path, rec.number, content))
        lowered = content.lower()
        if any(phrase in lowered for phrase in INJECTION_PHRASES):
            findings.append(_finding("MOCK-INJ", "critical", "security", "prompt-injection content",
                                     section.path, rec.number, content))

    return findings


def _is_empty_catch(lines: list[LineRecord], start: int) -> bool:
    """True when the catch block opened at `start` contains no code before its close."""
    idx = start
    brace = lines[idx].content.find("{", lines[idx].content.find("catch"))
    while brace == -1:
        idx += 1
        if idx >= len(lines):
            return False
        brace = lines[idx].content.find("{")

    text = lines[idx].content
    pos = brace
    depth = 0
    saw_content = False
    while True:
        while pos < len(text):
            ch = text[pos]
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    return not saw_content
            elif not ch.isspace():
                saw_content = True
            pos += 1
        idx += 1
        if idx >= len(lines):
            return False
        text = lines[idx].content
        pos = 0
