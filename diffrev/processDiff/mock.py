import re

from diffrev.processDiff.base import BaseProvider

FILE_HEADER_RE = re.compile(r"^\+\+\+ (\S+)")
HUNK_RE = re.compile(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@")

# One line inside a file's diff: (line number in the new file, content, was it added).
# One file's worth of diff: (path, its line records).

CREDENTIAL_RE = re.compile(
    r"(api[_-]?key|secret|token)\s*[:=]\s*['\"][A-Za-z0-9_\-]{16,}['\"]",
    re.IGNORECASE,
)
CATCH_RE = re.compile(r"\bcatch\b")
SQL_KEYWORD_RE = re.compile(r"\b(select|insert|update|delete)\b", re.IGNORECASE)
INJECTION_PHRASES = ("ignore previous instructions", "disregard all prior", "you are now")


class MockProvider(BaseProvider):
    """Deterministic rule-based provider. No model is called."""

    name = "mock"

    async def analyze(self, chunk):
        findings = []
        for path, lines in iter_sections(chunk):
            findings.extend(scan_section(path, lines))
        return findings


def iter_sections(diff):
    """Split a unified diff into (path, [(new_line_number, content, added)]) sections.

    Hunk headers declare how many lines follow, so `+`/`-` content that looks
    like a file header (e.g. an added line starting with `++`) is never
    mistaken for a new file.
    """
    sections = []
    path = None
    lines = []
    new_counter = 0
    remaining_old = 0
    remaining_new = 0

    for line in diff.splitlines():
        if remaining_old or remaining_new:
            if line.startswith("-"):
                remaining_old -= 1
            elif line.startswith("+"):
                lines.append((new_counter, line[1:], True))
                remaining_new -= 1
                new_counter += 1
            elif line.startswith(" "):
                lines.append((new_counter, line[1:], False))
                remaining_old -= 1
                remaining_new -= 1
                new_counter += 1
            continue

        header = FILE_HEADER_RE.match(line)
        if header:
            if path is not None:
                sections.append((path, lines))
            name = header.group(1)
            if name.startswith("b/"):
                name = name[2:]
            path = None if name == "/dev/null" else name
            lines = []
            new_counter = 0
            continue

        if path is None:
            continue

        if line.startswith("\\"):
            continue

        hunk = HUNK_RE.match(line)
        if hunk:
            new_counter = int(hunk.group(3))
            remaining_old = int(hunk.group(2) or 1)
            remaining_new = int(hunk.group(4) or 1)

    if path is not None:
        sections.append((path, lines))
    return sections


def string_spans(line):
    """Return (start, end) ranges of the quoted string literals in `line`."""
    spans = []
    i = 0
    while i < len(line):
        char = line[i]
        if char in ('"', "'"):
            quote = char
            start = i
            i += 1
            while i < len(line):
                current = line[i]
                if current == "\\":
                    i += 2
                    continue
                if current == quote:
                    spans.append((start, i + 1))
                    break
                i += 1
        i += 1
    return spans


def is_sql_concat(line):
    """True when a SQL keyword sits inside a quoted string that is joined
    with a `+` concatenation (the `+` sits outside any string literal)."""
    spans = string_spans(line)
    if not spans:
        return False
    for i, char in enumerate(line):
        if char == "+" and not any(start <= i < end for start, end in spans):
            match = SQL_KEYWORD_RE.search(line)
            while match:
                if any(start <= match.start() < end for start, end in spans):
                    return True
                match = SQL_KEYWORD_RE.search(line, match.end())
            return False
    return False


def finding(rule_id, severity, category, title, path, line, evidence):
    return {
        "ruleId": rule_id,
        "path": path,
        "line": line,
        "severity": severity,
        "category": category,
        "title": title,
        "evidence": evidence,
    }


def scan_section(path, lines):
    findings = []
    for i, (number, content, added) in enumerate(lines):
        if not added:
            continue

        if "eval(" in content:
            findings.append(finding("MOCK-001", "critical", "security", "eval usage",
                                     path, number, content))
        if CREDENTIAL_RE.search(content):
            findings.append(finding("MOCK-002", "critical", "security", "hardcoded credential",
                                     path, number, content))
        if is_sql_concat(content):
            findings.append(finding("MOCK-003", "high", "security", "SQL string concatenation",
                                     path, number, content))
        if CATCH_RE.search(content) and is_empty_catch(lines, i):
            findings.append(finding("MOCK-004", "high", "correctness", "swallowed exception",
                                     path, number, content))
        if "== null" in content or "!= null" in content:
            findings.append(finding("MOCK-005", "medium", "correctness", "loose null comparison",
                                     path, number, content))
        if "JSON.parse(JSON.stringify(" in content:
            findings.append(finding("MOCK-006", "medium", "performance", "deep-clone via JSON",
                                     path, number, content))
        if "console.log(" in content:
            findings.append(finding("MOCK-007", "low", "style", "console.log left in",
                                     path, number, content))
        if "TODO" in content or "FIXME" in content:
            findings.append(finding("MOCK-008", "low", "style", "unresolved marker",
                                     path, number, content))
        lowered = content.lower()
        if any(phrase in lowered for phrase in INJECTION_PHRASES):
            findings.append(finding("MOCK-INJ", "critical", "security", "prompt-injection content",
                                     path, number, content))

    return findings


def is_empty_catch(lines, start):
    """True when the catch block opened at `start` contains no code before its close."""
    idx = start
    content = lines[idx][1]
    brace = content.find("{", content.find("catch"))
    while brace == -1:
        idx += 1
        if idx >= len(lines):
            return False
        content = lines[idx][1]
        brace = content.find("{")

    text = content
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
        text = lines[idx][1]
        pos = 0
