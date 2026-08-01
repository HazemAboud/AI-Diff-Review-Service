import re
from dataclasses import dataclass

FILE_HEADER_RE = re.compile(r"^\+\+\+ (\S+)")
HUNK_RE = re.compile(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@")


@dataclass
class LineRecord:
    number: int
    content: str
    added: bool


@dataclass
class FileSection:
    path: str
    lines: list[LineRecord]


def parse_diff(diff: str) -> list[FileSection]:
    """Split a unified diff into per-file sections with new-file line numbers."""
    sections: list[FileSection] = []
    current_path: str | None = None
    current_lines: list[LineRecord] = []
    new_counter = 0

    def flush() -> None:
        nonlocal current_path, current_lines
        if current_path is not None:
            sections.append(FileSection(current_path, current_lines))
        current_lines = []
        current_path = None

    for line in diff.splitlines():
        header = FILE_HEADER_RE.match(line)
        if header:
            flush()
            path = header.group(1)
            if path.startswith("b/"):
                path = path[2:]
            current_path = path if path != "/dev/null" else None
            new_counter = 0
            continue

        if current_path is None:
            continue

        hunk = HUNK_RE.match(line)
        if hunk:
            new_counter = int(hunk.group(3))
            continue

        if line.startswith("\\"):
            continue

        if line.startswith("+"):
            current_lines.append(LineRecord(new_counter, line[1:], True))
            new_counter += 1
        elif line.startswith("-"):
            continue
        elif line.startswith(" "):
            current_lines.append(LineRecord(new_counter, line[1:], False))
            new_counter += 1
        else:
            continue

    flush()
    return sections
