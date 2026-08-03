import re

DEFAULT_CHUNK_BYTES = 65536

HUNK_RE = re.compile(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@")


def chunk_diff(diff, chunk_bytes=DEFAULT_CHUNK_BYTES):
    """Group whole files into chunks.

    - A diff at or under the budget is a single chunk.
    - A single-file diff is always one chunk, even over budget.
    - Otherwise the first file forms its own chunk, then the same rule
      recurses on the remaining files. Files are never split.
    """
    blocks = split_file_blocks(diff)
    if not blocks:
        return []
    if len(blocks) == 1 or size(diff) <= chunk_bytes:
        return [diff]

    chunks = []
    rest = blocks
    while True:
        first, rest = rest[0], rest[1:]
        chunks.append(first)
        if len(rest) == 1 or sum(size(b) for b in rest) <= chunk_bytes:
            chunks.append("".join(rest))
            break
    return chunks


def size(s):
    return len(s.encode("utf-8"))


def split_file_blocks(diff):
    """Split on the start of a new file, regardless of what produced the diff.

    A new file begins at `diff --git ` (git) or at the `--- ` old-file header
    (plain `diff -u`). Hunk headers declare how many content lines follow, so
    those counts are consumed exactly; `+`/`-` content lines that happen to
    look like headers (e.g. an added line starting with `++`) are never
    mistaken for file boundaries.
    """
    lines = diff.splitlines(keepends=True)
    blocks = []
    start = 0
    in_file = False
    git_file = False
    remaining_old = 0
    remaining_new = 0

    def begin_file(i, git):
        nonlocal start, in_file, git_file, remaining_old, remaining_new
        if in_file:
            blocks.append("".join(lines[start:i]))
        start = i if in_file else 0
        in_file = True
        git_file = git
        remaining_old = 0
        remaining_new = 0

    for i, line in enumerate(lines):
        if not in_file:
            if line.startswith("diff --git "):
                begin_file(i, True)
            elif line.startswith("--- "):
                begin_file(i, False)
            continue

        if remaining_old or remaining_new:
            if line.startswith("diff --git "):
                begin_file(i, True)
            elif line.startswith("--- ") and not git_file:
                begin_file(i, False)
            elif line.startswith("-"):
                remaining_old -= 1
            elif line.startswith("+"):
                remaining_new -= 1
            elif line.startswith(" "):
                remaining_old -= 1
                remaining_new -= 1
            continue

        if line.startswith("diff --git "):
            begin_file(i, True)
        elif line.startswith("--- ") and not git_file:
            begin_file(i, False)
        elif line.startswith("+++ "):
            continue
        else:
            hunk = HUNK_RE.match(line)
            if hunk:
                remaining_old = int(hunk.group(2) or 1)
                remaining_new = int(hunk.group(4) or 1)

    blocks.append("".join(lines[start:]))
    return [b for b in blocks if b]
