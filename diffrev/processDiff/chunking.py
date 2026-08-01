DEFAULT_CHUNK_BYTES = 65536


def chunk_diff(diff: str, chunk_bytes: int = DEFAULT_CHUNK_BYTES) -> list[str]:
    """Split a unified diff into per-file chunks of at most `chunk_bytes`.

    Splits only on file boundaries (each `diff --git` block stays intact); a
    single file larger than the budget is returned as its own chunk.
    """
    blocks = _split_file_blocks(diff)
    chunks: list[str] = []
    current: list[str] = []
    current_size = 0
    for block in blocks:
        block_size = len(block.encode("utf-8"))
        if current and current_size + block_size > chunk_bytes:
            chunks.append("".join(current))
            current = []
            current_size = 0
        current.append(block)
        current_size += block_size
    if current:
        chunks.append("".join(current))
    return chunks


def _split_file_blocks(diff: str) -> list[str]:
    lines = diff.splitlines(keepends=True)
    blocks: list[str] = []
    start = 0
    for i, line in enumerate(lines):
        if i > 0 and line.startswith("diff --git "):
            blocks.append("".join(lines[start:i]))
            start = i
    blocks.append("".join(lines[start:]))
    return [b for b in blocks if b]
