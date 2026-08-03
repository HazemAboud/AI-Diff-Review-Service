import os
from pathlib import Path


def load_env(path=".env"):
    """Load KEY=VALUE pairs from `path` into the environment.

    Existing environment variables win over the file, and blank lines and
    `#` comments are ignored, so a checked-in secret never overrides a value
    the operator already exported.
    """
    env_file = Path(path)
    if not env_file.exists():
        return
    for line in env_file.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, _, value = stripped.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value
