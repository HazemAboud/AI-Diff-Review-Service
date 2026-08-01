from abc import ABC, abstractmethod


class ProviderError(Exception):
    """Raised when a provider cannot complete a review."""


class BaseProvider(ABC):
    """Interface every diff-review provider implements."""

    name: str = "base"

    @abstractmethod
    async def review(self, diff: str) -> list[dict]:
        """Review a unified diff and return all findings, unordered."""
        raise NotImplementedError


def get_provider(name: str) -> BaseProvider:
    if name == "mock":
        from diffrev.processDiff.mock import MockProvider
        return MockProvider()
    if name == "llm":
        from diffrev.processDiff.llm import LlmProvider
        return LlmProvider()
    raise ProviderError(f"unknown provider: {name}")
