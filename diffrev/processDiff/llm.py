import os

from diffrev.processDiff.base import BaseProvider, ProviderError
from diffrev.processDiff.mock import MockProvider


class LlmProvider(BaseProvider):
    """Real-model review path. Currently a mock: no model is called.

    - Without LLM_API_KEY configured, review() raises ProviderError so the
      job fails gracefully with a clear message.
    - With LLM_API_KEY set, it still returns deterministic results through the
      mock engine as a stand-in until a real model call is wired in.
    """

    name = "llm"

    def __init__(self):
        self.api_key = os.getenv("LLM_API_KEY")
        self.model = os.getenv("LLM_MODEL", "")
        self._mock = MockProvider()

    async def review(self, diff: str) -> list[dict]:
        if not self.api_key:
            raise ProviderError(
                "llm provider is not configured: set LLM_API_KEY (and LLM_MODEL) to enable it"
            )
        return await self._mock.review(diff)
