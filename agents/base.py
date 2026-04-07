import ldai
import ldclient
from ldclient.config import Config


def init_ld_clients(sdk_key: str) -> tuple:
    ldclient.set_config(Config(sdk_key))
    ld_client = ldclient.get()
    ai_client = ldai.client.LDAIClient(ld_client)
    return ld_client, ai_client


class AgentService:
    """
    Base class for all E100 agents.
    Follows the pattern from the LD agent-graphs tutorial.
    Each agent:
      1. Gets its AI Config from LD (model, instructions, tools)
      2. Executes its specific data collection or processing logic
      3. Returns normalized AccountRecord list
    """

    def __init__(self, ai_client, config_key: str, context: dict):
        self.ai_client = ai_client
        self.config_key = config_key
        self.context = context  # LD targeting context

    def get_config(self):
        """Resolve AI Config from LD — gets model, instructions, tools.
        Returns None if no AI client is available (local/test mode)."""
        if self.ai_client is None:
            return None
        return self.ai_client.config(
            self.config_key,
            self.context,
            ldai.client.AIConfig(enabled=True),
            {}
        )

    async def run(self) -> list:
        """Override in subclass. Returns list of AccountRecord."""
        raise NotImplementedError
