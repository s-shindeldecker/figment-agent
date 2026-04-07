def init_ld_clients(sdk_key: str) -> tuple:
    import ldai
    import ldclient
    from ldclient.config import Config
    ldclient.set_config(Config(sdk_key))
    ld_client = ldclient.get()
    ai_client = ldai.client.LDAIClient(ld_client)
    return ld_client, ai_client


class AgentService:
    """
    Base class for all E100 agents.

    Each agent:
      1. Gets its instructions from the LD Agent Graph node (if available)
      2. Executes its specific data collection or processing logic
      3. Returns normalized AccountRecord list

    The graph parameter is optional — if None, the agent runs with
    default instructions (local/test mode).
    """

    def __init__(self, ai_client, config_key: str, context, graph=None):
        self.ai_client = ai_client
        self.config_key = config_key
        self.context = context
        self.graph = graph  # AgentGraphDefinition from LD, or None

    def get_node(self):
        """
        Get this agent's node from the LD Agent Graph.
        Returns the AIAgentConfig for this node, or None if not available.
        The node contains interpolated instructions from the LD AI Config.
        """
        if self.graph is None or not self.graph.enabled:
            return None
        return self.graph.get_node(self.config_key)

    def get_instructions(self):
        """
        Get the instructions for this agent from LD.
        Returns None if running in local mode — agents use their own logic.
        """
        node = self.get_node()
        if node:
            return node.instructions
        return None

    async def run(self) -> list:
        """Override in subclass. Returns list of AccountRecord."""
        raise NotImplementedError
