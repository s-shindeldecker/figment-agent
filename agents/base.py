class AgentService:
    """
    Base class for E100 tier agents.

    ``ai_client`` and ``context`` are optional legacy hooks (unused in the linear pipeline).

    The optional ``graph`` parameter is legacy (Agent Graph); the linear pipeline passes ``None``.
    """

    def __init__(self, ai_client, config_key: str, context, graph=None):
        self.ai_client = ai_client
        self.config_key = config_key
        self.context = context
        self.graph = graph

    def get_node(self):
        if self.graph is None or not getattr(self.graph, "enabled", False):
            return None
        return self.graph.get_node(self.config_key)

    def get_instructions(self):
        node = self.get_node()
        if node is None:
            return None
        cfg = node.get_config()
        if cfg is None:
            return None
        return cfg.instructions

    def log_graph_binding(self):
        """No-op when graph is unused; kept for subclasses that may log config binding."""
        if self.graph is None:
            return
        node = self.get_node()
        if node is None:
            print(f"[Graph] {self.config_key}: no node for this config key")
            return
        instr = self.get_instructions()
        n = len(instr) if instr else 0
        print(f"[Graph] {self.config_key}: node bound ({n} chars instructions)")

    async def run(self) -> list:
        """Override in subclass. Returns list of AccountRecord."""
        raise NotImplementedError
