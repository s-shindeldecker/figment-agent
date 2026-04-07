from agents.base import AgentService
from core.schema import AccountRecord


class Tier3ExternalAgent(AgentService):
    """
    Phase 2 placeholder — external data sources.
    Wire into the LD Agent Graph by adding a node in the UI
    and implementing this class. No changes to run.py required.
    """

    async def run(self) -> list[AccountRecord]:
        raise NotImplementedError("Tier 3 external agent is a Phase 2 deliverable")
