"""
Processor modules for handling different request types.

Each processor module must implement:
- process(user_id, message_id, body, [publish_callback]) -> Optional[dict]
- STREAMING: bool (optional, True for streaming processors)

Processor Types:
- Type 1: Fire-and-forget (no return)
- Type 2: Execute and return (returns dict)
- Type 3: Streaming (uses publish_callback for multiple responses)
"""

from . import type_1_execute
from . import type_2_execute_return
from . import type_3_stream

__all__ = [
    'type_1_execute', 
    'type_2_execute_return', 
    'type_3_stream'
]
