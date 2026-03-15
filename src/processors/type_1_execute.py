"""
Type 1 Processor: Execute Only (Fire-and-forget)

This processor performs an action without returning a response.
Use cases: logging, triggering side effects, fire-and-forget operations.

Expected body fields:
- action: str - The action to perform
- data: any - Data associated with the action
"""
import logging

logger = logging.getLogger(__name__)

# No streaming for this processor
STREAMING = False


def process(user_id: str, message_id: str, body: dict) -> None:
    """
    Execute an action without returning a response.
    
    Args:
        user_id: ID of the user who sent the request
        message_id: Unique identifier for this message
        body: Request body containing action details
    
    Returns:
        None - This processor does not return a response
    """
    action = body.get('action', 'unknown')
    data = body.get('data')
    
    logger.info(f"[Type 1] Executing action '{action}' for user {user_id}")
    logger.info(f"[Type 1] Message ID: {message_id}")
    logger.info(f"[Type 1] Data: {data}")
    
    # Example: Perform the action based on action type
    if action == 'log_event':
        logger.info(f"[Type 1] EVENT LOGGED: {data}")
    elif action == 'trigger_task':
        logger.info(f"[Type 1] TASK TRIGGERED: {data}")
    else:
        logger.info(f"[Type 1] GENERIC ACTION: {action} with data: {data}")
    
    # No return value - fire and forget
    return None
