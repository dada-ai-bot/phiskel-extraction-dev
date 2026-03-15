"""
Type 2 Processor: Execute and Return

This processor performs an action and returns a single response.
Use cases: data lookup, calculations, synchronous operations.

Expected body fields:
- operation: str - The operation to perform
- value: any - Input value for the operation
"""
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# No streaming for this processor
STREAMING = False


def process(user_id: str, message_id: str, body: dict) -> dict:
    """
    Execute an operation and return the result.
    
    Args:
        user_id: ID of the user who sent the request
        message_id: Unique identifier for this message
        body: Request body containing operation details
    
    Returns:
        dict: Response body with operation result
    """
    operation = body.get('operation', 'echo')
    value = body.get('value')
    
    logger.info(f"[Type 2] Processing operation '{operation}' for user {user_id}")
    logger.info(f"[Type 2] Message ID: {message_id}, Value: {value}")
    
    # Perform operation based on type
    result = None
    status = "success"
    
    try:
        if operation == 'echo':
            # Simple echo - return the input value
            result = value
            
        elif operation == 'calculate':
            # Example calculation - double the value
            if isinstance(value, (int, float)):
                result = value * 2
            else:
                result = str(value) + str(value)
                
        elif operation == 'get_time':
            # Return current server time
            result = datetime.now().isoformat()
            
        elif operation == 'get_info':
            # Return some info about the request
            result = {
                "user_id": user_id,
                "message_id": message_id,
                "received_at": datetime.now().isoformat(),
                "input_value": value
            }
            
        else:
            # Unknown operation - still return something
            result = f"Unknown operation: {operation}"
            status = "unknown_operation"
            
    except Exception as e:
        logger.error(f"[Type 2] Error processing operation: {e}")
        result = str(e)
        status = "error"
    
    response = {
        "status": status,
        "operation": operation,
        "result": result
    }
    
    logger.info(f"[Type 2] Returning response: {response}")
    return response
