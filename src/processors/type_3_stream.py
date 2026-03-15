"""
Type 3 Processor: Continuous Execution with Streaming

This processor performs ongoing execution, streaming multiple responses
until an internal loop breaks. Designed for LLM-style query processing.

Expected body fields:
- prompt: str - The prompt/query to process
- max_chunks: int (optional) - Maximum number of chunks to generate (default: 1000)
"""
import logging
import time
from typing import Callable

logger = logging.getLogger(__name__)

# Enable streaming for this processor
STREAMING = True


def process(user_id: str, message_id: str, body: dict, 
            publish_callback: Callable[[dict], None]) -> None:
    """
    Execute a streaming operation, publishing multiple responses.
    
    Args:
        user_id: ID of the user who sent the request
        message_id: Unique identifier for this message
        body: Request body containing prompt details
        publish_callback: Function to call to publish each response chunk
    
    Returns:
        None - Responses are sent via publish_callback
    """
    prompt = body.get('message', '')
    max_chunks = body.get('max_chunks', 1000) # Increased default for long story
    
    logger.info(f"[Type 3] Starting stream for user {user_id}")
    logger.info(f"[Type 3] Message ID: {message_id}")
    
    # Story content to stream
    story = """The clock on Elias’s mantle didn’t tick; it pulsed. He had spent forty years in the basement of the Royal Observatory, tending to the Master Chronometer, the clock by which all others in the kingdom were set.

Most people believed time was a river, flowing inevitably forward. Elias knew better. Time was a delicate mechanism of brass gears and hair-thin springs that required constant lubrication and an occasional, rhythmic prayer. If the Master Chronometer slowed, the city slowed. People grew lethargic, tea cooled faster, and shadows stretched too long.

One Tuesday, Elias noticed a speck of dust wedged in the escapement wheel. As he reached in with his silver tweezers, the world outside froze. A bird hung motionless in the sky; a falling teacup hovered inches above a cafe floor. In that sliver of a second, Elias looked into the gears and saw not metal, but starlight.

He gently removed the dust. With a soft click-clack, the pulse returned. Outside, the cup shattered, and the bird flapped its wings. Elias sighed, wiping his brow. Being the architect of "now" was a lonely job, but someone had to keep the world moving."""


    words = story.split()
    chunk_index = 0
    chunk_size = 10
    
    for i in range(0, len(words), chunk_size):
        if chunk_index >= max_chunks:
            logger.info(f"[Type 3] Max chunks ({max_chunks}) reached, stopping")
            break
        
        # Get next chunk of words
        batch = words[i : i + chunk_size]
        content = " ".join(batch) + " "
        
        # Publish this chunk to simulate LLM token stream
        chunk_response = {
            "message": content,
            "done": False
        }
        
        logger.info(f"[Type 3] Publishing chunk {chunk_index}: {content}")
        publish_callback(chunk_response)
        
        chunk_index += 1
        
        # 0.10s delay to simulate generation
        time.sleep(0.10)
    
    # Send final "done" message
    final_response = {
        "message": None,
        "done": True
    }
    
    logger.info(f"[Type 3] Stream complete, sent {chunk_index} chunks")
    publish_callback(final_response)
