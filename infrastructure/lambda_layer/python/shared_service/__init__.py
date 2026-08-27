"""Shared Service Layer Package"""

__version__ = "3.3.0-dual-backend"

from .dynamo import (
    DynamoService,
    estimate_request_tokens,
    estimate_request_tokens_split,
    BurstCapacityExceeded,
)
from .bedrock_client import (
    BedrockClient,
    RuntimeConverseClient,
    MantleMessagesClient,
    BedrockResponse,
    TokenEstimate,
    client_for,
)

__all__ = [
    "DynamoService",
    "estimate_request_tokens",
    "estimate_request_tokens_split",
    "BurstCapacityExceeded",
    "BedrockClient",
    "RuntimeConverseClient",
    "MantleMessagesClient",
    "BedrockResponse",
    "TokenEstimate",
    "client_for",
    "__version__",
]
