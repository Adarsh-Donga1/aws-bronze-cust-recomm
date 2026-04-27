import logging
from typing import Optional

from bronze.services.base import ServiceDefinition

logger = logging.getLogger(__name__)

_REGISTRY: dict = {}


def register(definition: ServiceDefinition) -> None:
    """Register a service definition by its name."""
    key = definition.name.upper()
    _REGISTRY[key] = definition
    logger.debug("Registered service: %s", key)


def get_service_definition(name: str) -> Optional[ServiceDefinition]:
    """Look up a registered ServiceDefinition by name (case-insensitive)."""
    return _REGISTRY.get(name.upper())


def list_registered_services() -> list:
    """Return names of all registered services."""
    return list(_REGISTRY.keys())
