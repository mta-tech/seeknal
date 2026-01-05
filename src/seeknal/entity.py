import json
from typing import List, Optional
from dataclasses import dataclass, field
import typer
from tabulate import tabulate
import hashlib

from .context import context as seeknal_context, logger
from .utils import to_snake
from .models import metadata, EntityTable
from .request import EntityRequest
from .exceptions import EntityNotFoundError, EntityNotSavedError


def require_saved(func):
    """Decorator that ensures an entity has been saved before method execution.

    This decorator checks if the entity instance has an 'entity_id' attribute,
    which indicates it has been persisted via get_or_create(). If not, it raises
    an EntityNotSavedError.

    Args:
        func: The method to wrap.

    Returns:
        A wrapper function that validates the entity is saved before calling
        the original method.

    Raises:
        EntityNotSavedError: If the entity has not been saved or loaded.
    """
    def wrapper(self, *args, **kwargs):
        if not "entity_id" in vars(self):
            raise EntityNotSavedError("Entity not loaded or saved")
        else:
            func(self, *args, **kwargs)

    return wrapper


@dataclass
class Entity:
    """Represents an entity in the feature store.

    An entity defines a domain object (e.g., user, product, transaction) that
    features are associated with. Entities have join keys that uniquely identify
    instances and can optionally specify PII (Personally Identifiable Information)
    keys for data privacy compliance.

    Attributes:
        name: The entity name. Will be converted to snake_case automatically.
        join_keys: List of column names that uniquely identify entity instances.
            These keys are used for joining features during retrieval.
        pii_keys: Optional list of column names containing personally identifiable
            information. Used for data privacy and compliance purposes.
        description: Optional human-readable description of the entity.

    Example:
        >>> entity = Entity(
        ...     name="customer",
        ...     join_keys=["customer_id"],
        ...     pii_keys=["email", "phone"],
        ...     description="Customer entity for retail features"
        ... )
        >>> entity.get_or_create()
    """

    name: str
    join_keys: Optional[List[str]] = None
    pii_keys: Optional[List[str]] = None
    description: Optional[str] = None

    def __post_init__(self):
        """Initialize the entity and normalize the name to snake_case."""
        self.name = to_snake(self.name)

    def get_or_create(self):
        """Retrieve an existing entity or create a new one.

        This method checks if an entity with the same name already exists in the
        feature store. If found, it loads the existing entity's properties into
        this instance. If not found, it creates a new entity with the current
        instance's properties.

        After calling this method, the entity will have an 'entity_id' attribute
        set, which is required for operations like update().

        Returns:
            Entity: The current instance with entity_id set and properties
                synchronized with the persisted entity.

        Example:
            >>> entity = Entity(name="user", join_keys=["user_id"])
            >>> entity = entity.get_or_create()
            >>> print(entity.entity_id)  # Now has an ID
        """
        req = EntityRequest(body=vars(self))
        entity = req.select_by_name(self.name)
        if entity is None:
            self.entity_id = req.save()
        else:
            self.entity_id = entity.id
            self.name = entity.name
            self.join_keys = entity.join_keys.split(",")
            if entity.pii_keys is not None:
                self.pii_keys = entity.pii_keys.split(",")
            else:
                self.pii_keys = None
            self.description = entity.description
        return self

    @staticmethod
    def list():
        """List all registered entities in the feature store.

        Retrieves all entities from the feature store and displays them in a
        formatted table. The table includes entity name, join keys, PII keys,
        and description.

        This is a static method that can be called without instantiating an
        Entity object.

        Example:
            >>> Entity.list()
            | name     | join_keys   | pii_keys | description          |
            |----------|-------------|----------|----------------------|
            | customer | customer_id | email    | Customer entity      |
            | product  | product_id  | None     | Product catalog item |
        """
        entities = EntityRequest.select_all()
        if entities:
            entities = [
                {
                    "name": entity.name,
                    "join_keys": entity.join_keys,
                    "pii_keys": entity.pii_keys,
                    "description": entity.description,
                }
                for entity in entities
            ]
            typer.echo(tabulate(entities, headers="keys", tablefmt="github"))
        else:
            typer.echo("No entities found.")

    @require_saved
    def update(self, name=None, description=None, pii_keys=None):
        """Update the entity's properties in the feature store.

        Updates the entity with new values for name, description, or PII keys.
        Only the provided parameters will be updated; others retain their
        current values. The entity must have been previously saved via
        get_or_create() before calling this method.

        Note:
            Join keys cannot be updated after entity creation as they define
            the entity's identity.

        Args:
            name: Optional new name for the entity. Will be converted to
                snake_case.
            description: Optional new description for the entity.
            pii_keys: Optional new list of PII key column names.

        Raises:
            EntityNotSavedError: If the entity has not been saved via
                get_or_create() first.
            EntityNotFoundError: If the entity no longer exists in the
                feature store.

        Example:
            >>> entity = Entity(name="user", join_keys=["user_id"])
            >>> entity.get_or_create()
            >>> entity.update(description="Updated user entity")
        """
        entity = EntityRequest.select_by_id(self.entity_id)
        if entity is None:
            raise EntityNotFoundError("Entity not found.")
        if name is None:
            name = entity.name
        if description is None:
            description = entity.description
        if pii_keys is None:
            pii_keys = entity.pii_keys
        req = EntityRequest(
            body={
                "name": name,
                "description": description,
                "pii_keys": pii_keys,
                "join_keys": entity.join_keys.split(","),
            }
        )
        req.save()
        self.name = name
        self.description = description
        self.pii_keys = pii_keys

    def set_key_values(self, *args):
        """Set specific values for the entity's join keys.

        Maps positional arguments to the entity's join keys in order, storing
        them in the key_values attribute. This is useful for point lookups
        when retrieving features for a specific entity instance.

        Args:
            *args: Values for each join key, in the same order as defined
                in join_keys. The number of arguments must match the number
                of join keys.

        Returns:
            Entity: The current instance with key_values set.

        Example:
            >>> entity = Entity(name="order", join_keys=["user_id", "order_id"])
            >>> entity.get_or_create()
            >>> entity.set_key_values("user123", "order456")
            >>> print(entity.key_values)
            {'user_id': 'user123', 'order_id': 'order456'}
        """
        key_values = {}
        for idx, i in enumerate(self.join_keys):
            key_values[i] = args[idx]

        self.key_values = key_values
        return self
