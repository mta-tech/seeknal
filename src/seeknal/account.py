import uuid
from dataclasses import dataclass
from typing import Optional
import requests
from .context import context


@dataclass
class Account:
    """
    Account: Used for managing, and retrieving account.

    """

    @staticmethod
    def get_username():
        """
        Get username
        """
        return context.secrets.SEEKNAL_USERNAME
