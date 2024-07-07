from typing import Tuple

from pyflink.datastream import KeySelector


class CustomKeySelector(KeySelector):
    """
    KeySelector class used to key by date and vault_id in query 2
    """

    def get_key(self, value) -> Tuple[str, int]:
        return value[0], value[1]
