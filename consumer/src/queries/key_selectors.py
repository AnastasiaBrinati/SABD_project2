from typing import Tuple

from pyflink.datastream import KeySelector


class CustomKeySelector(KeySelector):
    """
    KeySelector class used to key by timestamp and vault_id
    """

    def get_key(self, value) -> Tuple[str, int]:
        return value[0], value[1]
