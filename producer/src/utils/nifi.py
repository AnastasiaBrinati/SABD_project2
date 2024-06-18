from __future__ import annotations

import urllib3
import nipyapi.config
import nipyapi.utils
import nipyapi.security
import nipyapi.canvas

from .logs import factory as loggerfactory
from .conf import factory as configfactory


class NifiError(Exception):
    pass


class NifiApi:

    def __init__(self) -> None:
        pass

    @staticmethod
    def init_api() -> NifiApi:
        endpoint = configfactory.ConfigFactory.config().nifi_endpoint

        # Keep logs minimal
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        # disable TLS check (trusted network configuration)
        nipyapi.config.nifi_config.verify_ssl = False
        nipyapi.config.registry_config.verify_ssl = False

        # connect to Nifi
        nipyapi.utils.set_endpoint(endpoint + "/nifi-api")
        # wait for connection to be set up
        connected = nipyapi.utils.wait_to_complete(
            test_function=nipyapi.utils.is_endpoint_up,
            endpoint_url=endpoint + "/nifi",
            nipyapi_delay=nipyapi.config.long_retry_delay,
            nipyapi_max_wait=nipyapi.config.short_max_wait
        )

        if connected:
            loggerfactory.LoggerFactory.nifi().nifi_connection_success()
        else:
            raise NifiError('Connection to Nifi failed..')

        return NifiApi()

    def login(self, username: str, password: str) -> NifiApi:
        login = nipyapi.security.service_login(
            service='nifi', username=username, password=password, bool_response=True)
        if login:
            loggerfactory.LoggerFactory.nifi().nifi_login_success()
        else:
            raise NifiError('Login failed..')
        return self

    def schedule_ingestion(self) -> None:
        nipyapi.canvas.schedule_process_group(
            process_group_id='root', scheduled=True)
        loggerfactory.LoggerFactory.nifi().log("Ingestion scheduled..")
