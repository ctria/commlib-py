from kafka import KafkaConsumer, KafkaProducer

from commlib.serializer import Serializer, JSONSerializer
from commlib.logger import Logger


class Credentials:
    def __init__(self, username: str = '', password: str = ''):
        self.username = username
        self.password = password


class ConnectionParameters:
    def __init__(self,
                 host: str = 'localhost',
                 port: int = 1883,
                 creds: Credentials = Credentials()):
        """__init__.

        Args:
            host (str): host
            port (int): port
            protocol (MQTTProtocolType): protocol
            creds (Credentials): creds
        """
        self.host = host
        self.port = port
        self.creds = creds

    @property
    def credentials(self):
        return self.creds


class MQTTTransport:
    def __init__(self,
                 conn_params: ConnectionParameters = ConnectionParameters(),
                 serializer: Serializer = JSONSerializer(),
                 logger: Logger = None):
        """__init__.

        Args:
            conn_params (ConnectionParameters): conn_params
            logger (Logger): logger
        """
        self._conn_params = conn_params
        self._logger = logger
        self._connected = False

        self._serializer = serializer

        self.logger = Logger(self.__class__.__name__) if \
            logger is None else logger

