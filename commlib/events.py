import time
import datetime
from typing import Text, OrderedDict, Any

from .serializer import JSONSerializer, Serializer
from .logger import Logger
from .utils import gen_random_id
from .msg import Object, DataClass, DataField


@DataClass
class Event(Object):
    """Event.
    """

    name: Text
    description: Text
    uri: Text
    payload: OrderedDict = DataField(default_factory=OrderedDict)


class BaseEventEmitter(object):
    """BaseEventEmitter.
    """

    def __init__(self,
                 name: Text = None,
                 logger: Logger = None,
                 debug: bool = False,
                 serializer: Serializer = None):
        """__init__.

        Args:
            name (Text): name
            logger (Logger): logger
            debug (bool): debug
            serializer (Serializer): serializer
        """
        if name is None:
            name = gen_random_id()
        self._name = name
        self._debug = debug
        if serializer is not None:
            self._serializer = serializer
        else:
            self._serializer = JSONSerializer

        self._logger = Logger(self.__class__.__name__, debug=debug) if \
            logger is None else logger
        self.logger.info(f'Initiated Event Emitter <{self._name}>')

    @property
    def debug(self) -> bool:
        """debug.

        Args:

        Returns:
            bool:
        """
        return self._debug

    @property
    def logger(self) -> Logger:
        """logger.

        Args:

        Returns:
            Logger:
        """
        return self._logger

    def send_event(self, event: Event) -> None:
        """send_event.

        Args:
            event (Event): event

        Returns:
            None:
        """
        raise NotImplementedError()
