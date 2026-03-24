import logging

from .server import connect_server, start_server
from .watch import watch

# ignore the expected noise of the websocket handshake failures
logging.getLogger("websockets.server").addFilter(lambda _: False)

__all__ = ["watch", "start_server", "connect_server"]
