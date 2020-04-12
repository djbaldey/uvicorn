from asyncio import Event, Queue, Protocol, get_event_loop
from logging import getLogger
from urllib.parse import unquote

from h11 import Response, EndOfMessage, Data
import wsproto
from wsproto import ConnectionType, WSConnection
from wsproto.events import (
    Request,
    Message,
    TextMessage,
    BytesMessage,
    RejectConnection,
    RejectData,
    CloseConnection,
    Ping,
    AcceptConnection,
)
from wsproto.connection import ConnectionState
from wsproto.extensions import PerMessageDeflate
from wsproto.utilities import RemoteProtocolError

from uvicorn.protocols.utils import get_local_addr, get_remote_addr, is_ssl

# Check wsproto version. We've build against 0.13. We don't know about 0.14 yet.
assert wsproto.__version__ > "0.13", "Need wsproto version 0.13"


class WSProtocol(Protocol):
    def __init__(self, config, server_state, _loop=None):
        if not config.loaded:
            config.load()

        self.config = config
        self.app = config.loaded_app
        self.loop = _loop or get_event_loop()
        self.logger = logger = getLogger("uvicorn.error")
        self.log_info = logger.info
        self.log_error = logger.error
        self.root_path = config.root_path

        # Shared server state
        self.connections = server_state.connections
        self.tasks = server_state.tasks

        # Connection state
        self.transport = None
        self.server = None
        self.client = None
        self.scheme = None

        # WebSocket state
        self.connect_event = None
        self.queue = Queue()
        self.handshake_complete = False
        self.close_sent = False

        self.conn = WSConnection(connection_type=ConnectionType.SERVER)

        self.read_paused = False
        self.writable = Event()
        self.writable.set()

        # Buffers
        self.bytes = b""
        self.text = ""

    # Protocol interface

    def connection_made(self, transport):
        self.connections.add(self)
        self.transport = transport
        self.server = get_local_addr(transport)
        self.client = get_remote_addr(transport)
        self.scheme = "wss" if is_ssl(transport) else "ws"

    def connection_lost(self, exc):
        if exc is not None:
            self.queue.put_nowait({"type": "websocket.disconnect"})
        self.connections.remove(self)

    def eof_received(self):
        pass

    def data_received(self, data):
        try:
            self.conn.receive_data(data)
        except RemoteProtocolError as err:
            if err.event_hint is not None:
                self.transport.write(self.conn.send(err.event_hint))
                self.transport.close()
            else:
                self.handle_no_connect(CloseConnection())
        else:
            self.handle_events()

    def handle_events(self):
        for event in self.conn.events():
            if isinstance(event, Request):
                self.handle_connect(event)
            elif isinstance(event, TextMessage):
                self.handle_text(event)
            elif isinstance(event, BytesMessage):
                self.handle_bytes(event)
            elif isinstance(event, RejectConnection):
                self.handle_no_connect(event)
            elif isinstance(event, RejectData):
                self.handle_no_connect(event)
            elif isinstance(event, CloseConnection):
                self.handle_close(event)
            elif isinstance(event, Ping):
                self.handle_ping(event)

    def pause_writing(self):
        """
        Called by the transport when the write buffer exceeds the high water mark.
        """
        self.writable.clear()

    def resume_writing(self):
        """
        Called by the transport when the write buffer drops below the low water mark.
        """
        self.writable.set()

    def shutdown(self):
        self.queue.put_nowait({"type": "websocket.disconnect", "code": 1012})
        output = self.conn.send(CloseConnection(code=1012))
        transport = self.transport
        transport.write(output)
        transport.close()

    def on_task_complete(self, task):
        self.tasks.discard(task)

    # Event handlers

    def handle_connect(self, event):
        self.connect_event = event
        headers = [(b"host", event.host.encode())]
        headers += [(key.lower(), value) for key, value in event.extra_headers]
        raw_path, _, query_string = event.target.partition("?")
        self.scope = {
            "type": "websocket",
            "http_version": "1.1",
            "scheme": self.scheme,
            "server": self.server,
            "client": self.client,
            "root_path": self.root_path,
            "path": unquote(raw_path),
            "raw_path": raw_path,
            "query_string": query_string.encode("ascii"),
            "headers": headers,
            "subprotocols": event.subprotocols,
        }
        self.queue.put_nowait({"type": "websocket.connect"})
        task = self.loop.create_task(self.run_asgi())
        task.add_done_callback(self.on_task_complete)
        self.tasks.add(task)

    def handle_no_connect(self, event):
        headers = [
            (b"content-type", b"text/plain; charset=utf-8"),
            (b"connection", b"close"),
        ]
        send = self.conn.send
        msg = Response(status_code=400, headers=headers, reason="Bad Request")
        output = send(msg)
        msg = Data(data=event.reason.encode("utf-8"))
        output += send(msg)
        msg = EndOfMessage()
        output += send(msg)
        transport = self.transport
        transport.write(output)
        transport.close()

    def handle_text(self, event):
        self.text += event.data
        if event.message_finished:
            self.queue.put_nowait({"type": "websocket.receive", "text": self.text})
            self.text = ""
            if not self.read_paused:
                self.read_paused = True
                self.transport.pause_reading()

    def handle_bytes(self, event):
        self.bytes += event.data
        # todo: we may want to guard the size of self.bytes and self.text
        if event.message_finished:
            self.queue.put_nowait({"type": "websocket.receive", "bytes": self.bytes})
            self.bytes = b""
            if not self.read_paused:
                self.read_paused = True
                self.transport.pause_reading()

    def handle_close(self, event):
        conn = self.conn
        transport = self.transport
        if conn.state == ConnectionState.REMOTE_CLOSING:
            transport.write(conn.send(event.response()))
        self.queue.put_nowait({"type": "websocket.disconnect", "code": event.code})
        transport.close()

    def handle_ping(self, event):
        self.transport.write(self.conn.send(event.response()))

    def send_500_response(self):
        headers = [
            (b"content-type", b"text/plain; charset=utf-8"),
            (b"connection", b"close"),
        ]
        conn = self.conn
        send = conn.send
        if conn.connection is None:
            output = send(RejectConnection(status_code=500))
        else:
            msg = Response(
                status_code=500, headers=headers, reason="Internal Server Error"
            )
            output = send(msg)
            msg = Data(data=b"Internal Server Error")
            output += send(msg)
            msg = EndOfMessage()
            output += send(msg)
        self.transport.write(output)

    async def run_asgi(self):
        try:
            result = await self.app(self.scope, self.receive, self.send)
        except BaseException as exc:
            msg = "Exception in ASGI application\n"
            self.log_error(msg, exc_info=exc)
            if not self.handshake_complete:
                self.send_500_response()
            self.transport.close()
        else:
            if not self.handshake_complete:
                msg = "ASGI callable returned without completing handshake."
                self.log_error(msg)
                self.send_500_response()
                self.transport.close()
            elif result is not None:
                msg = "ASGI callable should return None, but returned '%s'."
                self.log_error(msg, result)
                self.transport.close()

    async def send(self, message):
        await self.writable.wait()

        message_type = message["type"]

        if not self.handshake_complete:
            if message_type == "websocket.accept":
                self.log_info(
                    '%(client)s - "WebSocket %(root_path)s%(path)s" [accepted]',
                    self.scope,
                )
                self.handshake_complete = True
                subprotocol = message.get("subprotocol")
                output = self.conn.send(
                    AcceptConnection(
                        subprotocol=subprotocol, extensions=[PerMessageDeflate()]
                    )
                )
                self.transport.write(output)

            elif message_type == "websocket.close":
                self.queue.put_nowait({"type": "websocket.disconnect", "code": None})
                self.log_info(
                    '%(client)s - "WebSocket %(root_path)s%(path)s" 403', self.scope,
                )
                self.handshake_complete = True
                self.close_sent = True
                msg = Response(status_code=403, headers=[])
                output = self.conn.send(msg)
                msg = EndOfMessage()
                output += self.conn.send(msg)
                transport = self.transport
                transport.write(output)
                transport.close()

            else:
                msg = "Expected ASGI message 'websocket.accept' or 'websocket.close', but got '%s'."
                raise RuntimeError(msg % message_type)

        elif not self.close_sent:
            if message_type == "websocket.send":
                bytes_data = message.get("bytes")
                text_data = message.get("text")
                data = text_data if bytes_data is None else bytes_data
                output = self.conn.send(Message(data=data))
                transport = self.transport
                if not transport.is_closing():
                    transport.write(output)

            elif message_type == "websocket.close":
                self.close_sent = True
                code = message.get("code", 1000)
                self.queue.put_nowait({"type": "websocket.disconnect", "code": code})
                output = self.conn.send(CloseConnection(code=code))
                transport = self.transport
                if not transport.is_closing():
                    transport.write(output)
                    transport.close()

            else:
                msg = "Expected ASGI message 'websocket.send' or 'websocket.close', but got '%s'."
                raise RuntimeError(msg % message_type)

        else:
            msg = "Unexpected ASGI message '%s', after sending 'websocket.close'."
            raise RuntimeError(msg % message_type)

    async def receive(self):
        message = await self.queue.get()
        if self.read_paused and self.queue.empty():
            self.read_paused = False
            self.transport.resume_reading()
        return message
