from asyncio import Event, Protocol, get_event_loop
from http import HTTPStatus
from logging import getLogger
from urllib.parse import unquote

from h11 import (
    Connection,
    ConnectionClosed,
    Data,
    EndOfMessage,
    InformationalResponse,
    LocalProtocolError,
    RemoteProtocolError,
    Request,
    Response,
    DONE,
    ERROR,
    MUST_CLOSE,
    NEED_DATA,
    PAUSED,
    SERVER,
)

from uvicorn.protocols.utils import (
    get_client_addr,
    get_local_addr,
    get_path_with_query_string,
    get_remote_addr,
    is_ssl,
)

# Application memory cache for encoded text of statuses.
_cache_status_phrases = {}


def _get_status_phrase(status_code):
    if status_code in _cache_status_phrases:
        return _cache_status_phrases[status_code]
    try:
        phrase = HTTPStatus(status_code).phrase.encode()
    except ValueError:
        return b""
    _cache_status_phrases[status_code] = phrase
    return phrase


STATUS_PHRASES = {
    status_code: _get_status_phrase(status_code) for status_code in range(100, 600)
}

HIGH_WATER_LIMIT = 65536

TRACE_LOG_LEVEL = 5


class FlowControl:
    def __init__(self, transport):
        self._transport = transport
        self.read_paused = False
        self.write_paused = False
        self._is_writable_event = event = Event()
        event.set()

    async def drain(self):
        await self._is_writable_event.wait()

    def pause_reading(self):
        if not self.read_paused:
            self.read_paused = True
            self._transport.pause_reading()

    def resume_reading(self):
        if self.read_paused:
            self.read_paused = False
            self._transport.resume_reading()

    def pause_writing(self):
        if not self.write_paused:
            self.write_paused = True
            self._is_writable_event.clear()

    def resume_writing(self):
        if self.write_paused:
            self.write_paused = False
            self._is_writable_event.set()


async def service_unavailable(scope, receive, send):
    await send(
        {
            "type": "http.response.start",
            "status": 503,
            "headers": [
                (b"content-type", b"text/plain; charset=utf-8"),
                (b"connection", b"close"),
            ],
        }
    )
    await send({"type": "http.response.body", "body": b"Service Unavailable"})


class H11Protocol(Protocol):
    def __init__(self, config, server_state, _loop=None):
        if not config.loaded:
            config.load()

        self.config = config
        self.app = config.loaded_app
        self.loop = _loop or get_event_loop()

        self.logger = logger = getLogger("uvicorn.error")

        def trace(message, *args, **kwargs):
            logger.log(TRACE_LOG_LEVEL, message, *args, **kwargs)

        self.log_trace = logger.trace = trace
        self.log_info = logger.info
        self.log_warning = logger.warning
        self.use_log_trace = bool(logger.level <= TRACE_LOG_LEVEL)

        self.access_logger = logger = getLogger("uvicorn.access")
        self.access_log = logger.hasHandlers()

        self.conn = Connection(SERVER)
        self.ws_protocol_class = config.ws_protocol_class
        self.root_path = config.root_path
        self.limit_concurrency = config.limit_concurrency

        # Timeouts
        self.timeout_keep_alive_task = None
        self.timeout_keep_alive = config.timeout_keep_alive

        # Shared server state
        self.server_state = server_state
        self.connections = server_state.connections
        self.tasks = server_state.tasks
        self.default_headers = server_state.default_headers

        # Per-connection state
        self.transport = None
        self.flow = None
        self.server = None
        self.client = None
        self.scheme = None

        # Per-request state
        self.scope = None
        self.headers = None
        self.cycle = None
        self.message_event = Event()

    # Protocol interface
    def connection_made(self, transport):
        self.connections.add(self)

        self.transport = transport
        self.flow = FlowControl(transport)
        self.server = get_local_addr(transport)
        self.client = get_remote_addr(transport)
        self.scheme = "https" if is_ssl(transport) else "http"

        if self.use_log_trace:
            prefix = "%s:%d - " % tuple(self.client) if self.client else ""
            self.log_trace("%sConnection made", prefix)

    def connection_lost(self, exc):
        self.connections.discard(self)

        if self.use_log_trace:
            prefix = "%s:%d - " % tuple(self.client) if self.client else ""
            self.log_trace("%sConnection lost", prefix)

        if self.cycle and not self.cycle.response_complete:
            self.cycle.disconnected = True

        conn = self.conn
        if conn.our_state != ERROR:
            event = ConnectionClosed()
            try:
                conn.send(event)
            except LocalProtocolError:
                # Premature client disconnect
                pass

        self.message_event.set()
        flow = self.flow
        if flow is not None:
            flow.resume_writing()

    def eof_received(self):
        pass

    def data_received(self, data):
        if self.timeout_keep_alive_task is not None:
            self.timeout_keep_alive_task.cancel()
            self.timeout_keep_alive_task = None

        self.conn.receive_data(data)
        self.handle_events()

    def handle_events(self):
        conn = self.conn
        connections = self.connections
        tasks = self.tasks

        while True:
            try:
                event = conn.next_event()
            except RemoteProtocolError:
                msg = "Invalid HTTP request received."
                self.log_warning(msg)
                self.transport.close()
                return
            event_type = type(event)

            if event_type is NEED_DATA:
                break

            elif event_type is PAUSED:
                # This case can occur in HTTP pipelining, so we need to
                # stop reading any more data, and ensure that at the end
                # of the active request/response cycle we handle any
                # events that have been buffered up.
                self.flow.pause_reading()
                break

            elif event_type is Request:
                self.headers = [(key.lower(), value) for key, value in event.headers]
                raw_path, _, query_string = event.target.partition(b"?")
                self.scope = {
                    "type": "http",
                    "http_version": event.http_version.decode("ascii"),
                    "server": self.server,
                    "client": self.client,
                    "scheme": self.scheme,
                    "method": event.method.decode("ascii"),
                    "root_path": self.root_path,
                    "path": unquote(raw_path.decode("ascii")),
                    "raw_path": raw_path,
                    "query_string": query_string,
                    "headers": self.headers,
                }

                for name, value in self.headers:
                    if name == b"connection":
                        tokens = [token.lower().strip() for token in value.split(b",")]
                        if b"upgrade" in tokens:
                            self.handle_upgrade(event)
                            return

                # Handle 503 responses when 'limit_concurrency' is exceeded.
                if self.limit_concurrency is not None and (
                    len(connections) >= self.limit_concurrency
                    or len(tasks) >= self.limit_concurrency
                ):
                    app = service_unavailable
                    message = "Exceeded concurrency limit."
                    self.log_warning(message)
                else:
                    app = self.app

                self.cycle = RequestResponseCycle(
                    scope=self.scope,
                    conn=self.conn,
                    transport=self.transport,
                    flow=self.flow,
                    logger=self.logger,
                    access_logger=self.access_logger,
                    access_log=self.access_log,
                    default_headers=self.default_headers,
                    message_event=self.message_event,
                    on_response=self.on_response_complete,
                )
                task = self.loop.create_task(self.cycle.run_asgi(app))
                task.add_done_callback(tasks.discard)
                tasks.add(task)

            elif event_type is Data:
                if conn.our_state is DONE:
                    continue
                cycle = self.cycle
                cycle.body += event.data
                if len(cycle.body) > HIGH_WATER_LIMIT:
                    self.flow.pause_reading()
                self.message_event.set()

            elif event_type is EndOfMessage:
                if conn.our_state is DONE:
                    self.transport.resume_reading()
                    conn.start_next_cycle()
                    continue
                self.cycle.more_body = False
                self.message_event.set()

    def handle_upgrade(self, event):
        upgrade_value = None
        for name, value in self.headers:
            if name == b"upgrade":
                upgrade_value = value.lower()

        if upgrade_value != b"websocket" or self.ws_protocol_class is None:
            msg = "Unsupported upgrade request."
            self.log_warning(msg)
            reason = STATUS_PHRASES[400]
            headers = [
                (b"content-type", b"text/plain; charset=utf-8"),
                (b"connection", b"close"),
            ]
            event = Response(status_code=400, headers=headers, reason=reason)
            send = self.conn.send
            transport = self.transport
            write = transport.write
            output = send(event)
            write(output)
            event = Data(data=b"Unsupported upgrade request.")
            output = send(event)
            write(output)
            event = EndOfMessage()
            output = send(event)
            write(output)
            self.transport.close()
            return

        self.connections.discard(self)
        output = [event.method, b" ", event.target, b" HTTP/1.1\r\n"]
        for name, value in self.headers:
            output += [name, b": ", value, b"\r\n"]
        output.append(b"\r\n")
        protocol = self.ws_protocol_class(
            config=self.config, server_state=self.server_state
        )
        protocol.connection_made(self.transport)
        protocol.data_received(b"".join(output))
        self.transport.set_protocol(protocol)

    def on_response_complete(self):
        self.server_state.total_requests += 1

        if self.transport.is_closing():
            return

        # Set a short Keep-Alive timeout.
        self.timeout_keep_alive_task = self.loop.call_later(
            self.timeout_keep_alive, self.timeout_keep_alive_handler
        )

        # Unpause data reads if needed.
        self.flow.resume_reading()

        # Unblock any pipelined events.
        conn = self.conn
        if conn.our_state is DONE and conn.their_state is DONE:
            conn.start_next_cycle()
            self.handle_events()

    def shutdown(self):
        """
        Called by the server to commence a graceful shutdown.
        """
        if self.cycle is None or self.cycle.response_complete:
            event = ConnectionClosed()
            self.conn.send(event)
            self.transport.close()
        else:
            self.cycle.keep_alive = False

    def pause_writing(self):
        """
        Called by the transport when the write buffer exceeds the high water mark.
        """
        self.flow.pause_writing()

    def resume_writing(self):
        """
        Called by the transport when the write buffer drops below the low water mark.
        """
        self.flow.resume_writing()

    def timeout_keep_alive_handler(self):
        """
        Called on a keep-alive connection if no new data is received after a short delay.
        """
        if not self.transport.is_closing():
            event = ConnectionClosed()
            self.conn.send(event)
            self.transport.close()


class RequestResponseCycle:
    def __init__(
        self,
        scope,
        conn,
        transport,
        flow,
        logger,
        access_logger,
        access_log,
        default_headers,
        message_event,
        on_response,
    ):
        self.scope = scope
        self.conn = conn
        self.transport = transport
        self.flow = flow
        self.logger = logger
        self.access_logger = logger
        self.access_log = access_log
        self.default_headers = default_headers
        self.message_event = message_event
        self.on_response = on_response

        # Connection state
        self.disconnected = False
        self.keep_alive = True
        self.waiting_for_100_continue = conn.they_are_waiting_for_100_continue

        # Request state
        self.body = b""
        self.more_body = True

        # Response state
        self.response_started = False
        self.response_complete = False

    # ASGI exception wrapper
    async def run_asgi(self, app):
        try:
            result = await app(self.scope, self.receive, self.send)
        except BaseException as exc:
            msg = "Exception in ASGI application\n"
            self.logger.error(msg, exc_info=exc)
            if not self.response_started:
                await self.send_500_response()
            else:
                self.transport.close()
        else:
            if result is not None:
                msg = "ASGI callable should return None, but returned '%s'."
                self.logger.error(msg, result)
                self.transport.close()
            elif not self.response_started and not self.disconnected:
                msg = "ASGI callable returned without starting response."
                self.logger.error(msg)
                await self.send_500_response()
            elif not self.response_complete and not self.disconnected:
                msg = "ASGI callable returned without completing response."
                self.logger.error(msg)
                self.transport.close()
        finally:
            self.on_response = None

    async def send_500_response(self):
        await self.send(
            {
                "type": "http.response.start",
                "status": 500,
                "headers": [
                    (b"content-type", b"text/plain; charset=utf-8"),
                    (b"connection", b"close"),
                ],
            }
        )
        await self.send(
            {"type": "http.response.body", "body": b"Internal Server Error"}
        )

    # ASGI interface
    async def send(self, message):
        message_type = message["type"]
        conn = self.conn
        send = conn.send
        transport = self.transport
        write = transport.write

        if self.flow.write_paused and not self.disconnected:
            await self.flow.drain()

        if self.disconnected:
            return

        if not self.response_started:
            # Sending response status line and headers
            if message_type != "http.response.start":
                msg = "Expected ASGI message 'http.response.start', but got '%s'."
                raise RuntimeError(msg % message_type)

            self.response_started = True
            self.waiting_for_100_continue = False

            status_code = message["status"]
            headers = self.default_headers + message.get("headers", [])

            if self.access_log:
                scope = self.scope
                self.access_logger.info(
                    '%s - "%s %s HTTP/%s" %d',
                    get_client_addr(scope),
                    scope["method"],
                    get_path_with_query_string(scope),
                    scope["http_version"],
                    status_code,
                    extra={"status_code": status_code, "scope": scope},
                )

            # Write response status line and headers
            reason = STATUS_PHRASES[status_code]
            event = Response(status_code=status_code, headers=headers, reason=reason)
            output = self.conn.send(event)
            self.transport.write(output)

        elif not self.response_complete:
            # Sending response body
            if message_type != "http.response.body":
                msg = "Expected ASGI message 'http.response.body', but got '%s'."
                raise RuntimeError(msg % message_type)

            body = message.get("body", b"")
            more_body = message.get("more_body", False)

            # Write response body
            if self.scope["method"] == "HEAD":
                event = Data(data=b"")
            else:
                event = Data(data=body)

            output = send(event)
            write(output)

            # Handle response completion
            if not more_body:
                self.response_complete = True
                event = EndOfMessage()
                output = send(event)
                write(output)

        else:
            # Response already sent
            msg = "Unexpected ASGI message '%s' sent, after response already completed."
            raise RuntimeError(msg % message_type)

        if self.response_complete:
            if conn.our_state is MUST_CLOSE or not self.keep_alive:
                event = ConnectionClosed()
                send(event)
                transport.close()
            self.on_response()

    async def receive(self):
        transport = self.transport
        if self.waiting_for_100_continue and not transport.is_closing():
            event = InformationalResponse(
                status_code=100, headers=[], reason="Continue"
            )
            output = self.conn.send(event)
            transport.write(output)
            self.waiting_for_100_continue = False

        if not self.disconnected and not self.response_complete:
            self.flow.resume_reading()
            event = self.message_event
            await event.wait()
            event.clear()

        if self.disconnected or self.response_complete:
            message = {"type": "http.disconnect"}
        else:
            message = {
                "type": "http.request",
                "body": self.body,
                "more_body": self.more_body,
            }
            self.body = b""

        return message
