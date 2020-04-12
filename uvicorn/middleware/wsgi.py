import sys
from asyncio import Event, wait_for as asyncio_wait_for, get_event_loop
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO


def build_environ(scope, message, body):
    """
    Builds a scope and request message into a WSGI environ object.
    """
    environ = {
        "REQUEST_METHOD": scope["method"],
        "SCRIPT_NAME": "",
        "PATH_INFO": scope["path"],
        "QUERY_STRING": scope["query_string"].decode("ascii"),
        "SERVER_PROTOCOL": "HTTP/%s" % scope["http_version"],
        "wsgi.version": (1, 0),
        "wsgi.url_scheme": scope.get("scheme", "http"),
        "wsgi.input": BytesIO(body),
        "wsgi.errors": sys.stdout,
        "wsgi.multithread": True,
        "wsgi.multiprocess": True,
        "wsgi.run_once": False,
    }

    # Get server name and port - required in WSGI, not in ASGI
    server = scope.get("server")
    if server is None:
        server = ("localhost", 80)
    environ["SERVER_NAME"] = server[0]
    environ["SERVER_PORT"] = server[1]

    # Get client IP address
    client = scope.get("client")
    if client is not None:
        environ["REMOTE_ADDR"] = client[0]

    # Go through headers and make them into environ entries
    for name, value in scope.get("headers", []):
        name = name.decode("latin1")
        if name == "content-length":
            corrected_name = "CONTENT_LENGTH"
        elif name == "content-type":
            corrected_name = "CONTENT_TYPE"
        else:
            corrected_name = "HTTP_%s" % name.upper().replace("-", "_")
        # HTTPbis say only ASCII chars are allowed in headers, but we latin1 just in case
        value = value.decode("latin1")
        if corrected_name in environ:
            value = environ[corrected_name] + "," + value
        environ[corrected_name] = value
    return environ


class WSGIMiddleware:
    def __init__(self, app, workers=10):
        self.app = app
        self.executor = ThreadPoolExecutor(max_workers=workers)

    async def __call__(self, scope, receive, send):
        assert scope["type"] == "http"
        instance = WSGIResponder(self.app, self.executor, scope)
        await instance(receive, send)


class WSGIResponder:
    def __init__(self, app, executor, scope):
        self.app = app
        self.executor = executor
        self.scope = scope
        self.status = None
        self.response_headers = None
        self.send_event = Event()
        self.send_queue = []
        self.loop = None
        self.response_started = False
        self.exc_info = None

    async def __call__(self, receive, send):
        message = await receive()
        body = message.get("body", b"")
        more_body = message.get("more_body", False)
        while more_body:
            body_message = await receive()
            body += body_message.get("body", b"")
            more_body = body_message.get("more_body", False)
        environ = build_environ(self.scope, message, body)
        self.loop = loop = get_event_loop()
        wsgi = loop.run_in_executor(
            self.executor, self.wsgi, environ, self.start_response
        )
        sender = loop.create_task(self.sender(send))
        try:
            await asyncio_wait_for(wsgi, None)
        finally:
            self.send_queue.append(None)
            self.send_event.set()
            await asyncio_wait_for(sender, None)
        exc_info = self.exc_info
        if exc_info is not None:
            raise exc_info[0].with_traceback(exc_info[1], exc_info[2])

    async def sender(self, send):
        queue = self.send_queue
        event = self.send_event
        while True:
            if queue:
                message = queue.pop(0)
                if message is None:
                    return
                await send(message)
            else:
                await event.wait()
                event.clear()

    def start_response(self, status, response_headers, exc_info=None):
        self.exc_info = exc_info
        if not self.response_started:
            self.response_started = True
            status_code, _ = status.split(" ", 1)
            status_code = int(status_code)
            headers = [
                (name.encode("ascii"), value.encode("ascii"))
                for name, value in response_headers
            ]
            self.send_queue.append(
                {
                    "type": "http.response.start",
                    "status": status_code,
                    "headers": headers,
                }
            )
            self.loop.call_soon_threadsafe(self.send_event.set)

    def wsgi(self, environ, start_response):
        event = self.send_event
        queue = self.send_queue
        call_soon_threadsafe = self.loop.call_soon_threadsafe
        for chunk in self.app(environ, start_response):
            queue.append(
                {"type": "http.response.body", "body": chunk, "more_body": True}
            )
            call_soon_threadsafe(event.set)

        queue.append({"type": "http.response.body", "body": b""})
        call_soon_threadsafe(event.set)
