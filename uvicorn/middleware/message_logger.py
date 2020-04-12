import logging

PLACEHOLDER_FORMAT = {
    "body": "<{length} bytes>",
    "bytes": "<{length} bytes>",
    "text": "<{length} chars>",
    "headers": "<...>",
}
TRACE_LOG_LEVEL = 5


def message_with_placeholders(message):
    """
    Return an ASGI message, with any body-type content omitted and replaced
    with a placeholder.
    """
    new_message = message.copy()
    for attr in PLACEHOLDER_FORMAT.keys():
        if message.get(attr) is not None:
            content = message[attr]
            placeholder = PLACEHOLDER_FORMAT[attr].format(length=len(content))
            new_message[attr] = placeholder
    return new_message


class MessageLoggerMiddleware:
    def __init__(self, app):
        self.task_counter = 0
        self.app = app
        self.logger = logging.getLogger("uvicorn.asgi")

        def trace(message, *args, **kwargs):
            logging.log(TRACE_LOG_LEVEL, message, *args, **kwargs)

        self.logger.trace = trace

    async def __call__(self, scope, receive, send):
        self.task_counter += 1

        task_counter = self.task_counter
        client = scope.get("client")
        prefix = "%s:%d - ASGI" % (client[0], client[1]) if client else "ASGI"

        log_trace = self.logger.trace

        async def inner_receive():
            message = await receive()
            logged_message = message_with_placeholders(message)
            log_text = "%s [%d] Receive %s"
            log_trace(log_text, prefix, task_counter, logged_message)
            return message

        async def inner_send(message):
            logged_message = message_with_placeholders(message)
            log_text = "%s [%d] Send %s"
            log_trace(log_text, prefix, task_counter, logged_message)
            await send(message)

        logged_scope = message_with_placeholders(scope)
        log_text = "%s [%d] Started scope=%s"
        log_trace(log_text, prefix, task_counter, logged_scope)
        try:
            await self.app(scope, inner_receive, inner_send)
        except BaseException as exc:
            log_text = "%s [%d] Raised exception"
            log_trace(log_text, prefix, task_counter)
            raise exc from None
        else:
            log_text = "%s [%d] Completed"
            log_trace(log_text, prefix, task_counter)
