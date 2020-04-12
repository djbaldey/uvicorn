from asyncio import Event, Queue, get_event_loop
from logging import getLogger

STATE_TRANSITION_ERROR = "Got invalid state transition on lifespan protocol."


class LifespanOn:
    def __init__(self, config):
        if not config.loaded:
            config.load()

        self.config = config
        logger = getLogger("uvicorn.error")
        self.log_info = logger.info
        self.log_error = logger.error
        self.logger = logger
        self.startup_event = Event()
        self.shutdown_event = Event()
        self.receive_queue = Queue()
        self.error_occured = False
        self.startup_failed = False
        self.should_exit = False

    async def startup(self):
        log_info = self.log_info
        log_info("Waiting for application startup.")

        loop = get_event_loop()
        loop.create_task(self.main())

        await self.receive_queue.put({"type": "lifespan.startup"})
        await self.startup_event.wait()

        if self.startup_failed or (self.error_occured and self.config.lifespan == "on"):
            self.log_error("Application startup failed. Exiting.")
            self.should_exit = True
        else:
            log_info("Application startup complete.")

    async def shutdown(self):
        if self.error_occured:
            return
        log_info = self.log_info
        log_info("Waiting for application shutdown.")
        await self.receive_queue.put({"type": "lifespan.shutdown"})
        await self.shutdown_event.wait()
        log_info("Application shutdown complete.")

    async def main(self):
        try:
            app = self.config.loaded_app
            scope = {"type": "lifespan"}
            await app(scope, self.receive, self.send)
        except BaseException as exc:
            self.asgi = None
            self.error_occured = True
            if self.startup_failed:
                return
            if self.config.lifespan == "auto":
                msg = "ASGI 'lifespan' protocol appears unsupported."
                self.log_info(msg)
            else:
                msg = "Exception in 'lifespan' protocol\n"
                self.log_error(msg, exc_info=exc)
        finally:
            self.startup_event.set()
            self.shutdown_event.set()

    async def send(self, message):
        message_type = message["type"]
        assert message_type in (
            "lifespan.startup.complete",
            "lifespan.startup.failed",
            "lifespan.shutdown.complete",
        )
        startup_event = self.startup_event
        shutdown_event = self.shutdown_event

        if message_type == "lifespan.startup.complete":
            assert not startup_event.is_set(), STATE_TRANSITION_ERROR
            assert not shutdown_event.is_set(), STATE_TRANSITION_ERROR
            startup_event.set()

        elif message_type == "lifespan.startup.failed":
            assert not startup_event.is_set(), STATE_TRANSITION_ERROR
            assert not shutdown_event.is_set(), STATE_TRANSITION_ERROR
            startup_event.set()
            self.startup_failed = True
            if message.get("message"):
                self.log_error(message["message"])

        elif message_type == "lifespan.shutdown.complete":
            assert startup_event.is_set(), STATE_TRANSITION_ERROR
            assert not shutdown_event.is_set(), STATE_TRANSITION_ERROR
            shutdown_event.set()

    async def receive(self):
        return await self.receive_queue.get()
