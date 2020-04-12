from asyncio import SelectorEventLoop, set_event_loop, new_event_loop
from selectors import SelectSelector
from platform import system
from sys import version_info as version


def asyncio_setup():
    if version.major >= 3 and version.minor >= 8 and system() == "Windows":
        selector = SelectSelector()
        loop = SelectorEventLoop(selector)
        set_event_loop(loop)
    else:
        loop = new_event_loop()
        set_event_loop(loop)
