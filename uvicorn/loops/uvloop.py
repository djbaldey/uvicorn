from asyncio import set_event_loop_policy

from uvloop import EventLoopPolicy


def uvloop_setup():
    set_event_loop_policy(EventLoopPolicy())
