from socket import AF_INET, AF_INET6


def get_remote_addr(transport):
    get_extra_info = transport.get_extra_info
    socket_info = get_extra_info("socket")
    if socket_info is not None:
        try:
            info = socket_info.getpeername()
        except OSError:
            # This case appears to inconsistently occur with uvloop
            # bound to a unix domain socket.
            family = None
            info = None
        else:
            family = socket_info.family

        if family in (AF_INET, AF_INET6):
            return (str(info[0]), int(info[1]))
        return None
    info = get_extra_info("peername")
    if info is not None and isinstance(info, (list, tuple)) and len(info) == 2:
        return (str(info[0]), int(info[1]))
    return None


def get_local_addr(transport):
    get_extra_info = transport.get_extra_info
    socket_info = get_extra_info("socket")
    if socket_info is not None:
        info = socket_info.getsockname()
        family = socket_info.family
        if family in (AF_INET, AF_INET6):
            return (str(info[0]), int(info[1]))
        return None
    info = get_extra_info("sockname")
    if info is not None and isinstance(info, (list, tuple)) and len(info) == 2:
        return (str(info[0]), int(info[1]))
    return None


def is_ssl(transport):
    return bool(transport.get_extra_info("sslcontext"))


def get_client_addr(scope):
    client = scope.get("client")
    if not client:
        return ""
    return "%s:%d" % client


def get_path_with_query_string(scope):
    root_path = scope.get("root_path", "")
    path = scope["path"]
    if scope["query_string"]:
        query = scope["query_string"].decode("ascii")
        return "%s%s?%s" % (root_path, path, query)
    return "%s%s" % (root_path, path)
