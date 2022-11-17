import gettext

def L(message:str, *args, **kwds) -> str:
    return gettext.gettext(message).format(*args, **kwds)