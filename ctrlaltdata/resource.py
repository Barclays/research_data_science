import importlib
from functools import wraps

from .qad.qad import QAD


def import_module(name, package=None):
    module = importlib.import_module(name, package)
    def inner(func):
        @wraps(func)
        def inner_import_module(*args, **kwargs):
            args = list(args)
            args.append(module)
            return func(module, *args, **kwargs)
        return inner_import_module
    return inner


def import_class(name, module, package=None):
    cls = getattr(importlib.import_module(module, package), name)

    def inner(func):
        @wraps(func)
        def inner_import_module(*args, **kwargs):
            args = list(args)
            args.append(cls)
            return func(*args, **kwargs)
        return inner_import_module
    return inner


class Borg:
    _shared_state = {}
    def __init__(self):
        self.__dict__ = self._shared_state


class ResourceManager(Borg):
    # [TODO: refactor resource manager to move methods to their own modules.]
    def __init__(self):
        Borg.__init__(self)
        if not hasattr(self, "resources"):
            self.resources = dict()

    if __package__:
        context = __package__
    else:
        context = 'research_data_science.research_data_science_public.ctrlaltdata'

    @property            
    @import_class("QAD", module=f"{context}.qad.qad")
    def qad(self, QAD):
        if "qad" not in self.resources: ## Warning this is not thread safe
            qad = QAD()
            self.resources["qad"] = qad
        else:
            qad = self.resources["qad"]
        return qad