import importlib
import logging
from functools import wraps
from .config import enabled_modules


class ModuleNotEnabledException(Exception):
    def __init__(self, module_name):
        default_message = f'{module_name} is not enabled. Enable it by adding it in the enabled_modules list' \
                          f' in config.py.'
        super().__init__(default_message)


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


def import_ctrlaltdata_module(name, module, package=None):
    cls = None
    if module.split('.')[-2] in enabled_modules:
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
    @import_ctrlaltdata_module("QAD", module=f"{context}.qad.qad")
    def qad(self, QAD):
        if QAD is None:
            raise ModuleNotEnabledException('qad')
        if "qad" not in self.resources: ## Warning this is not thread safe
            qad = QAD()
            self.resources["qad"] = qad
        else:
            qad = self.resources["qad"]
        return qad

    @property
    @import_ctrlaltdata_module("Compustat", module=f"{context}.compustat.compustat")
    def compustat(self, Compustat):
        if Compustat is None:
            raise ModuleNotEnabledException('compustat')
        if "compustat" not in self.resources:  ## Warning this is not thread safe
            compustat = Compustat()
            self.resources["compustat"] = compustat
        else:
            compustat = self.resources["compustat"]
        return compustat

    @property
    @import_ctrlaltdata_module("FredReader", module=f"{context}.fred.fred")
    def fred(self, FredReader):
        if FredReader is None:
            raise ModuleNotEnabledException('fred')
        if "fred" not in self.resources:  ## Warning this is not thread safe
            fred = FredReader()
            self.resources["fred"] = fred
        else:
            fred = self.resources["fred"]
        logging.warning("The use of Fred module is intended for non-commercial purposes only.")
        return fred
