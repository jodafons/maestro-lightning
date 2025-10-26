__all__ = [ "get_context"]



class Context:
    def __init__(self, path : str="", virtualenv : str=""):
        self.tasks = {}
        self.datasets = {}
        self.images = {}
        self.path = path
        self.virtualenv = virtualenv
    def clear(self):
        self.tasks = {}
        self.datasets = {}
        self.images = {}

__context__ = Context()

def get_context(clear : bool=False):
    global __context__
    if clear:
        __context__.clear()
    return __context__
     

from . import task 
__all__.extend( task.__all__ )
from .task import *

from . import dataset
__all__.extend( dataset.__all__ )
from .dataset import *

from . import image
__all__.extend( image.__all__ )
from .image import *
