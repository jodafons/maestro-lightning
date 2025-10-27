__all__ = []

from . import process
__all__.extend( process.__all__ )
from .process import *

from . import slurm 
__all__.extend( slurm.__all__ )
from .slurm import *