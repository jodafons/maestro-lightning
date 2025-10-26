__all__ = [
    "random_id",
    "get_hash",
    "setup_logs",
    "get_argparser_formatter",
    "random_id", 
    "random_token", 
    "md5checksum", 
    "symlink",
]


import os
import errno
import sys, argparse
import uuid
import hashlib 

from loguru         import logger
from rich_argparse  import RichHelpFormatter



def get_hash( path : str) -> str:
    hasher = hashlib.sha256()
    with open(path, 'rb') as f:
        while chunk := f.read(8192):
            hasher.update(chunk)
    return hasher.hexdigest()

def get_argparser_formatter():
    RichHelpFormatter.styles["argparse.args"]     = "green"
    RichHelpFormatter.styles["argparse.prog"]     = "bold grey50"
    RichHelpFormatter.styles["argparse.groups"]   = "bold green"
    RichHelpFormatter.styles["argparse.help"]     = "grey50"
    RichHelpFormatter.styles["argparse.metavar"]  = "blue"
    return RichHelpFormatter

def setup_logs( name , level):
    """Setup and configure the logger"""
    logger.configure(extra={"name" : name})
    logger.remove()  # Remove any old handler
    #format="<green>{time:DD-MMM-YYYY HH:mm:ss}</green> | <level>{level:^12}</level> | <cyan>{extra[slurms_name]:<30}</cyan> | <blue>{message}</blue>"
    if level=="DEBUG":
        format="<blue>{time:DD-MMM-YYYY HH:mm:ss}</blue> | <level>{level:^12}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> |{message}"
    else:
        format="<blue>{time:DD-MMM-YYYY HH:mm:ss}</blue> | {message}"
    logger.add(
        sys.stdout,
        colorize=True,
        backtrace=True,
        diagnose=True,
        level=level,
        format=format,
    )
    


def symlink(target, linkpath):
    try:
        os.symlink(target, linkpath)
        return linkpath
    except OSError as e:
        if e.errno == errno.EEXIST:
            os.remove(linkpath)
            os.symlink(target, linkpath)
            return linkpath
        else:
            raise e
      
from . import sbatch 
__all__.extend( sbatch.__all__ )
from .sbatch import *
         
from . import popen 
__all__.extend( popen.__all__ )
from .popen import *

from . import db
__all__.extend( db.__all__ )
from .db import *

from . import models
__all__.extend( models.__all__ )
from .models import *

from . import provider
__all__.extend( provider.__all__ )
from .provider import *