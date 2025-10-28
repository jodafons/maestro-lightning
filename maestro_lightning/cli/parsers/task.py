__all__ = []

import argparse
from loguru import logger   
from tabulate import tabulate
from maestro_lightning import get_context
from maestro_lightning import setup_logs
from maestro_lightning.flow import load, print_tasks


def run_list(args):
    task_file = args.input_file+f"/flow.json"
    setup_logs( name = f"task_list", level=args.message_level )
    ctx = get_context( clear=True )
    logger.info(f"Loading task file {task_file}.")
    load( task_file , ctx)
    print_tasks(ctx)  
    
    

#
# args 
#
def list_parser():
    parser = argparse.ArgumentParser(description = '', add_help = False)

    parser.add_argument('-i','--input', action='store', dest='input_file', required = True,
                        help = "The job input file")
    parser.add_argument("--message-level", action="store", dest="message_level", default="ERROR"
                        , help="Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL). Default is INFO.")
    return [parser]
    
    

