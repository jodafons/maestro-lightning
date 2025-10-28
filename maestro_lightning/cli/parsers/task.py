__all__ = []

import argparse
from loguru import logger   
from maestro_lightning import get_context
from maestro_lightning import setup_logs
from maestro_lightning.models import Dataset, Image, Task
from maestro_lightning.flow import load, print_tasks, Flow


def run_list(args):
    task_file = args.input_file+f"/flow.json"
    setup_logs( name = f"task_list", level=args.message_level )
    ctx = get_context( clear=True )
    logger.info(f"Loading task file {task_file}.")
    load( task_file , ctx)
    print_tasks(ctx)  
    
    
def run_create(args):
    
    with Flow(name="flow", path=args.output_dir) as session:
        Task(name=args.name,
             command=args.command,
             input_data= Dataset(name="input_dataset", path=args.input_file),
             outputs=eval(args.outputs),
             partition=args.partition,
             image=Image(name="image", path=args.image) if args.image else None,
             binds=eval(args.binds))
        session.run()

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
   
def create_parser():
    parser = argparse.ArgumentParser(description = '', add_help = False)

    parser.add_argument('-i','--input', action='store', dest='input_file', required = True,
                        help = "The job input file")
    parser.add_argument("--message-level", action="store", dest="message_level", default="ERROR",
                        help="Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL). Default is INFO.")
    parser.add_argument("-c", "--command", action="store", dest="command", required=True,
                        help="The command to execute.")
    parser.add_argument("-m", "--image", action="store", dest="image", required=False, default=None,
                        help="The image to use.")
    parser.add_argument("-o", "--outputs", action="store", dest="outputs", required=True,
                        help="The outputs of the task.")
    parser.add_argument("-p", "--partition", action="store", dest="partition", required=False,
                        help="The partition to use.")
    parser.add_argument("-b", "--binds", action="store", dest="binds", required=False, default="{}",
                        help="The binds to use.")
    parser.add_argument("-d", "--output-dir", action="store", dest="output_dir", required=True,
                        help="The output directory for the task.")
    parser.add_argument("-n", "--name", action="store", dest="name", required=True,
                        help="The name of the task.")
    return [parser] 
    

