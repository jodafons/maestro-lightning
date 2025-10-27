"""
This module provides functionality for managing tasks in a job scheduling system.

It includes the following main functions:

1. `init(args)`: Initializes a task by loading it from a specified task file, updating its status to running, and submitting it for execution. It also creates a closing script that will be executed after the task completes.

2. `close(args)`: Finalizes a task by checking its status and updating the database accordingly. If the task fails, it cancels all dependent tasks. If successful, it starts any tasks that depend on the completed task.

3. `args_parser()`: Creates and returns an argument parser for command-line arguments related to task management.

4. `build_argparser()`: Builds the main argument parser with subparsers for the 'init' and 'close' modes.

5. `run_parser(args)`: Executes the appropriate function (`init` or `close`) based on the parsed command-line arguments.

6. `run()`: The entry point of the module that sets up the argument parser and processes command-line input.

Usage:
    This module can be executed from the command line to manage tasks by providing the appropriate arguments.
"""
__all__ = []

import sys
import argparse
from loguru import logger


from novacula.models.task   import load
from novacula               import get_context, sbatch, setup_logs
from novacula               import get_argparser_formatter
from novacula.db            import get_db_service, models 
from novacula.db            import JobStatus as job_status
from novacula.db            import TaskStatus as task_status



def init(args):
    
    setup_logs( name = f"TaskInit:{args.index}", level=args.message_level )
    ctx = get_context( clear=True )
    db_service = get_db_service( args.db_file )
    logger.info(f"Initializing task with index {args.index}.")
    logger.info(f"Loading task file {args.task_file}.")
    load( args.task_file , ctx)
    tasks = {task.task_id: task for task in ctx.tasks.values()}
    task = tasks.get( args.index )
    
    
    # update the task status to running
    logger.info(f"Updating status of task {task.name} to RUNNING.")
    db_service.task(task.name).update_status( task_status.RUNNING )
    
    # create the main script
    logger.info(f"Submitting main script for task {task.name}.")
    job_id = task.submit()
    logger.info(f"Submitted task {task.name} with job ID {job_id}.")
    
    # check if the job submission was successful
    if not job_id:
        logger.error(f"Failed to submit task {task.name}.")
        raise Exception(f"Failed to submit task {task.name}.")
    
    # create the closing script
    logger.info(f"Creating closing script for task {task.name}.")
    script = sbatch( f"{task.path}/scripts/close_task_{task.task_id}.sh",
                    args = {
                        "output"    : f"{task.path}/scripts/close_task_{task.task_id}.out",
                        "error"     : f"{task.path}/scripts/close_task_{task.task_id}.err",
                        "job-name"  : f"{task.task_id}-{task.task_id}",
                        "dependency": f"afterok:{job_id}",
                    }
    )
    script += f"source {ctx.virtualenv}/bin/activate"
    script += f"ntask close --task-file {ctx.path}/tasks.json --index {task.task_id} --db-file {ctx.path}/db/data.db"
    logger.info(f"Submitting closing script for task {task.name}.")
    script.submit()
        
    

def close(args):
    
    setup_logs( name = f"TaskCloser:{args.index}", level=args.message_level )
    ctx = get_context( clear=True )
    db_service = get_db_service( args.db_file )

    logger.info(f"Finalizing task with index {args.index}.")
    logger.info(f"Loading task file {args.task_file}.")
    load( args.task_file , ctx)
    tasks = {task.task_id: task for task in ctx.tasks.values()}
    task = tasks.get( args.index )
    
    logger.info(f"Fetched task {task.name} for finalization.")
    
    if not task:
        raise Exception(f"Task with index {args.index} not found.")

    ok = True
    logger.info(f"Checking job statuses for task {task.name}.")
    with db_service() as session:
        try:
            task_db = session.query( models.Task ).filter( models.Task.name == task.name ).one()
            if not task_db:
                raise Exception(f"Task with index {args.index} not found in database.")
            if all( job.status == job_status.COMPLETED for job in task_db.jobs ):
                logger.info(f"All jobs for task {task.name} completed successfully.")
                task_db.status = task_status.COMPLETED
            elif sum([ job.status == job_status.FAILED for job in task_db.jobs ]) / len(task_db.jobs) > 0.1:
                logger.info(f"More than 10% of jobs for task {task.name} failed.")
                task_db.status = task_status.FAILED
                ok = False
            else:
                logger.info(f"Some jobs for task {task.name} failed, but within acceptable limits.")
                task_db.status = task_status.FINALIZED
            session.commit()
        finally:
            session.close()
            
    
    # if the current task is failed, we need to cancel the entire graph
    if not ok:
        logger.info(f"Task {task.name} failed. Canceling dependent tasks.")
        with db_service() as session:
            try:
                for task_db in session.query( models.Task ).all():
                    if task_db.status == task_status.ASSIGNED:
                        logger.info(f"Canceling task {task_db.name}.")
                        task_db.status = task_status.CANCELED
                session.commit()
            finally:
                session.close()
                
    else: 
        logger.info(f"Task {task.name} finalized successfully.")
        # need to start the other tasks that depend on this one
        for task in task.next:
            logger.info(f"Starting dependent task {task.name}.")
            script = sbatch( f"{task.path}/scripts/init_task_{task.task_id}.sh",
                    args = {
                        "output"    : f"{task.path}/scripts/init_task_{task.task_id}.out",
                        "error"     : f"{task.path}/scripts/init_task_{task.task_id}.err",
                        "job-name"  : f"init-{task.task_id}",
                    }
            )
            script += f"source {ctx.virtualenv}/bin/activate"
            script += f"ntask init --task-file {ctx.path}/tasks.json --index {task.task_id} --db-file {ctx.path}/db/data.db"
            logger.info(f"Submitting initialization script for task {task.name}.")
            script.submit()
    

def args_parser():

    parser = argparse.ArgumentParser(description = '', add_help = False)
    parser.add_argument('-i','--index', action='store', dest='index', required = True,
                        help = "The task index", type=int)   
    parser.add_argument('--task-file', action='store', dest='task_file', required=True,
                        help="The task file input")
    parser.add_argument('--db-file', action='store', dest='db_file', required=True,
                        help="The database file input")
    parser.add_argument('--message-level', action='store', dest='message_level', required=False,
                        help="The logging message level", default="INFO", choices=["DEBUG","INFO","WARNING","ERROR","CRITICAL"])
    return parser


def run():
    formatter_class = get_argparser_formatter()
    parser    = argparse.ArgumentParser(formatter_class=formatter_class)
    mode = parser.add_subparsers(dest='mode')
    mode.add_parser( "init", parents=[args_parser()], help="",formatter_class=formatter_class)
    mode.add_parser( "close", parents=[args_parser()], help="",formatter_class=formatter_class)
    
    if len(sys.argv)==1:
        print(parser.print_help())
        sys.exit(1)
    args = parser.parse_args()
    
    if args.mode == "init":
        init(args)
    elif args.mode == "close":
        close(args)
       

if __name__ == "__main__":
  run()