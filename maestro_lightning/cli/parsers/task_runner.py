__all__ = []

import argparse

from loguru import logger
from maestro_lightning import State, get_context, get_argparser_formatter 
from maestro_lightning import sbatch, setup_logs 
from maestro_lightning.flow import load
from maestro_lightning.models import Task



def run_init(args):
    
    setup_logs( name = f"task_runner:{args.index}", level=args.message_level )
    ctx = get_context( clear=True )
    logger.info(f"Initializing task with index {args.index}.")
    logger.info(f"Loading task file {args.task_file}.")
    load( args.task_file , ctx)
    tasks = {task.task_id: task for task in ctx.tasks.values()}
    task = tasks.get( args.index )
    
    slurm_ops = {
        "OUTPUT_FILE"    : f"{task.path}/scripts/task_next_{task.task_id}.out",
        "ERROR_FILE"     : f"{task.path}/scripts/task_next_{task.task_id}.err",
        "JOB_NAME"      : f"next-{task.task_id}",
    }
    
    if task.status==State.COMPLETED:
        logger.info(f"Task {task.name} already completed. Skipping initialization.")
    else:
        logger.info(f"Fetched task {task.name} for initialization.")
        task.status=State.RUNNING  
        # create the main script
        logger.info(f"Submitting main script for task {task.name}.")
        job_id = task.submit()
        logger.info(f"Submitted task {task.name} with job ID {job_id}.")
        slurm_ops["DEPENDENCY"] = f"afterok:{job_id}"
    
    # create the closing script
    logger.info(f"Creating closing script for task {task.name}.")
    script = sbatch( f"{task.path}/scripts/close_task_{task.task_id}.sh", args = slurm_ops , virtualenv=ctx.virtualenv )    
    script += f"maestro run next --task-file {ctx.path}/tasks.json --index {task.task_id}"
    logger.info(f"Submitting closing script for task {task.name}.")
    script.submit()
        
    

def run_next(args):
    
    setup_logs( name = f"TaskCloser:{args.index}", level=args.message_level )
    ctx = get_context( clear=True )
    logger.info(f"Finalizing task with index {args.index}.")
    logger.info(f"Loading task file {args.task_file}.")
    load( args.task_file , ctx)
    tasks = {task.task_id: task for task in ctx.tasks.values()}
    task = tasks.get( args.index )
    logger.info(f"Fetched task {task.name} for finalization.")
    
    # update task status 
    job_status = [job.status for job in task.jobs]
    if all( [status==State.COMPLETED for status in job_status] ):
        logger.info(f"All jobs for task {task.name} completed successfully.")
        task.status = State.COMPLETED
    elif sum([status == State.FAILED for status in job_status ]) / len(job_status) > 0.1:
        logger.info(f"More than 10% of jobs for task {task.name} failed.")
        task.status = State.FAILED

        def cancel_task( task : Task ):
            for next_task in task.next:
                logger.info(f"Canceling dependent task {next_task.name}.")
                next_task.status = State.CANCELED
                cancel_task( next_task )
        logger.info(f"Task {task.name} failed. Canceling dependent tasks.")
        cancel_task( task )      
    else:
        logger.info(f"Some jobs for task {task.name} failed, but within acceptable limits.")
        task.status = State.FINALIZED
        
        
    # if the current task is failed, we need to cancel the entire graph
    if task.status not in [State.COMPLETED, State.FINALIZED]:
        logger.info(f"Task {task.name} finalized successfully.")
        # need to start the other tasks that depend on this one
        for task in task.next:
            slurm_opts = {
                        "OUTPUT_FILE"    : f"{task.path}/scripts/task_{task.task_id}.out",
                        "ERROR_FILE"     : f"{task.path}/scripts/task_{task.task_id}.err",
                        "JOB_NAME"       : f"init-{task.task_id}",
                        }
            logger.info(f"Starting dependent task {task.name}.")
            script = sbatch( f"{task.path}/scripts/init_task_{task.task_id}.sh", args = slurm_opts , virtualenv=ctx.virtualenv)
            script += f"maestro run task -i {ctx.path}/tasks.json -x {task.task_id} "
            logger.info(f"Submitting initialization script for task {task.name}.")
            script.submit()
    

def task_parser():

    parser = argparse.ArgumentParser(description = '', add_help = False)
    parser.add_argument('-i','--index', action='store', dest='index', required = True,
                        help = "The task index", type=int)   
    parser.add_argument('-t', '--task-file', action='store', dest='task_file', required=True,
                        help="The task file input")
    parser.add_argument('--message-level', action='store', dest='message_level', required=False,
                        help="The logging message level", default="INFO", choices=["DEBUG","INFO","WARNING","ERROR","CRITICAL"])
    return [parser]



