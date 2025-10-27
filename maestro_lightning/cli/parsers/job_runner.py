

import json
import argparse
import traceback
import shutil
import os, sys

from time import sleep
from loguru import logger
from pprint import pprint
from loguru import logger
from maestro_lightning import setup_logs, Popen, symlink
from maestro_lightning import Job, State


         
def run_job( args ):

    setup_logs( name = f"job_runner", level=args.message_level )
    workarea = args.output

    logger.info(f"loaded job {job.id} from input file {args.input}.")
    with open ( args.input , 'r') as f:    
        job = Job.from_dict( json.load(f) )
        
    logger.info(f"job id: {job.id}")
    logger.info(f"reset job status...")
    job.reset()
    job.status = State.PENDING
    
    command = job.command
    job_id = job.job_id

    logger.info("starting...")
    os.makedirs( workarea, exist_ok=True)
    logger.info(f"workarea {workarea} created.")
    logger.info("preparing command...")
    print(command)
    
     
    logger.info(f"starting env builder for job {job_id}...")
    logger.info(f"workarea {workarea}...")


    image     = job.image.path
    imagename = image.split('/')[-1]
    logger.info(f"using singularity image with name {imagename}.")
    linkpath  = symlink(image, f"{workarea}/{imagename}")
    image     = linkpath


    #for key, name in task.secondary_data.items():
    #    logger.info(f"creating secondary data link for {name} inside of the job workarea.")
    #    dataset_id = db_service.fetch_dataset_from_name( name )
    #    basepath = io_service.dataset(dataset_id).basepath
    #    linkpath = symlink( f"{basepath}" , f"{workarea}/{name}")
    #    command = command.replace(f"%{key}", linkpath)    

    
    input_data = job.input_file
    filename = input_data.split('/')[-1]
    dataset_name = input_data.split('/')[-2]
    linkpath = symlink( input_data, f"{workarea}/{dataset_name}.{filename}")
    command = command.replace(f"%IN", linkpath)
      
    outputs = []
    outputs_data = job.outputs
    for key, f in outputs_data.items():
        filename = f['name']
        targetpath = f['target']
        dataset_name = targetpath.split('/')[-1]
        filename, extension = os.path.splitext( filename )
        filename = f"{filename}.{job_id}{extension}"
        command = command.replace(f"%{key}", filename)
        targetpath = f"{targetpath}/{filename}"
        sourcepath = f"{workarea}/{filename}"
        outputs.append( (sourcepath, targetpath) )
        
    entrypoint = f"{workarea}/entrypoint.sh"
    with open(entrypoint,'w') as f:
        f.write(f"cd {workarea}\n")
        f.write(command)
            
    ok=True
    try:
        logger.info("preparing singularity command...")
        binds   = f''
        for key,value in job.binds.items():
            binds+= f' --bind {key}:{value}'
        command = f"singularity exec --nv --writable-tmpfs {binds} {image} bash {entrypoint}"
        command = command.replace('  ',' ') 

        envs = {}
        envs["JOB_ID"]               = f"{job_id}"
        envs["JOB_WORKAREA"]         = workarea 
        envs["TF_CPP_MIN_LOG_LEVEL"] = "3"
        envs["CUDA_VISIBLE_ORDER"]   = "PCI_BUS_ID"
        envs["CUDA_VISIBLE_DEVICES"] = os.environ.get("CUDA_VISIBLE_DEVICES","-1")
        envs["OMP_NUM_THREADS"]      = os.environ.get("SLURM_CPUS_PER_TASK", '4')
        envs["SLURM_CPUS_PER_TASK"]  = envs["OMP_NUM_THREADS"]
        envs["SLURM_MEM_PER_NODE"]   = os.environ.get("SLURM_MEM_PER_NODE", '2048')
        envs.update(job.envs)
        pprint(envs)
        
        logger.info("🚀 run job!")   
        logger.info(f"command: {command}")
        
        logger.info("starting the process...")
        proc = Popen(command, envs = envs)
        logger.info("process started.")
        proc.run_async()
        
        logger.info("updating job status to running...")
        job.status = State.RUNNING
        proc.join()
        while proc.is_alive():
            sleep(10)
            job.ping()
    except:
        traceback.print_exc()
        logger.error("error during the job execution.")
        job.status=State.FAILED
        sys.exit(0)

    if not ok:
        logger.error("job execution failed.")
        sys.exit(0)
    
    logger.info("job execution completed.")
    if proc.status()!="completed":
        logger.error(f"something happing during the job execution. exiting with status {proc.status()}")
        job.status=State.FAILED
        sys.exit(0)
    
    
    logger.info("uploading output files into the storage...")
    for filename, targetpath in outputs:
        logger.info(f"uploading output file {filename} to storage location {targetpath}...")
        if os.path.exists(filename):
            shutil.move( filename, targetpath)
            symlink( targetpath, filename )
        else:
            logger.error(f"output file {filename} not found in workarea {workarea}.")
            job.status=State.FAILED
            sys.exit(0)
            
    logger.info("job completed successfully.")
    job.ping()
    job.status=State.COMPLETED
    sys.exit(0)




#
# args 
#
def job_parser():
    parser = argparse.ArgumentParser(description = '', add_help = False)

    parser.add_argument('-i','--input', action='store', dest='input', required = True,
                        help = "The job input file")
    parser.add_argument('-o','--output', action='store', dest='output', required = False, default='circuit.json',
                        help = "The job output")
    parser.add_argument('-m','--message-level', action='store', dest='message_level', required = False, default='INFO',
                        help = "The job message level (DEBUG, INFO, WARNING, ERROR)")

    return [parser]
    
    

