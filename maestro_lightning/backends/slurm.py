__all__ = [
    "sbatch"
]

import os
import subprocess
import shlex

from typing import Dict, Any
from loguru import logger


slurm_opts = {
    "JOB_NAME"              : (True, "--job-name="),
    "OUTPUT_FILE"           : (True, "--output="),
    "ERROR_FILE"            : (True, "--error="),
    "PARTITION"             : (True, "--partition="),
    "TIME"                  : (True, "--time="),
    "EXTRA_NODE_INFO"       : (True, "--extra-node-info="),
    "BURST_BUFFER"          : (True, "--bb="),
    "BURST_BUFFER_FILE"     : (True, "--bbf="),
    "BEGIN"                 : (True, "--begin="),
    "CHDIR"                 : (True, "--chdir="),
    "CLUSTER_CONSTRAINT"    : (True, "--cluster-constraint="),
    "COMMENT"               : (True, "--comment="),
    "CONTIGUOUS"            : (False, "--contiguous"),
    "CORES_PER_SOCKET"      : (True, "--cores-per-socket="),
    "CPU_FREQ"              : (True, "--cpu-freq="),
    "CPUS_PER_TASK"         : (True, "--cpus-per-task="),
    "DEADLINE"              : (True, "--deadline="),
    "DEPENDENCY"            : (True, "--dependency="),
    "EXPORT_FILE"           : (True, "--export-file="),
    "NODE_FILE"             : (True, "--nodefile="),
    "GID"                   : (True, "--gid="),
    "GPUS_PER_SOCKET"       : (True, "--gpus-per-socket="),
    "HOLD"                  : (False, "--hold"),
    "INPUT"                 : (True, "--input="),
    "KILL_ON_INVALID_DEP"   : (True, "--kill-on-invalid-dep="),
    "LICENSES"              : (True, "--licenses="),
    "MAIL_TYPE"             : (True, "--mail-type="),
    "MAIL_USER"             : (True, "--mail-user="),
    "MIN_CPUS"              : (True, "--mincpus="),
    "NODES"                 : (True, "--nodes="),
    "NTASKS"                : (True, "--ntasks="),
    "NICE"                  : (True, "--nice="),
    "NTASKS_PER_CORE"       : (True, "--ntasks-per-core="),
    "NTASKS_PER_NODE"       : (True, "--ntasks-per-node="),
    "NTASKS_PER_SOCKET"     : (True, "--ntasks-per-socket="),
    "PRIORITY"              : (True, "--priority="),
    "PROPAGATE"             : (True, "--propagate="),
    "REBOOT"                : (False, "--reboot"),
    "OVERSUBSCRIBE"         : (False, "--oversubscribe"),
    "CORE_SPEC"             : (True, "--core-spec="),
    "SOCKETS_PER_NODE"      : (True, "--sockets-per-node="),
    "THREAD_SPEC"           : (True, "--thread-spec="),
    "THREADS_PER_CORE"      : (True, "--threads-per-core="),
    "TIME_MIN"              : (True, "--time-min="),
    "TMP"                   : (True, "--tmp="),
    "UID"                   : (True, "--uid="),
    "VERBOSE"               : (True, "--verbose"),
    "NODE_LIST"             : (True, "--nodelist="),
    "WRAP"                  : (True, "--wrap="),
    "EXCLUDE"               : (True, "--exclude="),
    "ARRAY"                 : (True, "--array="),
    "ACCOUNT"               : (True, "--account="),
    "QOS"                   : (True, "--qos="),
    "MEM"                   : (True, "--mem="),
    "MEM_PER_CPU"           : (True, "--mem-per-cpu="),
    "GRES"                  : (True, "--gres="),
}

class sbatch:
    def __init__(self, 
                 path : str,
                 opts : Dict[str, Any] = {},
                 virtualenv : str = os.environ.get("VIRTUAL_ENV", None),
            ):
            """
            Initializes a Slurm batch script with specified options.
            Args:
                path (str): The file path where the batch script will be saved.
                opts (Dict[str, Any]): A dictionary of SLURM options and their values.
                
            Raises:
                ValueError: If an invalid SLURM option is provided.ß
            """
            self.path = path
            self.lines = [f"#!/bin/bash"]
            for key, value in opts.items():
                if key not in slurm_opts:
                    raise ValueError(f"Invalid SLURM option: {key}")
                has_value, _ = slurm_opts[key]
                if has_value:
                    opts[key] = slurm_opts[key][1] + str(value)
                else:
                    opts[key] = slurm_opts[key][1]
                          
            for key, value in opts.items():
                logger.info(f"Adding SLURM option: {key} with value: {value}")
                self.lines.append( f"#SBATCH {value}" )
                
            if virtualenv:
                self.lines.append( f"source {virtualenv}/bin/activate" )


    def __add__(self, line : str):
        self.lines.append(line)
        return self

    def dump(self):
        with open (self.path, 'w') as f:
            f.write( "\n".join(self.lines) + "\n" )
            
    def submit(self) -> int:
        """
        Submits a Slurm batch script using 'sbatch' and returns the Job ID.

        Returns:
            str: The extracted Slurm Job ID, or None if submission failed.
        """
        command = f"sbatch {self.path}"
        self.dump()
        logger.info(f"Submitting job...")
        with open(self.path, 'r') as f:
            for line in f:
                print(line.rstrip())

        try:
            # shlex.split is used to correctly handle paths with spaces, etc.
            result = subprocess.run(
                shlex.split(command),
                capture_output=True,
                text=True,
                check=True  # Raise an exception for non-zero return codes
            )
            # Slurm's sbatch output format is typically: "Submitted batch job 12345"
            output = result.stdout.strip()
            # Extract the job ID (the last word in the output)
            if "Submitted batch job" in output:
                job_id = int(output.split()[-1])
                logger.info(f"Job submitted successfully with Job ID: {job_id}")
                return job_id
            else:
                logger.error(f"Submission failed or unexpected sbatch output: {output}")
                return None

        except subprocess.CalledProcessError as e:
            logger.error(f"Error submitting job (Exit Code {e.returncode}):")
            print(e.stderr)
            return None
        except FileNotFoundError:
            logger.error("Error: 'sbatch' command not found. Is Slurm installed and in your PATH?")
            return None
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
            return None

