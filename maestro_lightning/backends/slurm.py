__all__ = [
    "sbatch"
]

import subprocess
import shlex

from typing import Union, Dict
from loguru import logger




class sbatch:
    def __init__(self, 
                 path : str,
                 args : Dict[str, Union[str, int]] = {}
            ):
            """
            Initializes a new instance of the class.

            Parameters:
            path (str): The path to the script or job file.
            partition (str, optional): The partition to submit the job to. Defaults to None.
            output (str, optional): The file to write standard output to. Defaults to None.
            error (str, optional): The file to write standard error to. Defaults to None.
            n_tasks (int, optional): The number of tasks for the job array. Defaults to None.
            name (str, optional): The name of the job. Defaults to None.

            This constructor sets up the necessary SLURM directives for job submission.
            """
            self.path = path
            self.lines = [f"#!/bin/bash"]
            for key, value in args.items():
                self.lines.append( f"#SBATCH --{key}={value}" )

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
                job_id = output.split()[-1]
                return int(job_id)
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

