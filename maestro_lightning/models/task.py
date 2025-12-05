

"""
This module defines the Task class, which represents a computational task
that can be executed within a specific context. The Task class manages
various attributes related to the task, including its name, associated
image, command to be executed, input and output data, and task dependencies.

Key functionalities of the Task class include:
- Initialization of task parameters with validation.
- Creation of necessary directory structures for task execution.
- Submission of the task to a job scheduler.
- Conversion of the task instance into a raw dictionary representation for serialization.

The module also imports necessary components from other parts of the application,
including context management, database services, and logging utilities.
"""

__all__ = [
    "Task",
]

import os, json

from typing                  import Union, Dict, List
from expand_folders          import expand_folders
from filelock                import FileLock
from loguru                  import logger

from maestro_lightning.models         import get_context, Job, Status, State, job_status
from maestro_lightning.models.image   import Image 
from maestro_lightning.models.dataset import Dataset
from maestro_lightning                import sbatch
from maestro_lightning.exceptions     import *




class Task:
    
    def __init__(self,
                     name           : str,
                     command        : str,
                     input_data     : Union[str, Dataset],
                     outputs        : Dict[str, str],
                     partition      : str,
                     image          : Union[str, Image]=None,
                     secondary_data : Dict[str, Union[str, Dataset]] = {},
                     binds          : Dict[str, str] = {},
                     envs           : Dict[str, str] = {},
            ):
            """
            Initializes a new task with the given parameters.

            Parameters:
            - name (str): The name of the task.
            - image (Union[str, Image]): The image associated with the task, can be a string or an Image object.
            - command (str): The command to be executed for the task, must contain placeholders for input and output data.
            - input_data (Union[str, Dataset]): The input data for the task, can be a string representing the dataset name or a Dataset object.
            - outputs (Dict[str, str]): A dictionary mapping output names to their respective dataset names.
            - partition (str): The partition to which the task belongs.
            - secondary_data (Dict[str, Union[str, Dataset]], optional): A dictionary of secondary data for the task, defaults to an empty dictionary.
            - binds (Dict[str, str], optional): A dictionary of binds for the task, defaults to an empty dictionary.

            Raises:
            - ValueError: If the command does not contain the required placeholders for input, output, or secondary data.
            - Exception: If the input dataset or image is not found in the context, or if a task with the same name already exists.
            """
            
            self.name = name
            self.command = command
            self.envs = envs

            if '%IN' not in command:
                raise ValueError("command must contain the placeholder %IN for input data.")
            for key in outputs.keys():
                if f"%{key}" not in command:
                    raise ValueError(f"command must contain the placeholder %{key} for output data.")
            for key in secondary_data.keys():
                if f"%{key}" not in command:
                    raise ValueError(f"command must contain the placeholder %{key} for secondary data.")

            ctx = get_context()

            if type(input_data) == str:
                logger.info(f"Task {self.name}: looking for input dataset '{input_data}'.")
                if input_data not in ctx.datasets:
                    DatasetNotFound(input_data)
                input_data = ctx.datasets[input_data]
            
            if image and (type(image) == str):
                logger.info(f"Task {self.name}: looking for image '{image}'.")
                if image not in ctx.images:
                    raise ImageNotFound(image)
                image = ctx.images[image]
            
            self.image = image

            if self.name in ctx.tasks:
                raise TaskExistsError(self.name)
            
            self.task_id = len(ctx.tasks)
            ctx.tasks[self.name] = self   
            self.input_data = input_data            
            self.partition = partition
            self.binds = binds
            self._next = []
            self._prev = []
            
            for key in outputs.keys():
                if type(outputs[key]) == str:
                    logger.info(f"Task {self.name}: creating output dataset '{outputs[key]}'.")
                    name = f"{self.name}.{outputs[key]}"
                    output_data = Dataset(name=name, 
                                           path=f"{ctx.path}/datasets/{name}", from_task=self)
                    outputs[key] = output_data
                    
            for key in secondary_data.keys():
                if type(secondary_data[key]) == str:
                    logger.info(f"Task {self.name}: looking for secondary dataset '{secondary_data[key]}'.")
                    if secondary_data[key] not in ctx.datasets:
                        raise DatasetNotFound(secondary_data[key])
                    else:
                        secondary_data[key] = ctx.datasets[secondary_data[key]]

                secondary = secondary_data[key]
                if secondary.from_task:
                    secondary.from_task.next += [self]
                    self._prev += [secondary.from_task]
            
            if self.input_data.from_task:
                self.input_data.from_task.next += [self]
                self._prev += [self.input_data.from_task]
            
            self.outputs_data = outputs
            self.secondary_data = secondary_data
            self.path = f"{ctx.path}/tasks/{self.name}"
            
            self.jobs   = []
            if os.path.exists(f"{self.path}/jobs/inputs"):
                for job_path in expand_folders(f"{self.path}/jobs/inputs/*"):
                    logger.info(f"Task {self.name}: loading existing job from {job_path}.")
                    with open( job_path , 'r') as f:
                        job = Job.from_dict(json.load(f))
                        self.jobs.append(job)
                    
    @property
    def next(self) -> List['Task']:
        return self._next
    
    @property
    def prev(self) -> List['Task']:
        return self._prev
    
    @next.setter 
    def next( self, tasks : Union['Task' , List['Task']] ):
        if type(tasks) != list:
            tasks = [ tasks ]
        for task in tasks:
            if task and (task not in self._next):
                self._next.append( task )
         
    @prev.setter
    def prev( self, tasks : Union['Task' , List['Task']] ):
        if type(tasks) != list:
            tasks = [ tasks ]
        for task in tasks:
            if task and (task not in self._prev):
                self._prev.append( task )
                       

    def mkdir(self):
            """
            Create a directory structure for the task.

            This method creates a main directory for the task at the specified
            base path, along with subdirectories for 'works', 'jobs', and 
            'scripts'. If the directories already exist, they will not be 
            created again.

            Args:
                basepath (str): The base path where the task directory will be created.
            """
            
            os.makedirs(self.path + "/works"       , exist_ok=True)
            os.makedirs(self.path + "/jobs/inputs" , exist_ok=True)
            os.makedirs(self.path + "/jobs/status" , exist_ok=True)
            os.makedirs(self.path + "/scripts"  , exist_ok=True)
            self._create_status()
            self._update_jobs()   


    
    def output(self, key: str) -> str:
            """
            Generate the output file path based on the provided key.

            This method constructs a string that represents the output file path
            by combining the name of the current instance with the file name
            associated with the given key in the outputs_data dictionary.

            Args:
                key (str): The key used to retrieve the file name from outputs_data.

            Returns:
                str: The constructed output file path in the format 'name.file'.
            """
            return self.outputs_data[key].name
    

    def submit(self) -> int:
            """
            Submits a job to the job scheduler.

            This method performs the following steps:
            1. Retrieves the current context and database service.
            2. Updates the database with the current task information.
            3. Constructs a script to run the task using sbatch.
            4. Activates the virtual environment.
            5. Prepares the njob command with necessary parameters.
            6. Submits the job and returns the job ID.

            Returns:
                int: The ID of the submitted job.
            """
            
            ctx = get_context()
            self._update_jobs()   
            script = sbatch( f"{self.path}/scripts/run_task_{self.task_id}.sh", 
                            {
                                "ARRAY"         : ",".join( [str(job_id) for job_id in self.get_array_of_jobs_with_status() ]),
                                "OUTPUT_FILE"   : f"{self.path}/works/job_%a/output.out",
                                "ERROR_FILE"    : f"{self.path}/works/job_%a/output.err",
                                "PARTITION"     : self.partition,
                                "JOB_NAME"      : f"run-{self.task_id}",
                            })
            script += f"source {ctx.virtualenv}/bin/activate"
            command = f"maestro run job"
            command+= f" -i {self.path}/jobs/job_$SLURM_ARRAY_TASK_ID.json"
            command+= f" -o {self.path}/works/job_$SLURM_ARRAY_TASK_ID"
            command+= f" -d {ctx.path}/db/data.db"
            script += command
            job_id = script.submit() 
            return int(job_id)
 
 
    def to_dict(self) -> Dict:
            """
            Converts the current task instance into a raw dictionary representation.

            This method gathers various attributes of the task, including its name,
            image path, command, input data, outputs, partition, secondary data,
            binds, and references to next and previous tasks. The resulting dictionary
            can be used for serialization or other purposes where a raw representation
            of the task is needed.

            Returns:
                dict: A dictionary containing the raw representation of the task.
            """
            
            d = {
                "task_id"           : self.task_id,
                "name"              : self.name,
                "image"             : self.image.name if self.image else self.image,
                "command"           : self.command,
                "input_data"        : self.input_data.name,
                "outputs"           : { key : value.name.replace(self.name+'.',"") for key, value in self.outputs_data.items() },
                "partition"         : self.partition,
                "secondary_data"    : { key : value.name for key, value in self.secondary_data.items() },
                "binds"             : self.binds,
                "envs"              : self.envs,
                "next"              : [ task.name for task in self._next ],
                "prev"              : [ task.name for task in self._prev ],
            }
            return d

    @classmethod
    def from_dict( cls, data : Dict) -> 'Task':
        return cls(
            name           = data["name"],
            image          = data["image"],
            command        = data["command"],
            input_data     = data["input_data"],
            outputs        = data["outputs"],
            partition      = data["partition"],
            secondary_data = data["secondary_data"],
            binds          = data["binds"],
            envs           = data["envs"],
        )
        
    def _create_status(self):
        with open( self.path + "/status.json", 'w') as f:
            json.dump( Status(State.ASSIGNED).to_dict() , f , indent=2)
        
    def _update_jobs(self):
            
            input_files  = [ job.input_file.split('/')[-1] for job in self.jobs ]
            job_id = len(input_files)
            for filepath in self.input_data:
                filename = filepath.split('/')[-1]
                if not filename in input_files:
                    logger.info(f"Task {self.name}: preparing job {job_id} for input file {filename}.")
                    outputs = {key: (value.name.replace(f"{self.name}.",""),value) for key, value in self.outputs_data.items()}
                    job = Job(
                        task_path = self.path,
                        job_id = job_id,
                        input_file = filepath,
                        outputs = outputs,
                        secondary_data = self.secondary_data,
                        image = self.image,
                        command = self.command,
                        binds = self.binds,
                        envs = self.envs,
                    )
                    job.dump()
                    self.jobs.append( job )
                    input_files.append( filename )
                    job_id += 1
                    
                    
    def get_array_of_jobs_with_status(self, status: State=State.ASSIGNED) -> List[int]:
        return [ job.job_id for job in self.jobs if job.status == status ]
        
        
    @property 
    def status(self) -> State:
        if os.path.exists( f"{self.path}/status.json" ):
            with FileLock( f"{self.path}/status.json.lock" ):
                with open( f"{self.path}/status.json", 'r') as f:
                    data = json.load(f)
                    return Status.from_dict(data).status
        else:
            return State.UNKNOWN
    
    @status.setter
    def status(self, new_status: State):
        with FileLock( f"{self.path}/status.json.lock" ):
            with open( f"{self.path}/status.json", 'r') as f:
                data = json.load(f)
                status = Status.from_dict(data)
            status.status=new_status
            with open( f"{self.path}/status.json", 'w') as f:
                json.dump( status.to_dict() , f , indent=2)
 
    def count(self) -> Dict[str, int]:
            """
            Counts the number of jobs in each status.

            This method iterates through the jobs associated with the current instance
            and counts how many jobs are in each possible status defined in `job_status`.
            
            Returns:
                Dict[str, int]: A dictionary where the keys are job statuses and the values
                are the counts of jobs in each status.
            """   
            status_count = { state : 0 for state in job_status }
            for job in self.jobs:
                status_count[job.status.value] += 1
            return status_count
