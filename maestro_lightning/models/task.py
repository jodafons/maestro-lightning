

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
    "load",
    "dump",
]

import os, json

from typing                  import Union, Dict, List
from novacula.models         import get_context, Context
from novacula.models.image   import Image 
from novacula.models.dataset import Dataset
from novacula.db             import get_db_service, models
from novacula                import sbatch
from loguru                  import logger

# ... rest of the Task class code ...
__all__ = [
    "Task",
    "load",
    "dump",
]

import os, json

from typing                  import Union, Dict, List
from novacula.models         import get_context, Context
from novacula.models.image   import Image 
from novacula.models.dataset import Dataset
from novacula.db             import get_db_service, models
from novacula                import sbatch
from loguru                  import logger


class Task:
    
    def __init__(self,
                     name           : str,
                     image          : Union[str, Image],
                     command        : str,
                     input_data     : Union[str, Dataset],
                     outputs        : Dict[str, str],
                     partition      : str,
                     secondary_data : Dict[str, Union[str, Dataset]] = {},
                     binds          : Dict[str, str] = {},
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
                if input_data not in ctx.datasets:
                    Exception(f"input dataset {input_data} not found in the group of tasks.")
                input_data = ctx.datasets[input_data]
            
            if type(image) == str:
                if image not in ctx.images:
                    Exception(f"image {image} not found in the group of tasks.")
                image = ctx.images[image]
            
            self.image = image

            if self.name in ctx.tasks:
                raise Exception(f"a task with name {name} already exists inside of this group of tasks.")
            
            self.task_id = len(ctx.tasks)
            ctx.tasks[self.name] = self   
            self.input_data = input_data            
            self.partition = partition
            self.binds = binds
            self._next = []
            self._prev = []
            
            for key in outputs.keys():
                if type(outputs[key]) == str:
                    name = f"{self.name}.{outputs[key]}"
                    output_data = Dataset(name=name, 
                                           path=f"{ctx.path}/datasets/{name}", from_task=self)
                    outputs[key] = output_data
                    
            for key in secondary_data.keys():
                if type(secondary_data[key]) == str:
                    if secondary_data[key] not in ctx.datasets:
                        Exception(f"secondary dataset {secondary[key]} not found in the group of tasks.")
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
            
            os.makedirs(self.path + "/works"    , exist_ok=True)
            os.makedirs(self.path + "/jobs"     , exist_ok=True)
            os.makedirs(self.path + "/scripts"  , exist_ok=True)
            self._create_db()

    
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
            db_service = get_db_service()
            self._update_db()   
            script = sbatch( f"{self.path}/scripts/run_task_{self.task_id}.sh", 
                            args = {
                                "array"     : ",".join( [str(job_id) for job_id in self.get_array_of_jobs_with_status() ]),
                                "output"    : f"{self.path}/works/job_%a/output.out",
                                "error"     : f"{self.path}/works/job_%a/output.err",
                                "partition" : self.partition,
                                "job-name"  : f"run-{self.task_id}",
                            })
            script += f"source {ctx.virtualenv}/bin/activate"
            command = f"njob "
            command+= f" -i {self.path}/jobs/job_$SLURM_ARRAY_TASK_ID.json"
            command+= f" -o {self.path}/works/job_$SLURM_ARRAY_TASK_ID"
            command+= f" -d {ctx.path}/db/data.db"
            script += command
            job_id = script.submit() 
            return int(job_id)
 
 
    def to_raw(self) -> Dict:
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
                "image"             : self.image.name,
                "command"           : self.command,
                "input_data"        : self.input_data.name,
                "outputs"           : { key : value.name.replace(self.name+'.',"") for key, value in self.outputs_data.items() },
                "partition"         : self.partition,
                "secondary_data"    : { key : value.name for key, value in self.secondary_data.items() },
                "binds"             : self.binds,
                "next"              : [ task.name for task in self._next ],
                "prev"              : [ task.name for task in self._prev ],
            }
            return d

    @classmethod
    def from_raw( cls, data : Dict) -> 'Task':
        
        task = Task(
            name = data['name'],
            image = data['image'],
            command = data['command'],
            input_data = data['input_data'],
            outputs = { key : value for key, value in data['outputs'].items() },
            partition = data['partition'],
            secondary_data = { key : value for key, value in data['secondary_data'].items() },
            binds = data['binds'],
        )
        
    #
    # database methods
    #
        
    def _create_db(self):
            """
            Creates a new task in the database if it does not already exist.

            This method checks for the existence of a task with the given name.
            If the task does not exist, it creates a new Task object, adds it to
            the session, and commits the transaction.

            Note: The task_id assignment is currently commented out and may need
            to be implemented based on the application's requirements.
            """
            
            db_service = get_db_service()
            if not db_service.task(self.name).check_existence():
                with db_service() as session:
                    task_db = models.Task()
                    task_db.task_id = self.task_id
                    task_db.name = self.name
                    session.add(task_db)
                    session.commit()

    def _update_db(self):
            """
            Updates the database with new job entries associated with the current task.

            This method retrieves the task from the database using its name, checks for existing jobs,
            and creates new job entries based on the input data provided. Each job is saved as a JSON file
            in a specified directory, and the job information is stored in the database.

            The following steps are performed:
            1. Retrieve the database session.
            2. Query the task by name.
            3. Iterate over the input data to create job entries.
            4. Check if a job with the same filename already exists.
            5. If not, create a new job entry and save it to the database and as a JSON file.
            6. Commit the changes to the database.

            Note: This method assumes that the `self.input_data`, `self.outputs_data`, `self.image`, 
            `self.path`, `self.command`, and `self.binds` attributes are properly initialized before calling 
            this method.
            """
            
            db_service = get_db_service()
     
            with db_service() as session:
                try:
                 
                    task_db = session.query(models.Task).filter_by(name=self.name).one()    
                    job_id = len(task_db.jobs)
                        
                    for filepath in self.input_data:
                        filename = filepath.split('/')[-1]
                        if session.query(models.Job).filter_by(task_name=self.name, filename=filename).count() == 0:
                                                
                            path = f"{self.path}/jobs/job_{job_id}.json"
                            with open( path, 'w') as f:
                                d = {
                                    "input_data"    : filepath,
                                    "outputs"       : { key : {"name":value.name.replace(f"{self.name}.",""), "target":value.path} for key, value in self.outputs_data.items() },
                                    "secondary_data": {},
                                    "image"         : self.image.path,
                                    "job_id"        : job_id,
                                    "task_id"       : self.task_id,
                                    "command"       : self.command,
                                    "binds"         : self.binds,
                                    "job_name"      : "",
                                    "task_name"     : self.name,
                                }
                                json.dump(d, f, indent=2)

                            job_db          = models.Job()
                            job_db.job_id   = job_id 
                            job_db.task_name= self.name
                            job_db.filename = filename
                            job_db.status   = models.JobStatus.ASSIGNED
                            task_db        += job_db 
                            job_id         += 1
                            
                    logger.info(f"creating task with name {self.name}")
                    session.commit()
                finally:
                    session.close()
            
    def get_array_of_jobs_with_status(self, status: models.JobStatus = models.JobStatus.ASSIGNED) -> List[int]:
            """
            Retrieve an array of job IDs with a specified status.

            This method queries the database for jobs associated with the current task name
            that match the given status. It returns a list of job IDs.

            Args:
                status (models.JobStatus): The status of the jobs to retrieve. Defaults to
                models.JobStatus.ASSIGNED.

            Returns:
                List[int]: A list of job IDs that match the specified status.

            Note:
                Ensure that the database service is properly configured and accessible.
            """
            
            db_service = get_db_service()
            jobs = []
            with db_service() as session:
                try:
                    task_db = session.query(models.Task).filter_by(name=self.name).one()
                    for job_db in task_db.jobs:
                        if job_db.status == status:
                            jobs.append(job_db.job_id)
                    
                finally:
                    session.close()
            return jobs
        
#
# read and write functions
#
        
def dump( ctx : Context, path : str):
    
    with open(path, 'w') as f:
        d = {
            "datasets":{},
            "images":{},
            "tasks":{},
            "path":ctx.path,
            "virtualenv": ctx.virtualenv
        }
        # step 1: dump all datasets which are not from tasks
        for dataset in ctx.datasets.values():
            if not dataset.from_task:
                d['datasets'][ dataset.name ] = dataset.to_raw()
        # step 2: dump all images
        for images in ctx.images.values():
            d['images'][ images.name ] = images.to_raw()
        
        # step 3: dump all tasks
        for task in ctx.tasks.values():
            d[ 'tasks' ][ task.task_id ] = task.to_raw()
        json.dump( d , f , indent=2 )

      
    
def load( path : str, ctx : Context):
    
    with open( path , 'r') as f:
        data = json.load(f)
        ctx.path = data['path']
        ctx.virtualenv = data['virtualenv']
        
        # step 1: load all datasets which are not from tasks
        for dataset in data['datasets'].values():
            Dataset.from_raw( dataset )
        
        # step 2: load all images
        for image in data['images'].values():
            Image.from_raw( image )

        # step 3: load all tasks
        for task_id, raw in data['tasks'].items():
            Task.from_raw( raw )
            
    