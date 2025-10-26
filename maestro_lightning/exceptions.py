
class RuntimeError(Exception):
    """Base class for errors raised by Qiskit."""

    def __init__(self, *message):
        """Set the error message."""
        super().__init__(" ".join(message))
        self.message = " ".join(message)

    def __str__(self):
        """Return the message."""
        return repr(self.message)

class DatasetError(RuntimeError):
    """Raised when an error"""

    message = "its not possible to create a session. please set the remote first."

class ImageError(RuntimeError):
    """Raised when an error"""

    message = "the server connection is not found."

class TaskError(RuntimeError):
    """Raised when an error"""

    message = "the token is not valid."




class Session:

    def __init__(self, path: str):
        self.path = path
    
    def run(self, dry_run : bool=False):

        global __tasks__, __datasets__, __images__ 
        os.makedirs(self.path + "/tasks", exist_ok=True)
        os.makedirs(self.path + "/datasets", exist_ok=True)
        os.makedirs(self.path + "/images", exist_ok=True)
        os.makedirs(self.path + "/scripts", exist_ok=True)
        os.makedirs(self.path + "/db", exist_ok=True)
        db_path = f"{self.path}/db/local.db"

        for dataset in __datasets__.values():
            dataset.mkdir( basepath = f"{self.path}/datasets" )
        for image in __images__.values():
            image.mkdir( basepath = f"{self.path}/images" )


        #if not os.path.exists(db_path):
        #recreate_db(db_path)
        #db_service = get_db_service(db_path)


        datasets = {
            'inputs'   : {},
            'outputs'  : {},
        }
        
        # NOTE: stage 1, collect all datasets (inputs/outputs)
        for task in __tasks__.values():
            for key, value in task.secondary_data.items():
                if not value in datasets["inputs"]:
                    datasets['inputs'][value]=[task.name]
                else:
                    datasets["inputs"].append(task.name)

            if task.input:
                if not task.input in datasets["inputs"]:
                    datasets['inputs'][task.input]=[task.name]
                else:
                    datasets["inputs"][task.input].append(task.name)
                    
            for key, value in task.outputs.items():
                name = f"{task.name}.{value}"
                if not name in datasets["outputs"]:
                    datasets["outputs"][name] = [task.name]
                else:
                    datasets["outputs"].append(task.name)

        pprint(datasets)

        logger.info("validating all tasks inside of this group")
        for task in __tasks__.values():

            logger.info(f"validating task with name {task.name}...")

            if task.image.name not in __images__.keys():
                reason=f"image with name {task.image} does not exist into the database or is not expected inside of this group of tasks."
                RuntimeError(reason)

            # NOTE: stage 3, checking input dataset
            if (task.input!="") and ( (task.input not in datasets["outputs"]) and  ( task.input not in __datasets__.keys() ) ):
                reason=f"input dataset with name {task.input} does not exist into the database or is not expected inside of this group of tasks as an output dataset."
                RuntimeError(reason)
            
            # NOTE: stage 4, checking extra inputs
            for key, value in task.secondary_data.items():
                if not f"%{key}" in task.command:
                    reason=f"you should have %{key} as extra input inside of the command."
                    RuntimeError(reason)

                if (value not in __datasets__.keys()) and (not value in datasets['outputs']):
                    reason=f"extra dataset with name {value} does not exist into the database or is not expected inside of this group of tasks as an output dataset."
                    RuntimeError(reason)
                        
            # NOTE: stage 5, check input dataset
            if len(task.outputs)==0:
                reason=f"you should have at least one output for this task"
                RuntimeError(reason)
            
            # NOTE: stage 6, checkout outputs
            logger.info("checking outputs...")
            for key, value in task.outputs.items():
                # format? outputs = {"OUT_RESULT":"result.pkl", "OUT_CIRCUIT":"circuit.json"}
                if not f"%{key}" in task.command:
                    reason=f"you should have %{key} inside of the command."
                    RuntimeError(reason)

                name = f"{task.name}.{value}"
                if (name in __datasets__.values()) and ( name in datasets["outputs"] and len(datasets["outputs"][name])!=1 ):
                    reason =f"output dataset with name {name} exist into the database or another task inside of this group uses the same name as output."
                    reason+=f"you should change the name of the task before launch a new one."
                    RuntimeError(reason)
                # create the output dataset
                Dataset( name, f"{self.path}/datasets/{name}" ).mkdir()


        logger.info("all tasks have been validated successfully.")
        # LOOP: end of tasks
            
        logger.info("creating all tasks")
        for task in __tasks__.values():
            #task_id           = random_id()
            task_db           = models.Task()
            #task_db.task_id   = task_id
            task_db.name      = task.name
            task_db.partition = task.partition
            print(task.command)
            parent_tasks      = []
            if task.input in datasets["outputs"]:
                parent_tasks.extend( datasets["outputs"][task.input] )
                
            for dataset in task.secondary_data.values():
                if dataset in datasets["outputs"]:
                    parent_tasks.extend(datasets['outputs'][dataset])                
            print(parent_tasks)
            parent_tasks       = list(set(parent_tasks)) # remove duplicates
            task_db.parents    = parent_tasks

            #db_service.save_task(task_db)


        logger.info("all tasks have been created successfully.")