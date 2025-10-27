__all__ = [
    "Job",
    "Status",
    "State",
]

import os
import json

from typing import Dict
from enum import Enum
from filelock import FileLock
from datetime import datetime, timedelta
from maestro_lightning.models import get_context
from maestro_lightning.models.dataset import Dataset
from maestro_lightning.models.image import Image



class State(Enum):
    ASSIGNED = "assigned"
    UNKNOWN  = "unknown"
    PENDING  = "pending"
    RUNNING  = "running"
    COMPLETED= "completed"
    FAILED   = "failed"
    FINALIZED= "finalized"
    
    
class Status:
    def __init__(self, 
                 status: str, 
                 start_time : datetime=datetime.now(),
                 last_time  : datetime=datetime.now()
    ):
        self.status = status
        self.start_time = start_time
        self.last_time = last_time
    
    def to_dict(self) -> Dict:
        return {
            "status"     : self.status,
            "last_time"  : self.last_time.isoformat(),
            "start_time" : self.start_time.isoformat(),
        }
        
    @classmethod
    def from_dict(cls, data: Dict):
        return cls(
            status = data["status"],
            start_time = datetime.fromisoformat(data["start_time"]),
            last_time = datetime.fromisoformat(data["last_time"])
        )
        
    def ping(self):
        self.time = datetime.now()
        
    def is_alive(self, minutes : int=5) -> bool:
        return datetime.now() - self.last_time < timedelta(minutes=minutes)
      
    def reset(self):
        self.start_time = datetime.now()
        self.last_time = datetime.now()





class Job:
    def __init__(self, 
                 task_path: str,
                 job_id: int,
                 input_file: str,
                 outputs: dict,
                 secondary_data: dict,
                 image: Image,
                 command: str,
                 binds: Dict[str, str]={},
                 envs: Dict[str, str]={}
                 ):
        
        self.task_path = task_path
        self.job_id = job_id
        self.input_file = input_file
        self.outputs = outputs
        self.secondary_data = secondary_data
        self.image = image
        self.command = command
        self.binds = binds
        self.envs = envs
        
    def to_dict(self) -> Dict:
        """
        Convert the Job instance to a dictionary representation.

        This method serializes the attributes of the Job instance into a 
        dictionary format, which can be useful for JSON serialization or 
        other forms of data interchange. The dictionary includes the 
        following keys:

        - task_path: The path to the task associated with the job.
        - job_id: The unique identifier for the job.
        - status: The current status of the job.
        - input_file: The input file associated with the job.
        - outputs: A dictionary of output data, where each key is an 
            identifier and each value is the serialized representation of 
            the corresponding output.
        - secondary_data: A dictionary of secondary data, serialized 
            in the same manner as outputs.
        - image: The serialized representation of the associated image.
        - command: The command associated with the job.
        - binds: The binds associated with the job.

        Returns:
                Dict: A dictionary representation of the Job instance.
        """
        return {
                "task_path"      : self.task_path,
                "job_id"         : self.job_id,
                "status"         : self.status,
                "input_file"     : self.input_file,
                "outputs"        : { key : value.to_dict() for key, value in self.outputs.items() },
                "secondary_data" : { key : value.to_dict() for key, value in self.secondary_data.items() },
                "image"          : self.image.to_dict(),
                "command"        : self.command,
                "binds"          : self.binds,
                "envs"           : self.envs,
        }
        
    @classmethod
    def from_dict(cls, data: Dict):
        
        ctx = get_context()
        outputs = data["outputs"]
        for key in outputs.keys():
            if outputs[key]["name"] not in ctx.datasets:
                dataset = Dataset.from_dict( outputs[key] )
                outputs[key]=dataset
            else:
                outputs[key]=ctx.datasets[ outputs[key]["name"] ]
                
        secondary_data = data["secondary_data"]
        for key in secondary_data.keys():
            if secondary_data[key]["name"] not in ctx.datasets:
                dataset = Dataset.from_dict( secondary_data[key] )
                secondary_data[key]=dataset
            else:
                secondary_data[key]=ctx.datasets[ secondary_data[key]["name"] ]
        
        image = data["image"]
        if image["name"] not in ctx.images:
            image = Image.from_dict( data["image"] )
        else:
            image = ctx.images[ image["name"] ]
        
        return cls(
            task_path      = data["task_path"],
            job_id         = data["job_id"],
            input_file     = data["input_file"],
            outputs        = outputs,
            secondary_data = secondary_data,
            image          = image,
            command        = data["command"],
            binds          = data["binds"],
            envs           = data["envs"],
        )
             
    def dump(self):
            """
            Dumps the job's data to JSON files.
            This method creates two JSON files:
            
            1. A file containing the job's input data, saved at 
                'jobs/inputs/job_{job_id}.json'.
            2. A file containing the job's status, saved at 
                'jobs/status/job_{job_id}.json'.
            
            The job's input data is obtained by calling the `to_dict` method,
            while the status is represented by an instance of the `Status` class
            initialized with the `ASSIGNED` status.
            Note: Ensure that the directories exist before calling this method.
            """
            with open( f"{self.task_path}/jobs/inputs/job_{self.job_id}.json", 'w') as f:
                  json.dump( self.to_dict() , f , indent=2)
            with open( f"{self.task_path}/jobs/status/job_{self.job_id}.json", 'w') as f:
                  json.dump(Status(State.ASSIGNED).to_dict(), f, indent=2)
    

    @property 
    def status(self) -> str:
        if os.path.exists( f"{self.task_path}/jobs/status/job_{self.job_id}.json" ):
            with FileLock( f"{self.task_path}/jobs/status/job_{self.job_id}.json.lock" ):
                with open( f"{self.task_path}/jobs/status/job_{self.job_id}.json", 'r') as f:
                    data = json.load(f)
                    return Status.from_dict(data).status
        else:
            return State.UNKNOWN
    
    @status.setter
    def status(self, new_status: str):
        status = Status(new_status)
        with FileLock( f"{self.task_path}/jobs/status/job_{self.job_id}.json.lock" ):
            with open( f"{self.task_path}/jobs/status/job_{self.job_id}.json", 'w') as f:
                json.dump(status.to_dict(), f, indent=2)
                    
                   
    def ping(self):
        if os.path.exists( f"{self.task_path}/jobs/status/job_{self.job_id}.json" ):
            with FileLock( f"{self.task_path}/jobs/status/job_{self.job_id}.json.lock" ):
                with open( f"{self.task_path}/jobs/status/job_{self.job_id}.json", 'r') as f:
                    status = Status.from_dict(json.load(f))
                    status.ping()
                with open( f"{self.task_path}/jobs/status/job_{self.job_id}.json", 'w') as f:
                    json.dump(status.to_dict(), f, indent=2)
                    
    def is_alive(self) -> bool:
        if os.path.exists( f"{self.task_path}/jobs/status/job_{self.job_id}.json" ):
            with FileLock( f"{self.task_path}/jobs/status/job_{self.job_id}.json.lock" ):
                with open( f"{self.task_path}/jobs/status/job_{self.job_id}.json", 'r') as f:
                    status = Status.from_dict(json.load(f))
                    return status.is_alive()
        else:
            return False
        
    def reset(self):
        if os.path.exists( f"{self.task_path}/jobs/status/job_{self.job_id}.json" ):
            with FileLock( f"{self.task_path}/jobs/status/job_{self.job_id}.json.lock" ):
                with open( f"{self.task_path}/jobs/status/job_{self.job_id}.json", 'r') as f:
                    status = Status.from_dict(json.load(f))
                    status.reset()
                with open( f"{self.task_path}/jobs/status/job_{self.job_id}.json", 'w') as f:
                    json.dump(status.to_dict(), f, indent=2)