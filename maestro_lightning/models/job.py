
    job = Job(
                        task_path = self.path,
                        job_id = job_id,
                        status = JobStatus.ASSIGNED.value,
                        input_data = filepath,
                        outputs = { key : value for key, value in self.outputs_data.items() },
                        secondary_data = { key : value for key, value in self.secondary_data.items() },
                        image = self.image,
                        command = self.command,
                        binds = self.binds,
                    )

class Job:
    def __init__(self, 
                 task_path: str,
                 job_id: int,
                 status: str,
                 input_data: Dataset,
                 outputs: dict,
                 secondary_data: dict,
                 image: Image,
                 command: str,
                 binds: dict):
        
        self.task_path = task_path
        self.job_id = job_id
        self.status = status
        self.input_data = input_data
        self.outputs = outputs
        self.secondary_data = secondary_data
        self.image = image
        self.command = command
        self.binds = binds
        
    def update_status(self, new_status: str):
        self.status = new_status
                
    def dump(self):
        
                

    
    
    
    
     logger.info(f"Task {self.name}: preparing job {job_id} for input file {filename}.")
                    task['files'].append( filename )                                        
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
                        }
                        json.dump(d, f, indent=2)                        

                    logger.info(f"Task {self.name}: creating job database entry for job {job_id}.")
                    with open( self.path + f"/db/job_{job_id}.json", 'w') as f:
                        d = {
                            "status"    : JobStatus.ASSIGNED.value,
                            "timestamp" : datetime.now(),
                            "job_id"    : job_id,
                        }
                        json.dump( d , f , indent=2)
                    