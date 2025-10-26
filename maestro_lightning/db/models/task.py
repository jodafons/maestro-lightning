__all__ = [
    "Task", 
    "DBTask", 
    "TaskStatus",
    ]

import enum 
from datetime import datetime
from typing import List, Dict
from sqlalchemy import Column, Integer, String, Enum, Float, DateTime, TEXT
from sqlalchemy.orm import load_only, relationship
from . import Base
from .job  import job_status


minutes=60 # seconds



#
# tasks and jobs
#
class TaskStatus(enum.Enum):

    ASSIGNED       = "assigned"
    RUNNING        = "running"
    COMPLETED      = "completed"
    FINALIZED      = "finalized"
    FAILED         = "failed"
    CANCELED       = "canceled"
    


    

class Task (Base):

    __tablename__    = 'task'
    id               = Column(Integer, primary_key=True)
    task_id          = Column(Integer)
    name             = Column(String, unique=True)
    jobs             = relationship("Job", order_by="Job.id", back_populates="task")
    status           = Column(Enum(TaskStatus), default=TaskStatus.ASSIGNED )
    start_time       = Column(DateTime, default=datetime.now())
    updated_time     = Column(DateTime, default=datetime.now())

    def __add__ (self, exp):
      self.jobs.append(exp)
      return self
    
    def ping(self):
        self.updated_time = datetime.now()

    
class DBTask:

    def __init__(self, name : str, session):
      self.name = name
      self.__session = session

    def fetch_status(self) -> TaskStatus:
        session = self.__session()
        try:
            fields = [Task.status]
            task = (
                session.query(Task)
                .filter_by(name=self.name)
                .options(load_only(*fields))
                .one()
            )
            return task.status
        finally:
            session.close()
     
    def update_status(self, status : TaskStatus):
        session = self.__session()
        try:
            task = session.query(Task).filter_by(name=self.name).one()
            setattr(task, "status", status)
            task.ping()
            session.commit()
        finally:
            session.close()
     
    def check_existence(self) -> bool:
        session = self.__session()
        try:
            count = session.query(Task).filter_by(name=self.name).count()
            return count > 0
        finally:
            session.close()
        
        
    def fetch_summary(self) -> Dict[str,Dict[str,int]]:
        session = self.__session()
        try:
           task_db = session.query(Task).filter_by(name=self.name).one()
           table = {status.value:0 for status in job_status}
           for job_db in task_db.jobs:
               table[job_db.status.value] += 1           
           return {'status':task_db.status.value, 'summary':table}
        finally:
            session.close()