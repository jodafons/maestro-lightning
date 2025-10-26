__all__ = [
    "Job" , 
    "DBJob", 
    "JobStatus",
    "job_status"
    ]

import enum 

from datetime import datetime
from sqlalchemy.orm import load_only, relationship
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Float, Enum

from . import Base


minutes=60 # seconds




class JobStatus(enum.Enum):

    ASSIGNED    = "assigned"
    PENDING     = "pending"
    RUNNING     = "running"
    COMPLETED   = "completed"
    FAILED      = "failed"
    KILL        = "kill"
    KILLED      = "killed"
    
job_status = [JobStatus.ASSIGNED, JobStatus.PENDING, JobStatus.RUNNING, JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.KILL, JobStatus.KILLED]

class Job (Base):

    __tablename__       = 'job'
    id                  = Column(Integer, primary_key=True)
    job_id              = Column(Integer)
    retry               = Column(Integer , default=-1)
    task                = relationship("Task", back_populates="jobs")
    taskid              = Column(Integer, ForeignKey('task.id'))
    status              = Column(Enum(JobStatus), default=JobStatus.ASSIGNED )
    start_time          = Column(DateTime, default=datetime.now())
    updated_time        = Column(DateTime, default=datetime.now())
    task_name           = Column(String)
    filename            = Column(String)

    
    def ping(self):
        self.updated_time = datetime.now()
        
    def is_alive(self, timeout=5*minutes):
        return (datetime.now() - self.updated_time).total_seconds() < timeout



class DBJob:

    def __init__(self, task_name : int, job_id : int, session):
        self.job_id = job_id
        self.task_name = task_name
        self.__session = session

    def update_status(self, status : JobStatus):
        session = self.__session()
        try:
            job = session.query(Job).filter_by(task_name=self.task_name).filter_by(job_id=self.job_id).one()
            setattr(job, "status", status)
            job.ping()
            session.commit()
        finally:
            session.close()

    def fetch_status(self) -> JobStatus:
        session = self.__session()
        try:
            fields = [Job.status]
            job = (
                session.query(Job)
                .filter_by(task_name=self.task_name)
                .filter_by(job_id=self.job_id)
                .options(load_only(*fields))
                .one()
            )
            return job.status
        finally:
            session.close()

    def ping(self):
        session = self.__session()
        try:
            job = session.query(Job).filter_by(task_name=self.task_name).filter_by(job_id=self.job_id).one()
            job.ping()
            session.commit()
        finally:
            session.close()
            
    def start(self):
        session = self.__session()
        try:
            print(self.task_name, self.job_id)
            job = session.query(Job).filter_by(task_name=self.task_name).filter_by(job_id=self.job_id).one()
            job.start_time = datetime.now()
            job.retry+=1
            session.commit()
        finally:
            session.close()