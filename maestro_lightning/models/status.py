__all__ = ["State", "Status"]

from enum import Enum
from typing import Dict
from datetime import datetime, timedelta


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
                 status: State, 
                 start_time : datetime=datetime.now(),
                 last_time  : datetime=datetime.now()
    ):
        self.status = status
        self.start_time = start_time
        self.last_time = last_time
    
    def to_dict(self) -> Dict:
        return {
            "status"     : self.status.value,
            "last_time"  : self.last_time.isoformat(),
            "start_time" : self.start_time.isoformat(),
        }
        
    @classmethod
    def from_dict(cls, data: Dict):
        return cls(
            status = State(data["status"]),
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




