__all__ = ["Popen"]


import nvsmi
import psutil
import traceback
import threading
import subprocess

from loguru import logger
from time   import sleep, time



def get_gpu_processes():
    try:
        return nvsmi.get_gpu_processes()
    except:
        return []
      

class Monitor(threading.Thread):
   
    def __init__(self, process , max_retry : int=5):
      threading.Thread.__init__(self)
      self.__proc     = process
      retry=0
      self.proc_stat=None
      while retry<max_retry:
        try:
          self.proc_stat = psutil.Process(process.pid)
          break
        except:
           sleep(0.01)
           self.proc_stat=None
           retry+=1

      self.__lock     = threading.Lock()
      self.__cpu_percent_avg     = 0
      self.__cpu_percent_peak    = 0
      self.__gpu_memory_mb_avg   = 0
      self.__gpu_memory_mb_peak  = 0
      self.__sys_memory_mb_avg   = 0
      self.__sys_memory_mb_peak  = 0
      self.__exec_time           = 0
 

    def run(self):
        self.start_time = time()
        loop_num = 1
        self.update(loop_num)
        def is_alive(proc) -> bool:
          return (True if (proc and proc.poll() is None) else False) if type(proc) == subprocess.Popen else proc.is_alive()
        # NOTE: only start the monitoring if we are able to get the process
        if self.__proc:
          while is_alive(self.__proc):
              self.__lock.acquire()
              self.update(loop_num)
              sleep(1)
              self.__lock.release()
        else:
           logger.warning("the constructor not be able to get the process from the reference pid. We will not be able to monitor it.")
        self.__exec_time = time() - self.start_time
        

    def update(self, loop_num):

        def get_average(curr_avg, new_sample, num) -> float:
            return (curr_avg * (num-1) + new_sample) / num

        try:
          children = self.proc_stat.children(recursive=True)
          # NOTE: if not children, use the parent for measurements...
          if len(children)==0:
            children=[self.proc_stat]
          else:
            children=[psutil.Process(child.pid) for child in children]

          gpu_children = get_gpu_processes()
          sys_used_memory_mb = 0; cpu_percent = 0; gpu_used_memory_mb = 0

          for child in children:
            sys_used_memory_mb += child.memory_info().rss/1024**2
            cpu_percent += child.cpu_percent()
            for gpu_child in gpu_children:
              gpu_used_memory_mb += (gpu_child.used_memory/1024**2) if gpu_child.pid==child.pid else 0

            self.__cpu_percent_avg     = get_average( self.__cpu_percent_avg   , cpu_percent       , loop_num)
            self.__sys_memory_mb_avg   = get_average( self.__sys_memory_mb_avg , sys_used_memory_mb, loop_num)
            self.__gpu_memory_mb_avg   = get_average( self.__gpu_memory_mb_avg , gpu_used_memory_mb, loop_num)
            self.__cpu_percent_peak    = cpu_percent if cpu_percent > self.__cpu_percent_peak else self.__cpu_percent_peak
            self.__sys_memory_mb_peak  = sys_used_memory_mb if sys_used_memory_mb > self.__sys_memory_mb_peak else self.__sys_memory_mb_peak
            self.__gpu_memory_mb_peak  = gpu_used_memory_mb if gpu_used_memory_mb > self.__gpu_memory_mb_peak else self.__gpu_memory_mb_peak
            self.__exec_time           = time() - self.start_time
        except:
          #traceback.print_exc()
          logger.debug("proc stat not available anymore.")


    def __call__(self):
        # NOTE: using lock to avoid access this region at same writting time in the monitor loop...
        self.__lock.acquire()
        metrics = {
            "exec_time"           : self.__exec_time            ,     
            "cpu_percent_avg"     : self.__cpu_percent_avg      ,
            "cpu_percent_peak"    : self.__cpu_percent_peak     ,
            "sys_memory_mb_avg"   : self.__sys_memory_mb_avg    ,
            "sys_memory_mb_peak"  : self.__sys_memory_mb_peak   ,
            "gpu_memory_mb_avg"   : self.__gpu_memory_mb_avg    ,
            "gpu_memory_mb_peak"  : self.__gpu_memory_mb_peak   ,
        }
        self.__lock.release()
        return metrics


class Popen:

  def __init__(self, 
               command       : str,
               envs          : dict={},
               ):

    self.command     = "sleep 2\n"+command
    self.__pending   = True
    self.__broken    = False
    self.__killed    = False
    self.env         = envs
    self.__proc      = None
    self.__proc_stat = None
    self.__mon_thread= None
 

  def run_async(self, verbose : bool=False):

    try:
      self.__killed=False
      self.__broken=False
      self.__proc = subprocess.Popen(self.command, env=self.env, shell=True)
      self.__mon_thread = Monitor(self.__proc)
      self.__mon_thread.start()
      self.__proc_stat = psutil.Process(self.__proc.pid)
      self.__pending=False
      broken = self.status() == "failed"
      self.__broken = broken
      return not broken # Lets considering the first seconds as broken

    except Exception as e:
      traceback.print_exc()
      logger.error(e)
      self.__broken=True
      return False
    

  @property
  def exitcode(self):
    if self.__proc:
        return self.__proc.returncode
    return None
 

  def metrics(self):
      if self.__mon_thread:
        return self.__mon_thread()
      else:
         metrics = {
            "exec_time"           : 0 ,     
            "cpu_percent_avg"     : 0 , 
            "cpu_percent_peak"    : 0 ,
            "sys_memory_mb_avg"   : 0 ,
            "sys_memory_mb_peak"  : 0 ,
            "gpu_memory_mb_avg"   : 0 ,
            "gpu_memory_mb_peak"  : 0 ,
        }
         return metrics
   
   
  def join(self):
    while self.is_alive():
      sleep(5)


  def is_alive(self):
    return True if (self.__proc and self.__proc.poll() is None) else False


  def kill(self):
    if self.is_alive() and self.__proc:
      children = self.__proc_stat.children(recursive=True)
      for child in children:
        p=psutil.Process(child.pid)
        p.kill()
      self.__proc.kill()
      self.__killed=True
    else:
      self.__killed=True


  def status(self):

    if self.is_alive():
      return "running"
    elif self.__pending:
      return "pending"
    elif self.__killed:
      return "killed"
    elif self.__broken:
      return "broken"
    elif (self.exitcode and  self.exitcode>0):
      return "failed"
    else:
      return "completed"