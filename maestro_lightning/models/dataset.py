__all__ = [
    "Dataset",
]

import os

from typing import List, Dict, Union
from expand_folders import expand_folders
from novacula import symlink
from novacula.models import get_context

class Dataset:
    
    def __init__(self, 
                 name: str,
                 path: str,
                 from_task : Union[None, 'Task']=None,
                ):
            """
            Initializes a new dataset instance.

            Parameters:
            ----------
            name : str
                The name of the dataset. This must be unique within the group of tasks.
            path : str
                The file path where the dataset is located.
            from_task : Union[None, 'Task'], optional
                An optional task from which this dataset is derived. Default is None.

            Note:
            -----
            This constructor is responsible for setting up the dataset with the provided
            parameters and ensuring that the dataset is ready for use in the application.

            Raises:
            ------
            RuntimeError
                If a dataset with the same name already exists in the group of tasks.

            Notes:
            -----
            This constructor also adds the dataset instance to the global 
            __datasets__ dictionary, ensuring that each dataset name is unique.
            """
            self.name = name
            self.path = path
            self.from_task = from_task
            ctx=get_context()
            if name in ctx.datasets:
                raise RuntimeError(f"a dataset with name {name} already exists inside of this group of tasks.")
            ctx.datasets[name] = self
            
    def to_raw(self) -> Dict:
            """
            Convert the dataset instance to a raw dictionary representation.

            Returns:
                Dict: A dictionary containing the dataset's name, path, 
                      and the name of the associated task if available.
            """
            
            return {
                'name'      : self.name,
                'path'      : self.path,
                'from_task' : self.from_task.name if self.from_task is not None else "",
            }
        
    @classmethod
    def from_raw(cls, raw: Dict) -> 'Dataset':
            """
            Create a Dataset instance from a raw dictionary representation.

            Parameters:
                raw (Dict): A dictionary containing the dataset's attributes.

            Returns:
                Dataset: An instance of the Dataset class initialized with the provided attributes.
            """
            #ctx = get_context()
            #from_task = None
            #if raw['from_task'] != "":
            #    from_task = ctx.tasks[ raw['from_task'] ]
            return cls(
                name=raw['name'],
                path=raw['path'],
            )
        
    def mkdir(self):
            """
            Create a directory and symlink files.

            This method creates a directory at the specified basepath with the name
            of the current instance. It then creates symbolic links for each file
            returned by the `files()` method in the newly created directory.

            Comments:
            - The directory will be created if it does not already exist.
            - Symbolic links will be created for each file in the directory.
            """
            ctx = get_context()
            dirpath = f"{ctx.path}/datasets/{self.name}"
            os.makedirs(dirpath, exist_ok=True)
            if self.path != dirpath:
                for target in self:
                    filename = target.split('/')[-1]
                    linkpath = f"{dirpath}/{filename}"
                    symlink(target, linkpath)

    def __iter__(self) -> List[str]:
        """ 
        Retrieve a sorted list of file paths from the specified directory.
        
        This method expands the folders in the given path and returns 
        a sorted list of all files found within those folders.
        
        Returns:
            List[str]: A sorted list of file paths.
        """
        self.index = 0
        self.files = sorted(expand_folders(self.path))
        return self
    
    def __next__(self):
        if self.index >= len(self.files):
            raise StopIteration
        path = self.files[self.index]
        self.index += 1
        return path

    def __len__(self):
        return len( expand_folders(self.path) )