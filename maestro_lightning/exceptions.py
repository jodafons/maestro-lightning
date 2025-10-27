

class DatasetNotFound(Exception):
    """Raised when a specified dataset is not found."""
    def __init__(self, name):
        """Set the error message with the dataset name."""
        message = f"Dataset {name} not found in the group of datasets."
        super().__init__(message)
        
class ImageNotFound(Exception):
    """Raised when a specified image is not found."""
    def __init__(self, name):
        """Set the error message with the image name."""
        message = f"Image '{name}' not found into the group of images."
        super().__init__(message)
        
class TaskNotFound(Exception):
    """Raised when a specified image is not found."""
    def __init__(self, name):
        """Set the error message with the image name."""
        message = f"Task {name} not found into the group of tasks."
        super().__init__(message)
        
class TaskExistsError(Exception):
    """Raised when a specified image is not found."""
    def __init__(self, name):
        """Set the error message with the image name."""
        message = f"Task {name} already exists into the group of tasks."
        super().__init__(message)
        
class DatasetExistsError(Exception):
    """Raised when a specified dataset already exists."""
    def __init__(self, name):
        """Set the error message with the dataset name."""
        message = f"Dataset {name} already exists in the group of datasets."
        super().__init__(message)

class ImageExistsError(Exception):
    """Raised when a specified image already exists."""
    def __init__(self, name):
        """Set the error message with the image name."""
        message = f"Image '{name}' already exists in the group of images."
        super().__init__(message)
        
        
        