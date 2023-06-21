
# In[]

from job import RetrievalJob

# In[]

job = RetrievalJob('agrs')
job.validate()
job.setup()


print(job.cfg.get_sensor())
print(job.cfg.get_carrier())
print(job.cfg.get_token())

print(job.aoi.keys)
print(job.aoi['berkner'].get_aoi_grid())

print(job.cfg.config)

print(job.lst.url)


# In[]

# class Process(ABC):
    
#     @classfunction
#     def process(self):
#         pass
    
#     @classmethod
#     def download_listing(self):
#         pass
    
#     @classmethod
#     def download_swath(self):
#         pass
    
#     @abstractmethod
#     def preprocess(self):
#         pass
    
#     @abstractmethod
#     def retrieve_swath_listing(self):
#         pass
    
#     #...




# In[]

# class Swath(ABC):
#     @abstractmethod
#     def add_swath_io(self, swath_type: str) -> None:
#         pass
    
#     def status(self) -> None:
#         print("normal method")
    
# class SwathModis(Swath):    
#     def add_swath_io(self) -> None:
#         self.io = ModisIO()
    

# class SwathIO(ABC):
#     @abstractmethod
#     def load(self, path: str) -> None:
#         pass
    
#     @abstractmethod
#     def get_var(self, var: str) -> None:
#         pass
    
#     @abstractmethod
#     def set_var(self, var: str) -> None:
#         pass
    
#     @abstractmethod
#     def save(self, path: str) -> None:
#         pass
    
# class ModisIO(SwathIO):
#     def __init__(self):
#         pass
    
#     def load(self, path: str) -> None:
#         print(f'this loads {path}')
        
#     def get_var(self, var: str) -> None:
#         print(f'this gets {var}')
        
#     def set_var(self, var: str) -> None:
#         print(f'this sets {var}')
        
#     def save(self, path: str) -> None:
#         print(f'this saves to {path}')
        
        
