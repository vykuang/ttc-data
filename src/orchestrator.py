from abc import ABC, abstractmethod
from pathlib import Path
from typing import TypeVar, Generic, BinaryIO

FrameT = TypeVar("FrameT")

class StorageBackend(ABC):
    """
    Responsible only for locating/moving bytes
    Does not know about data/file formats, only generic byte streams

    Possible adapters:
    local
    S3
    azure blob
    GCS
    
    To be composed into a repository, along with TabularFormat
    repo = Repository(
        storage=LocalBackend(),
        format=PolarsParquetFormat()
        )
    repo.read(src=path_to_parquet) -> pl.DataFrame
    """
    @abstractmethod
    def open_reader(self, src_path: str) -> BinaryIO:
        pass

    @abstractmethod
    def open_writer(self, dest_path: str, overwrite: bool = False) -> BinaryIO:
        pass

class TabularFormat(ABC, Generic[FrameT]):
    """
    Decouples storage from knowing data/file formats
    Use of FrameT allows type checker to know the typing should be consistent
    which generic Object would not allow
    """
    @abstractmethod
    def read(self, source: BinaryIO) -> FrameT:
        pass

    @abstractmethod
    def write(self, frame: FrameT, dest: BinaryIO) -> None:
        pass
    
class Repository(Generic[FrameT]):
    def __init__(self, storage: StorageBackend, format: TabularFormat[FrameT]):
        self._storage = storage
        self._format = format

    def read(self, path) -> FrameT:
        with self._storage.open_reader(path) as src:
            return self._format.read(src)
            
    def write(self, data: FrameT, path: str, overwrite: bool=False) -> str:
        with self._storage.open_writer(path, overwrite) as target:
            self._format.write(data, target)
        return path
