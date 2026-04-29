# backend/datasets/base.py
from abc import ABC, abstractmethod
from typing import Iterator

class DatasetRecord:
    """Normalized record from any dataset source."""
    def __init__(
        self,
        id: str,
        text: str,
        metadata: dict,      # arbitrary key-value, will be stored as Qdrant payload
    ):
        self.id = id
        self.text = text
        self.metadata = metadata


class BaseDatasetConnector(ABC):
    """
    Every dataset adapter must implement this interface.
    The ingestion pipeline only speaks to BaseDatasetConnector.
    """

    @property
    @abstractmethod
    def collection_name(self) -> str:
        """Target Qdrant collection."""
        ...

    @abstractmethod
    def iter_records(self) -> Iterator[DatasetRecord]:
        """
        Yield DatasetRecord objects one at a time (memory-efficient).
        Chunking happens INSIDE this method if needed.
        """
        ...

    @abstractmethod
    def total_records(self) -> int:
        """Approximate total — used for progress reporting."""
        ...
