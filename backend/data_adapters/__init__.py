# backend/datasets/__init__.py
import yaml
from pathlib import Path
from .base import BaseDatasetConnector, DatasetRecord
from .hf_legal import VNLegalDocumentConnector
from .hf_generic import HFGenericConnector

_REGISTRY_FILE = Path(__file__).parent / "registry.yaml"
_raw = yaml.safe_load(_REGISTRY_FILE.read_text())

REGISTRY: dict[str, dict] = {d["id"]: d for d in _raw["datasets"]}
_CONNECTOR_MAP = {
    "VNLegalDocumentConnector": VNLegalDocumentConnector,
    "HFGenericConnector": HFGenericConnector,
}

def get_connector(dataset_id: str, **kwargs) -> BaseDatasetConnector:
    entry = REGISTRY.get(dataset_id)
    if not entry:
        raise ValueError(f"Dataset not in registry: {dataset_id}")
    cls = _CONNECTOR_MAP[entry["connector"]]
    if entry["connector"] == "HFGenericConnector":
        merged_config = {**entry.get("config", {}), **kwargs}
        return cls(config=merged_config)
    return cls(**kwargs)
