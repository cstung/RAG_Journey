import os

import pytest

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_HF_INTEGRATION_TESTS") != "1",
    reason="Hugging Face integration tests are opt-in",
)

pytest.importorskip("datasets")
pytest.importorskip("pyarrow")

from data_adapters.hf_legal import DATASET_ID, VNLegalDocumentConnector


def test_vn_legal_connector_metadata_smoke():
    connector = VNLegalDocumentConnector(max_docs=2)
    total = connector.total_records()
    assert total > 0


@pytest.mark.integration
def test_vn_legal_connector_iter_records_smoke():
    connector = VNLegalDocumentConnector(max_docs=1)
    first = next(connector.iter_records())
    assert first.id
    assert first.text.strip()
    assert first.metadata["dataset"] == DATASET_ID
