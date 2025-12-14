from __future__ import annotations

from digital_operation_twin.services.csv_ingestion import CsvIngestionService


def test_csv_ingestion_load_and_build_api_body() -> None:
    csv_bytes = b"id,name\n1,alpha\n2,beta\n"
    svc = CsvIngestionService(default_source="unit_csv")
    df = svc.load_to_df(csv_bytes)
    assert df.shape == (2, 2)

    body = svc.build_api_body(
        df=df,
        event_type="record.ingest",
        attributes={"customer_id": "customer_a"},
    )
    assert "envelope" in body
    assert body["envelope"]["event_type"] == "record.ingest"
    assert body["envelope"]["attributes"]["customer_id"] == "customer_a"
    assert isinstance(body["payload"], list)
    assert body["payload"][0]["id"] == 1
