use super::*;
use arrow_array::{Int64Array, StringArray};
use arrow_schema::{Field, Schema};
use pyo3::types::PyDict;
use std::sync::Arc;

/// Two batches, same column *set*, different column *order*.
///
/// PE WAL flushes can assemble sparse vs dense chunks into one table flush;
/// each RecordBatch must bind using its own schema names so Int64 slots
/// never land under Utf8 keys like `expertStatus`.
#[test]
fn record_batches_to_py_rows_binds_by_name_when_column_order_differs() {
    Python::initialize();

    let schema_canonical = Arc::new(Schema::new(vec![
        Field::new("expertStatus", DataType::Utf8, false),
        Field::new("expertStatusStartTs", DataType::Utf8, false),
        Field::new("expertChatSlotsAvailable", DataType::Int64, false),
    ]));
    let batch0 = RecordBatch::try_new(
        Arc::clone(&schema_canonical),
        vec![
            Arc::new(StringArray::from(vec!["Available"])),
            Arc::new(StringArray::from(vec!["2026-10-14T15:00:00Z"])),
            Arc::new(Int64Array::from(vec![0_i64])),
        ],
    )
    .unwrap();

    // Same 3 columns, order swapped: slots, status, startTs.
    let schema_reordered = Arc::new(Schema::new(vec![
        Field::new("expertChatSlotsAvailable", DataType::Int64, false),
        Field::new("expertStatus", DataType::Utf8, false),
        Field::new("expertStatusStartTs", DataType::Utf8, false),
    ]));
    let batch1 = RecordBatch::try_new(
        schema_reordered,
        vec![
            Arc::new(Int64Array::from(vec![0_i64])),
            Arc::new(StringArray::from(vec!["Available"])),
            Arc::new(StringArray::from(vec!["2026-10-14T15:00:00Z"])),
        ],
    )
    .unwrap();

    Python::attach(|py| {
        let rows = record_batches_to_py_rows(py, &[batch0, batch1]).expect("convert");
        assert_eq!(rows.len().unwrap(), 2);

        for row_idx in 0..2 {
            let row = rows
                .get_item(row_idx)
                .unwrap()
                .cast_into::<PyDict>()
                .unwrap();
            assert_eq!(
                row.get_item("expertStatus")
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "Available",
                "row {row_idx}: expertStatus must stay Utf8"
            );
            assert_eq!(
                row.get_item("expertStatusStartTs")
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "2026-10-14T15:00:00Z",
                "row {row_idx}: startTs must stay Utf8"
            );
            assert_eq!(
                row.get_item("expertChatSlotsAvailable")
                    .unwrap()
                    .extract::<i64>()
                    .unwrap(),
                0,
                "row {row_idx}: slots must stay Int64"
            );
        }
    });
}

/// Same width, different name set must fail (not silently uneven row dicts).
#[test]
fn record_batches_to_py_rows_rejects_mismatched_column_sets() {
    Python::initialize();

    let batch0 = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("expertStatus", DataType::Utf8, false),
            Field::new("expertStatusStartTs", DataType::Utf8, false),
            Field::new("expertChatSlotsAvailable", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["Available"])),
            Arc::new(StringArray::from(vec!["2026-10-14T15:00:00Z"])),
            Arc::new(Int64Array::from(vec![0_i64])),
        ],
    )
    .unwrap();

    // Same count, but `expertMaxChatSlots` instead of `expertChatSlotsAvailable`.
    let batch1 = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("expertStatus", DataType::Utf8, false),
            Field::new("expertStatusStartTs", DataType::Utf8, false),
            Field::new("expertMaxChatSlots", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["Available"])),
            Arc::new(StringArray::from(vec!["2026-10-14T15:00:00Z"])),
            Arc::new(Int64Array::from(vec![0_i64])),
        ],
    )
    .unwrap();

    Python::attach(|py| {
        let err = record_batches_to_py_rows(py, &[batch0, batch1]).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("batch schema mismatch"),
            "expected schema mismatch error, got: {msg}"
        );
    });
}
