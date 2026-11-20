# Oil & Gas Well Telemetry: Medallion Pipeline on Databricks

A batch data pipeline built on Databricks (Auto Loader, Delta Lake, Unity Catalog). It ingests simulated well telemetry, cleans it, quarantines bad records, and produces a daily summary per well.

What this project is (and is not)
Synthetic data. A simulator (random.seed(42), so it is reproducible) writes JSON files with pressure and flow readings for 5 wells over 14 days: 70 files, 144 readings per file, 10,080 readings in total.
Injected defects. About 3% of readings have an empty pressure, 3% have the text ERR_VAL_99, and 3% have -999.0. Wells WELL_03 and WELL_05 include a pressure-drop scenario.
Incremental batch, not real time. Auto Loader runs with trigger(availableNow=True): it processes the new files and stops.
No machine learning. There is no predictive model. The status view uses a fixed threshold.
Architecture
Layer	Table / View	Purpose
Bronze	oil_gas_bronze.telemetry_raw	Raw data, all columns as STRING, plus source_file and ingested_at.
Silver	oil_gas_silver.telemetry_clean	Typed and validated readings. NOT NULL and CHECK (>= 0) constraints on pressure and flow.
Silver	oil_gas_silver.telemetry_quarantine	Rejected rows, with the raw text values and a reject_reason.
Gold	oil_gas_gold.well_daily_summary	One row per well per day: readings count, average and minimum pressure, average flow.
Gold	oil_gas_gold.v_well_status_daily	View that adds pressure_status (NORMAL or LOW_PRESSURE).

Each layer has its own schema. This keeps permissions separate and allows rebuilding one layer without touching the others.

Data quality

Bronze keeps everything as text, so no row is lost at ingestion. Silver converts types with TRY_CAST / TRY_TO_TIMESTAMP (they return NULL instead of failing) and classifies each row with a reject_reason:

missing_well_id, invalid_timestamp, invalid_pressure, negative_pressure, invalid_flow

Result on the generated data:

Check	Value
Bronze rows	10,080
Silver clean rows	9,184
Quarantined rows	896 (595 invalid_pressure, 301 negative_pressure)
Reconciliation	9,184 + 896 = 10,080
Gold	70 rows (5 wells x 14 days), 9,184 readings in total
Idempotency

Re-running any step does not duplicate or change data:

Bronze: Auto Loader keeps a checkpoint, so each file is ingested once.
Silver (clean and quarantine): MERGE INTO on the natural key. The clean table uses ROW_NUMBER() to keep the latest record if the same reading arrives twice. The second run reports 0 inserted and 0 updated rows.
Gold: INSERT OVERWRITE. Gold is derived from Silver, so it is recomputed in full and always gives the same result. For a much larger table, I would overwrite only the affected partitions or use MERGE.
Tests

07_Tests/01_pipeline_tests runs five assertions:

Reconciliation: Silver clean + quarantine = Bronze.
No duplicate keys in Silver.
No negative pressure or flow in Silver.
Every quarantined row has a reject_reason.
Gold total readings = Silver clean rows.
Limitations
The data is synthetic. The LOW_PRESSURE threshold (1,000 psi) was chosen from the observed distribution: healthy wells stay around 1,200 psi and the drop reaches around 750 psi. With real wells, the threshold should come from the operating specification.
The status is daily. It shows which day a well dropped, not at what time.
The simulator only injects defects into pressure. The rules missing_well_id, invalid_timestamp and invalid_flow match no rows in this dataset, and they have no dedicated test yet.
The reconciliation test assumes Bronze has no duplicate readings. If duplicates arrive, Silver removes them and the test would fail even though the pipeline is correct.
No scheduled job yet. The notebooks are run manually.
How to run

Run the notebooks in this order (Databricks serverless). In the Silver transform notebook, run the cells from top to bottom: the temporary views only exist in the current session.

00_setup: creates schemas, the landing volume and the Silver tables.
01_simulator: generates the JSON files.
02_bronze_ingest: Auto Loader into Bronze.
03_silver_transform: quarantine and clean-table merges.
04_gold_summary: builds the daily summary.
05_views: creates the status view.
01_pipeline_tests: runs the assertions.