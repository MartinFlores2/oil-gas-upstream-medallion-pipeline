Oil & Gas Well Telemetry: Medallion Pipeline on Databricks

Business question. An operations team needs to know, every day, which wells fell below normal pressure, and needs to trust the numbers. Raw telemetry arrives with defects (about 9% of readings), so averages computed over raw data are wrong. This pipeline ingests the telemetry, cleans it, keeps every rejected row together with the reason, and produces a daily summary per well.

A batch data pipeline built on Databricks (Auto Loader, Delta Lake, Unity Catalog). All data is simulated.

Results (synthetic data)
896 of 10,080 readings (8.9%) were rejected and sent to quarantine: 595 invalid_pressure and 301 negative_pressure. No row was lost: 9,184 clean + 896 quarantined = 10,080 in Bronze.
Effect of cleaning: the average pressure over the Bronze readings is 1,117.8 psi (computed with TRY_CAST, so empty and text values are ignored, but the -999.0 placeholders are still counted). Over the clean Silver table it is 1,187.2 psi. Without the quarantine step, the average would be understated by 69.4 psi (about 5.8%).
Low-pressure days flagged by the Gold view: WELL_03 and WELL_05, 1 day each. These wells carry a pressure-drop scenario injected by the simulator, so this is a check that the pipeline surfaces a known event, not a discovery.
Idempotent reruns: the second run of the Silver merge reports 0 inserted and 0 updated rows, and two consecutive job runs both passed all tests.

Pipeline diagram
(job-run.png.png)

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

Check	Value
Bronze rows	10,080
Silver clean rows	9,184
Quarantined rows	896 (595 invalid_pressure, 301 negative_pressure)
Reconciliation	9,184 + 896 = 10,080
Gold	70 rows (5 wells x 14 days), 9,184 readings in total
Idempotency

Re-running any step does not duplicate or change data:

Bronze: Auto Loader keeps a checkpoint, so each file is ingested once.
Silver clean: MERGE INTO on (well_id, reading_ts). A ROW_NUMBER() over that key (ordered by ingested_at descending) keeps only the latest record if the same reading arrives twice. Matched rows are updated only if pressure or flow actually changed.
Silver quarantine: MERGE INTO on (source_file, well_id, sensor_timestamp), insert-only. It uses null-safe equality (<=>) because rejected rows can have a NULL well_id or timestamp. The key includes source_file, so the same bad reading arriving in two different files is quarantined twice.
The second run of both merges reports 0 inserted and 0 updated rows.
Gold: INSERT OVERWRITE. Gold is derived from Silver, so it is recomputed in full and always gives the same result. For a much larger table, I would overwrite only the affected partitions or use MERGE.
Orchestration

A Databricks Job (oil_gas_medallion_pipeline) runs the pipeline end to end as four tasks in sequence:

bronze_ingest -> silver_transform -> gold_summary -> pipeline_tests

Dependencies: each task runs only if the previous one succeeded ("All succeeded"). If a task fails, the tasks after it are skipped and the run is marked as failed. Tasks that already finished are not rolled back, which is safe here because every step is idempotent.
Retries: bronze_ingest retries up to 2 times (1 minute between attempts), because ingestion is the step most likely to fail for temporary reasons and it can be repeated safely. The other tasks do not retry.
Trigger: file arrival on /Volumes/workspace/oil_gas_bronze/landing/telemetry/. The job runs when new files land, instead of on a fixed schedule, so there are no empty runs and new data is processed soon after it arrives. The trigger watches only the telemetry folder, not the whole volume, because the Auto Loader checkpoint lives in landing/_checkpoints.
Tests as the last task: if an assertion in pipeline_tests fails, the whole run fails.
Verified: two manual runs in a row both succeeded, and the tests passed both times, so re-running the job does not duplicate data.
Not included in the job: 00_setup, 01_simulator and 05_views. They are run once (the view is created with CREATE OR REPLACE VIEW and stays in place).
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
The file arrival trigger is configured, but it has not been tested with new files yet. All runs so far were started manually.
The job has no failure notifications configured.
How to run

Run the notebooks in this order (Databricks serverless). In the Silver transform notebook, run the cells from top to bottom: the temporary views only exist in the current session.

00_setup: creates schemas, the landing volume and the Silver tables.
01_simulator: generates the JSON files.
02_bronze_ingest: Auto Loader into Bronze.
03_silver_transform: quarantine and clean-table merges.
04_gold_summary: builds the daily summary.
05_views: creates the status view.
07_Tests/01_pipeline_tests: runs the assertions.

After the first setup, steps 3, 4, 5 and 7 can also run together through the Databricks Job described above.
