# ValidationFramework
Implemented a generic validation framework using rules from rules.json, adding status and reason columns dynamically. Validated data is stored in HDFS as Parquet and exposed via an external Hive table with inferred schema.

🛠️ Feature: Dynamic Validation with External Hive Table Creation
	•	Implemented a generic validation framework that reads rules from a rules.json file to dynamically validate multiple columns.
	•	After applying validation rules, two new columns are added per rule:
	•	validation_status_<column>: Marks the result as PASS or FAIL.
	•	failure_reason_<column>: Captures the failure reason when validation fails, else stores null.
	•	The validated DataFrame is written to HDFS in Parquet format.
	•	An external Hive table is dynamically created using the DataFrame’s inferred schema to reference the Parquet data in HDFS.
	•	Ensures schema flexibility and external data referencing for downstream consumption.
