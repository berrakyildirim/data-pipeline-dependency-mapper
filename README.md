# Data Pipeline Dependency Mapper

This Python project automatically maps dependencies between external tables, SQL scripts, Airflow DAG Python files, and YAML configuration files, then exports a unified CSV report containing ownership information and DAG execution times.

It is designed to help data engineering teams analyze and document data pipeline relationships (table → SQL → DAG → YAML → owner) efficiently.

## Features

**Read external table names from CSV**
Extracts a list of external tables using get_external_table_names_from_csv().

**Search SQL files for table usage**
Locates .sql files containing references to external tables via find_strings_in_sql_files().

**Find related Python DAG files**
Maps SQL files to .py DAG files using find_dict_strings_in_py_files().

**Detect DAGs referencing tables directly**
Identifies DAGs that use tables directly through find_list_strings_in_py_files().

**Extract YAML configuration paths**
Uses regex to capture YAML file paths from yaml.safe_load(open(...)) calls in Python DAGs.

**Extract DAG owner emails**
Finds and lists owner email addresses in Airflow DAGs using extract_owners_from_file().

Generate comprehensive CSV output
Combines all findings (tables, SQLs, DAGs, YAMLs, owners, and execution times) into a single CSV file.

## Input Files
### 1. input_csv_file_path

CSV file containing the list of external tables.
Expected columns:
| index | schema | table | ... |

Example:

index,schema,table
1,ODS_EXTERNAL,CCSTORE_ORDERSTATUS
2,ODS_EXTERNAL,PRODUCTION_BP_TARGET_GSHEET

### 2. pipeline_file_path

Root directory of your data pipeline project.
All .sql, .py, and .yaml files under this path will be scanned.

Example:

C:\Users\YourUser\PycharmProjects\PythonProject2\pipeline

### 3. dags_times_csv_file_path

CSV file that maps DAG IDs to their latest execution timestamps.
Expected columns:
| dag_id | execution_datetime |

Example:

monitor_fct_sales,2025-08-01 05:35:23.000000 UTC
spend_self_service_report,2025-08-01 08:02:20.000000 UTC

## Output
### Stage 1 – write_to_csv

Initial mapping between tables, SQLs, DAGs, and YAMLs:
| external_table_name | sql_path | py_path | yaml_path |

### Stage 2 – overwrite_csv

Final enriched CSV with complete metadata:

external_table_name	sql_file_path	dag	pod	dag_time	yaml_airflow_files	owners

Example:

ODS_EXTERNAL.MIGROS_OUTLET_DAILY_SALES_GSHEET,\pipeline\sales\sqls\migros_outlet_daily_sales_gsheet.sql,\pipeline\sales\dags\migros_outlet_daily_sales_gsheet.py,sales,2025-08-01 08:02:20.000000 UTC,/home/airflow/gcs/dags/voyage/configs/xmobile_data_gcp_to_voyage.yaml,['owner1@company.com']

## How to Use

Update file paths in the main script:

if __name__ == "__main__":
    input_csv_file_path = r"C:\path\to\external_tables.csv"
    pipeline_file_path = r"C:\path\to\pipeline"
    output_csv_file_path = r"C:\path\to\output.csv"
    dags_times_csv_file_path = r"C:\path\to\dags_times.csv"


Run the script:

python main.py


The output CSV will contain the full dependency mapping.

## Execution Flow

get_external_table_names_from_csv() → Read table names

find_strings_in_sql_files() → Locate SQL scripts using those tables

find_dict_strings_in_py_files() → Find DAGs referencing SQL scripts

find_list_strings_in_py_files() → Detect DAGs directly using tables

find_list_strings_in_py_files(['.yaml']) → Identify YAML references

find_yaml_paths_in_files() → Extract YAML file paths

create_dict_from_csv() → Load DAG execution timestamps

write_to_csv() → Write initial mapping

output_csv_to_list() → Reload CSV as list

overwrite_csv() → Add DAG execution times and owners

## Example Directory Structure
pipeline/

│

├── migros/

│   ├── sqls/

│   │   └── migros_delist_ghseet_to_bq.sql

│   └── dags/

│       └── migros_outlet_daily_sales_gsheet.py

│

├── voyage/

│   ├── dags/

│   │   └── voyage_etl.py

│   └── configs/

│       └── tables.yaml

## Requirements

Python 3.8+

Only built-in libraries required:
csv, os, re, pathlib, itertools, collections

## Notes

Missing or unreadable files trigger a warning but do not stop execution.

The tool uses regex to extract emails after owner: lines.

Uses pathlib.Path for cross-platform path handling (Windows/Linux).

Relative paths are formatted for readability in output CSVs.

## Author

Berrak Yıldırım
Developed as a tool to automatically document dependencies between data pipeline components and improve observability across Airflow, SQL, and YAML layers.

## Repository Info

Repository name: data-pipeline-dependency-mapper
Description:

Python tool that scans your data pipeline to map relationships between external tables, SQL scripts, Python DAGs, and YAML configs — generating a detailed CSV report with owners and DAG execution times.
