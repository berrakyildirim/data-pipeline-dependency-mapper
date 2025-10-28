import csv, os, re, pprint, itertools
from collections import defaultdict
from typing import Dict, List
from pathlib import Path

def get_external_table_names_from_csv(filepath: str) -> dict:
    """
    It reads a csv file from given path.
    """
    if not os.path.exists(filepath):
        print(f"Error: '{filepath}' file not found.")
        return {}

    external_table_names = []
    try:
        with open(filepath, mode='r', encoding='utf-8') as file:
            reader = csv.reader(file)
            # Skip the header
            next(reader)

            # Get external tables names
            external_table_names = [
                f"{row[1].strip()}.{row[2].strip()}"
                for row in reader if len(row) >= 3
            ]

    except (IOError, IndexError) as e:
        print(f"An error occurred while reading the csv file: {e}")
        return {}

    # Return a dictionary as:
    # {'external_table_names': [ODS_EXTERNAL.CCSTORE, ...]}
    return {'external_table_names': external_table_names}
def find_strings_in_sql_files(search_data: list, root_path: str) -> dict:
    """
    Searches for strings from a list within all .sql files in a given directory path,
    excluding any file named 'table_list.sql'.
    """
    results = {search_string: [] for search_string in search_data}

    for subdir, _, files in os.walk(root_path):
        for file in files:
            if file.endswith(".sql") and file != "table_list.sql":
                file_path = os.path.join(subdir, file)
                try:
                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                        content = f.read()
                        for search_string in search_data:
                            if search_string in content:
                                results[search_string].append(file_path)
                except (IOError, OSError) as e:
                    print(f"Error reading file: {file_path} - {e}")

    # Return a dict as:
    # {'FINANCE.dim_bofc_country': [],
    #  'FINANCE.e_commerce_stg_migros_delist': ['C:pipeline\\migros\\sqls\\migros_delist_ghseet_to_bq.sql'],
    #  ...}
    return results
def extract_owners(files_dict: Dict[str, List[str]]) -> Dict[str, List[str]]:
    """
    Analyzes specified files to extract owner email addresses from them.

    The function reads each file and searches for lines containing 'owner:'
    (case-insensitive). It then extracts email addresses from the lines that
    follow, until it encounters a new section defined by a 'Key:' pattern.

    Args:
        files_dict: A dictionary where keys are arbitrary group names (e.g., 'dags')
                    and values are lists of absolute file paths to the files
                    that need to be parsed.
                    Example: {'dags': ['/path/to/dag1.py', '/path/to/dag2.py']}

    Returns:
        A dictionary where each key is a file path that was analyzed and its
        value is a list of unique owner email addresses found within that file.
        Example: {'/path/to/dag1.py': ['owner1@example.com', 'owner2@example.com']}
    """
    # Regex to find any email address
    email_pattern = re.compile(r'[\w\.-]+@[\w\.-]+')

    # Regex to find a line that indicates an owner (case-insensitive)
    owner_tag_pattern = re.compile(r'owner:', re.IGNORECASE)

    # Regex to identify a new section (e.g., "State:", "**Description:**")
    # This stops the search for emails for the current owner tag.
    new_section_pattern = re.compile(r'^\s*(\*\*|#)?\s*[a-zA-Z_]+:\s*(\*\*|#)?\s*$')

    owner_results = {}

    # Iterate through each list of file paths in the input dictionary
    for file_list in files_dict.values():
        for filepath in file_list:
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
            except FileNotFoundError:
                print(f"Warning: The file at {filepath} was not found. Skipping.")
                continue
            except Exception as e:
                print(f"Warning: Could not read file {filepath} due to error: {e}. Skipping.")
                continue

            file_owners = []
            # Iterate through each line of the file with its index
            for i, line in enumerate(lines):
                # Check if the line contains an owner tag
                if owner_tag_pattern.search(line):
                    # Once an owner tag is found, start searching for emails
                    # in the subsequent lines.
                    for j in range(i + 1, len(lines)):
                        next_line = lines[j]

                        # If the next line marks a new section, stop searching for this owner
                        if new_section_pattern.match(next_line.strip()):
                            break

                        # Find all emails in the current line and add them to the list
                        found_emails = email_pattern.findall(next_line)
                        if found_emails:
                            file_owners.extend(found_emails)

            # If any owners were found, add them to the results dictionary
            # Using a set to ensure emails are unique, then converting back to a list
            if file_owners:
                owner_results[filepath] = sorted(list(set(file_owners)))

    return owner_results
def get_filename_from_path(filepath: str) -> str:
    r"""
    Extracts and returns the filename from a given file path, correctly
    handling path notations like '..', '.', '/', and '\'.

    Example: \pipeline\stock\stock.sql returns as stock.sql
    """
    path_object = Path(filepath)

    # The .name attribute of a Path object gives the final component of the path (the filename).
    return path_object.stem
def find_dict_strings_in_py_files(search_data: dict, root_path: str) -> dict:
    """
    Searches for strings from a dictionary within all .py files in a given directory path.
    """
    all_search_strings = [value for values in search_data.values() for value in values]
    results = {search_string: [] for search_string in all_search_strings}

    for subdir, _, files in os.walk(root_path):
        for file in files:
            if file.endswith(".py"):
                file_path = os.path.join(subdir, file)
                try:
                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                        content = f.read()
                        # Since the keys of the 'results' dictionary are now the search strings,
                        # we can iterate directly over these keys.
                        for search_string in results.keys():
                            if get_filename_from_path(search_string) in content:
                                # Check to avoid adding the same file path multiple times if the string is found
                                # more than once in the same file.
                                if file_path not in results[search_string]:
                                    results[search_string].append(file_path)
                except (IOError, OSError) as e:
                    print(f"Error: Could not read file: {file_path} - {e}")

    # (Optional) Return only the results for strings that were actually found.
    final_results = {key: value for key, value in results.items() if value}

    return final_results
def find_list_strings_in_py_files(search_data: list, root_path: str) -> dict:
    """
    Searches for strings from a list within all .py files in a given directory path,
    """
    results = {search_string: [] for search_string in search_data}

    for subdir, _, files in os.walk(root_path):
        for file in files:
            if file.endswith(".py"):
                file_path = os.path.join(subdir, file)
                try:
                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                        content = f.read()
                        for search_string in search_data:
                            if search_string in content:
                                results[search_string].append(file_path)
                except (IOError, OSError) as e:
                    print(f"Error reading file: {file_path} - {e}")

    return results
def find_yaml_paths_in_files(py_files: list) -> dict:
    """
    Analyzes a list of Python files to find and extract file paths
    contained within yaml.safe_load(open(...)) expressions.
    """
    pattern = re.compile(r"yaml\.(?:safe_)?load\(open\(['\"](.*?)['\"]")

    # The final dictionary to store the results
    extracted_paths_dict = {}

    for py_file in py_files:
        extracted_paths_dict[py_file] = []

        try:
            with open(py_file, 'r', encoding='utf-8') as f:
                content = f.read()
                found_paths = pattern.findall(content)
                if found_paths:
                    extracted_paths_dict[py_file].extend(found_paths)

        except FileNotFoundError:
            print(f"Warning: The file '{py_file}' was not found. Skipping.")
        except Exception as e:
            print(f"Warning: An error occurred while reading '{py_file}': {e}")

    # Example:
    #   {'path/to/dag1.py': ['config/a.yaml', 'config/b.yaml'],
    #    'path/to/dag2.py': ['config/c.yaml'],
    #    ... }
    return extracted_paths_dict
def get_paths(source_dict, key, default_value = [None]):
    return source_dict.get(key, default_value)
def write_to_csv(external_table_names: list, sql_paths: dict, py_paths: dict,
                 py_table_paths: dict, yaml_paths: dict, csv_output_path: str) -> None:
    """
    It writes the given information to the csv file
    """
    rows_to_write = []

    for external_table_name in external_table_names:
        associated_sql_paths = get_paths(sql_paths, external_table_name)

        if not any(associated_sql_paths):
            rows_to_write.append([external_table_name, '', '', ''])
            continue

        for sql_path in associated_sql_paths:
            if sql_path is None:
                rows_to_write.append([external_table_name, '', '', ''])
                continue

            py_paths_from_sql = get_paths(py_paths, sql_path, default_value=[])
            py_paths_from_table = get_paths(py_table_paths, external_table_name, default_value=[])

            all_py_paths = set(p for p in py_paths_from_sql + py_paths_from_table if p is not None)

            if not all_py_paths:
                rows_to_write.append([external_table_name, sql_path, '', ''])
                continue

            for py_path in all_py_paths:
                associated_yaml_paths = get_paths(yaml_paths, py_path, default_value=[None])

                if not any(associated_yaml_paths):
                    rows_to_write.append([external_table_name, sql_path, py_path, ''])
                    continue

                for yaml_path in associated_yaml_paths:
                    if yaml_path is not None:
                        rows_to_write.append([external_table_name, sql_path, py_path, yaml_path])
                    else:
                        rows_to_write.append([external_table_name, sql_path, py_path, ''])

    header = ['external_table_name', 'sql_path', 'py_path', 'yaml_path']
    try:
        with open(csv_output_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)

            writer.writerow(header)
            writer.writerows(rows_to_write)


    except IOError as e:
        print(f"File couldn't written in path: '{csv_output_path}'.")
        print(f"Error: {e}")
    except Exception as e:
        print(f"There is an unexpected error: {e}")
def extract_owners_from_file(filepath: str) -> list:
    """
    Analyzes a specified file to extract owner email addresses from it.

    The function reads the file and searches for lines containing 'owner:'
    (case-insensitive). It then extracts email addresses from the lines that
    follow, until it encounters a new section defined by a 'Key:' pattern.
    """
    # Regex to find any email address
    email_pattern = re.compile(r'[\w\.-]+@[\w\.-]+')

    # Regex to find a line that indicates an owner (case-insensitive)
    owner_tag_pattern = re.compile(r'owner:', re.IGNORECASE)

    # Regex to identify a new section (e.g., "State:", "**Description:**")
    # This stops the search for emails for the current owner tag.
    new_section_pattern = re.compile(r'^\s*(\*\*|#)?\s*[a-zA-Z_]+:\s*(\*\*|#)?\s*$')

    owner_emails = []

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except FileNotFoundError:
        print(f"Warning: The file at {filepath} was not found. Returning an empty list.")
        return []
    except Exception as e:
        print(f"Warning: Could not read file {filepath} due to error: {e}. Returning an empty list.")
        return []

    # Iterate through each line of the file with its index
    for i, line in enumerate(lines):
        # Check if the line contains an owner tag
        if owner_tag_pattern.search(line):
            # Once an owner tag is found, start searching for emails
            # in the subsequent lines.
            for j in range(i + 1, len(lines)):
                next_line = lines[j]

                # If the next line marks a new section, stop searching for this owner
                if new_section_pattern.match(next_line.strip()):
                    break

                # Find all emails in the current line and add them to the list
                found_emails = email_pattern.findall(next_line)
                if found_emails:
                    owner_emails.extend(found_emails)

    # To ensure emails are unique, use a set and then convert back to a sorted list
    if owner_emails:
        return sorted(list(set(owner_emails)))

    # Example:
    # ['owner1@example.com', 'owner2@example.com']
    return []
def create_dict_from_csv(file_path: str) -> dict:
    """
    Reads a CSV file from the given file path and converts its content into a dictionary.

    The CSV file must have two columns named 'dag_id' and 'execution_datetime'.
    """
    dag_executions = {}
    try:
        with open(file_path, mode='r', encoding='utf-8') as csv_file:
            reader = csv.DictReader(csv_file)

            for row in reader:
                dag_id = row.get('dag_id', '').strip()
                exec_datetime = row.get('execution_datetime', '').strip()

                if dag_id:
                    dag_executions[dag_id] = exec_datetime

    except FileNotFoundError:
        print(f"Error: File not found at '{file_path}'.")
    except Exception as e:
        print(f"An error occurred: {e}")
    return dag_executions
def output_csv_to_list(path):
    rows_list = []

    with open(path, "r", newline="", encoding="utf-8") as csvfile:
        reader = csv.reader(csvfile)
        next(reader)

        for row in reader:
            if len(row) == 4:
                rows_list.append(row)
    return rows_list
def overwrite_csv(rows: list, time: dict, path: str):
    with open(path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['external_table_name', 'sql_file_path', 'dag', 'pod', 'dag_time', 'yaml_airflow_files', 'owners'])
        for row in rows:
            if len(row[2]) > 0:
                writer.writerow([row[0], format_path_to_relative(row[1]), format_path_to_relative(row[2]), get_first_subfolder_from_anchor(row[2]), time.get(get_filename_from_path(row[2])), row[3], str(extract_owners_from_file(row[2]))])
            else:
                writer.writerow([row[0], format_path_to_relative(row[1]), '', '', '', '', ''])
def get_first_subfolder_from_anchor(file_path: str, anchor_folder: str = "pipeline"):
    """
    Parses a file path to find an anchor folder and returns its immediate subfolder.

    For a path like "...\\pipeline\\a\\b\\c", this function
    will find "pipeline" and return "a".

    Args:
        file_path (str): The full path to a file or directory.
        anchor_folder (str): The parent folder to search for. Defaults to
                             "pipeline".

    Returns:
        Optional[str]: The name of the folder immediately following the anchor folder.
                       Returns None if the anchor folder is not found or if it's
                       the last folder in the path.
    """
    try:
        # Create a Path object for robust, cross-platform path handling
        p = Path(file_path)

        # Get a tuple of all parts of the path (e.g., ('C:', 'Users', '...'))
        parts = p.parts

        # Find the index of our anchor folder in the path parts
        if anchor_folder in parts:
            anchor_index = parts.index(anchor_folder)

            # Check if there is another part (a subfolder) after the anchor
            if anchor_index + 1 < len(parts):
                return str(parts[anchor_index + 1])

        # Return None if the anchor wasn't found or it was the last item
        return None
    except (TypeError, ValueError):
        # Handle cases where the input is not a valid path
        return None
def format_path_to_relative(filepath: str) -> str:
    target_str = "\\pipeline"
    try:
        clean_filepath = filepath.strip()
        index = clean_filepath.index(target_str)
        return clean_filepath[index:]
    except ValueError:
        return filepath.strip()

if __name__ == "__main__":

    input_csv_file_path = r""
    pipeline_file_path = r""
    output_csv_file_path = r""
    dags_times_csv_file_path = r"


    print("Reading data from CSV file...")

    # {'external_table_names': ['ODS_EXTERNAL.CCSTORE_ORDERSTATUS', 'ODS_EXTERNAL.PRODUCTION_BP_TARGET_GSHEET, ...]}
    external_table_names = get_external_table_names_from_csv(input_csv_file_path)

    # {'FINANCE.dim_bofc_country': [],
    #  'FINANCE.e_commerce_stg_migros_delist': ['C:pipeline\\migros\\sqls\\migros_delist_ghseet_to_bq.sql'], ...}
    sql_file_paths = find_strings_in_sql_files(external_table_names.get('external_table_names'), pipeline_file_path)

    # {'pipeline\\commercial_asa_redesign\\sqls\\bigquery\\bigquery_read_contract_summary.sql': [pipeline\\commercial_asa_redesign\\dags\\commercial_asa_redesign_daily_export_master_data.py'],
    #   'pipeline\\connected_planning\\sqls\\opco_bp_volume.sql': ['pipeline\\connected_planning\\dags\\opco_dashboard.py'], ...}
    py_file_paths = find_dict_strings_in_py_files(sql_file_paths, pipeline_file_path)

    # {'ODS_EXTERNAL.RM_DSI_VALUE_GSHEET': ['C:\\Users\\beyyildirim\\PycharmProjects\\PythonProject2\\pipeline\\materials\\dags\\raw_material_gsheet_check.py'],
    #   'ODS_EXTERNAL.MIGROS_OUTLET_DAILY_SALES_GSHEET': ['C:\\Users\\beyyildirim\\PycharmProjects\\PythonProject2\\pipeline\\sales\\dags\\migros_outlet_daily_sales_gsheet.py']}
    py_file_paths_by_table_name = find_list_strings_in_py_files(external_table_names.get('external_table_names'), pipeline_file_path)

    # {'.yaml': ['C:\\Users\\beyyildirim\\PycharmProjects\\PythonProject2\\pipeline\\accounting\\dags\\accounting_documents_delta.py',
    #            '  C:\\Users\\beyyildirim\\PycharmProjects\\PythonProject2\\pipeline\\analytic_data_api\\commercial\\dags\\demography.py', ...}
    yaml_files = find_list_strings_in_py_files(['.yaml'], pipeline_file_path)

    #  {'C:\\Users\\beyyildirim\\PycharmProjects\\PythonProject2\\pipeline\\voyage\\dags\\voyage_etl.py': ['{voyage_etl_home}/configs/dags.yaml',
    #                                                                                                                   '{voyage_etl_home}/configs/tables.yaml',
    #                                                                                                                   '{voyage_etl_home}/configs/schemas_v2.yaml',
    #                                                                                                                   '{voyage_etl_home}/configs/connections.yaml'],
    #  'C:\\Users\\beyyildirim\\PycharmProjects\\PythonProject2\\pipeline\\voyage\\dags\\xmobile_data_gcp_to_voyage.py': ['/home/airflow/gcs/dags/voyage/configs/xmobile_data_gcp_to_voyage.yaml']}
    yaml_airflow_file_paths = find_yaml_paths_in_files(yaml_files.get('.yaml'))

    # {'monitor_fct_sales': '2025-08-01 05:35:23.000000 UTC',
    #   'spend_self_service_report': '2025-08-01 08:02:20.000000 UTC', ...}
    dags_times_dict = create_dict_from_csv(dags_times_csv_file_path)

    write_to_csv(external_table_names.get('external_table_names'), sql_file_paths, py_file_paths,
                 py_file_paths_by_table_name, yaml_airflow_file_paths, output_csv_file_path)

    dags_time = create_dict_from_csv(dags_times_csv_file_path)
    mlist = output_csv_to_list(output_csv_file_path)


    overwrite_csv(mlist, dags_time, output_csv_file_path)
    print("Done.")
