from pathlib import Path
from zipfile import BadZipFile
import csv
import re
import unicodedata
import pandas as pd

TT_DATA = "tt_data"

def get_dataset_title(directory):
    file = directory + "/" + "readme.md"
    markdown_text = open(file).read()

    in_code_block = False
    split_lines = markdown_text.splitlines()

    current_title = ""
    keep_current_title = False
    header_num = 0

    for line in split_lines:
        stripped = line.strip()

        if stripped.startswith("```"):
            in_code_block = not in_code_block

        if not in_code_block:
            bad_titles = ["data info", "cleaning script", "data dictionary", "scrape the additional datasets", "please add alt text to your posts"]
            if stripped.startswith("#"):
                header = stripped.lstrip("#").strip()
                if stripped.startswith("# "):
                    if header_num == 0 and header.lower() not in bad_titles:
                            current_title = header
                            keep_current_title = True
                    elif not keep_current_title and header.lower() not in bad_titles:
                        current_title = header
                        keep_current_title = True
                elif not keep_current_title and header.lower() not in bad_titles:
                        current_title = header
                        if header_num == 0:
                            keep_current_title = True
                header_num += 1
        
    return current_title

def get_data_dictionaries(directory):
    file = directory + "/" + "readme.md"
    this_dataset = Path(file).parent.name
    markdown_text = open(file).read()

    dataset_title = get_dataset_title(directory)

    tbls = []
    in_code_block = False
    in_tbl = False

    split_lines = markdown_text.splitlines()

    current_section = ""
    current_tbl = []

    for line in split_lines:
        stripped = line.strip()

        if stripped.startswith("```"):
            in_code_block = not in_code_block

        if not in_code_block:
            # for markdown link like [**file.csv**](file.csv)
            if stripped.startswith("[") and "](" in stripped:
                name = stripped.split("](", 1)[0].strip("[]*")
                current_section = name

            elif stripped.startswith("#"):# deal with e.g. 2019-03-05
                header = stripped.lstrip("#").strip(" `")
                if header.lower() != "data dictionary" and header.lower() != "get the data here" and header.lower().strip() != "get the data" and header.lower().strip() != "get the data!":
                    current_section = header
            
            elif stripped.startswith("**") and stripped.endswith("**") and stripped.count("**") == 2:# 2019-01-22
                current_section = stripped.strip("* ").strip()

        if not stripped.startswith("|"):
            if in_tbl:
                tbls.append({"section": current_section, "table": current_tbl})
                current_tbl = []
                in_tbl = False
            continue
        else:
            if not in_tbl:
                in_tbl = True
            current_tbl.append(stripped)
    
    if in_tbl:
        tbls.append({"section": current_section, "table": current_tbl})
        in_tbl = False

    data_dicts = []
    for item in tbls:
        table = item["table"]
        columns = [c.strip().lower() for c in table[0].strip("|").split("|")]
        if "variable" in columns and "description" in columns:
            data_dicts.append(item)
    
    real_data = []
    for item in data_dicts:
        table = item["table"]
        section = item["section"]
        columns = [c.strip().lower() for c in table[0].strip("|").split("|")]
        for row in table[2:]:
            parts = [v.strip() for v in row.strip("|").split("|")]
            
            if len(parts) > len(columns):
                # Merge overflow into the last column
                parts = parts[:len(columns)-1] + [" | ".join(parts[len(columns)-1:])]
                
            cell_vals = parts

            if len(cell_vals) != len(columns): 
                print("Messy table! check " + directory + " in " + section)
                continue
            this_row = {
                "dataset_id": this_dataset,
                "dataset_title": dataset_title,
                "section": section, 
                **dict(zip(columns, cell_vals))
                }
            real_data.append(this_row)
    
    return real_data

def get_active_columns(directory):
    this_dataset = Path(directory).name
    data_filetypes = {".csv", ".tsv", ".xls", ".xlsx"}
    source_files = [
        item
        for item in Path(directory).iterdir() 
        if item.suffix in data_filetypes and not item.name.startswith("~$")
    ]

    all_columns = []

    for source_file in source_files:
        if source_file.suffix == ".csv":
            with open(source_file, newline="", encoding="utf-8-sig", errors="replace") as f:
                reader = csv.reader(f)
                for row in reader:
                    if any(cell.strip() for cell in row):
                        column_names = [c.strip() for c in row]
                        break
        
        elif source_file.suffix == ".tsv":
            with open(source_file, newline="", encoding="utf-8-sig", errors="replace") as f:
                reader = csv.reader(f, delimiter="\t")
                column_names = [c.strip() for c in next(reader)]
        
        elif source_file.suffix in {".xls", ".xlsx"}:
            try:
                df = pd.read_excel(source_file, nrows=0, engine="openpyxl")
            except (ValueError, ImportError, BadZipFile, OSError):
                try:
                    df = pd.read_excel(source_file, nrows=0, engine="xlrd")
                except Exception as e:
                    print(f"Skipping {source_file}: {e}")
                    continue
            column_names = [c.strip() for c in df.columns]

        column_dictionary = {
            "dataset": this_dataset,
            "file": source_file.name,
            "columns": column_names
            }
        all_columns.append(column_dictionary)
    
    return all_columns

# probably the wrong direction
def file_from_section(directory):
    readme_dat = get_data_dictionaries(directory)
    sections = set([row["section"] for row in readme_dat])
    csvs = list(Path(directory).glob("*.csv"))
    column_lookup = {
        item["file"]: item["columns"]
        for item in get_active_columns(directory)
        }
    full_data = []
    for section in sections:
        best_score = 0
        best_file = None
        for file in csvs:
            csv_file = str(file.name)
            csv_name = csv_file.lower().removesuffix(".csv")
            csv_tokens = re.split(r'[^a-zA-Z0-9]+', csv_name)
            readme_cols = [r.lower() for r in [row["variable"] for row in readme_dat if row["section"] == section]]
            csv_cols = [c.lower() for c in column_lookup[csv_file]]
            answer_result = []
            for token in csv_tokens:
                answer = token in section.lower()
                answer_result.append(answer)
            matching_cols = len(set(readme_cols) & set(csv_cols))
            row_score = sum(answer_result) + matching_cols
            if row_score > best_score:
                best_score = row_score
                best_file = csv_file
        full_data.append({
            "section": section, 
            "file": best_file, 
            "score": best_score})
    return full_data

# probably what I want
def section_from_file(directory):
    readme_dat = get_data_dictionaries(directory)
    sections = set([row["section"] for row in readme_dat])
    data_filetypes = {".csv", ".tsv", ".xls", ".xlsx"}
    source_files = [
        item
        for item in Path(directory).iterdir() 
        if item.suffix in data_filetypes and not item.name.startswith("~$")]
    column_lookup = {
        item["file"]: item["columns"]
        for item in get_active_columns(directory)
        }
    full_data = []
    for file in source_files:
        source_file = str(file.name)
        source_name = Path(source_file).stem.lower()
        source_tokens = re.split(r'[^a-zA-Z0-9]+', source_name)
        best_score = 0
        best_section = None
        for section in sections:
            readme_cols = [r.lower() for r in [row["variable"] for row in readme_dat if row["section"] == section]]
            source_cols = [c.lower() for c in column_lookup[source_file]]
            answer_result = []
            for token in source_tokens:
                answer = token in section.lower()
                answer_result.append(answer)
            matching_cols = len(set(readme_cols) & set(source_cols))
            row_score = sum(answer_result) + matching_cols
            if row_score > best_score:
                best_score = row_score
                best_section = section
        full_data.append({
            "file": source_file, 
            "section": best_section, 
            "score": best_score})
    
    best_by_section = {}
    
    for row in full_data:
        sec = row["section"]
        if sec is None:
            continue
        
        if sec not in best_by_section or row["score"] > best_by_section[sec]["score"]:
            best_by_section[sec] = row
    
    full_data = list(best_by_section.values())
    
    return full_data

def build_dataset_index(directory):
    readme_rows = get_data_dictionaries(directory)
    active_columns = get_active_columns(directory)
    # section_file_map = file_from_section(directory)
    section_file_map = section_from_file(directory)
    section_to_file = {
        d["section"]: d["file"]
        for d in section_file_map
        }
    file_to_columns = {
        d["file"]: [normalize_variable(col) for col in d["columns"]]
        for d in active_columns
        }
    all_rows = []
    for item in readme_rows:
        this_file = section_to_file.get(item["section"])
        if this_file is None:
            continue

        doc_norm = Path(item["section"]).name.lower().replace("-", "_").strip("`") if isinstance(item["section"], str) and "." in item["section"] else None
        match_norm = Path(this_file).name.lower().replace("-", "_") if this_file else None
        
        match_status = (
            "missing" if doc_norm and not this_file else
            "exact" if doc_norm and doc_norm == match_norm else
            "unlikely" if doc_norm else
            "close"
            )
            
        this_row = {
            "dataset_id": item["dataset_id"],
            "dataset_title": item["dataset_title"],
            "readme_section": item["section"],
            "source": this_file,
            "source_match": match_status,
            "variable": item["variable"],
            "variable_norm": normalize_variable(item["variable"]),
            "variable_class": item.get("class"),
            "variable_description": item["description"],
            "variable_in_source": normalize_variable(item["variable"]) in file_to_columns[this_file]
            }
        all_rows.append(this_row)
    return all_rows

def normalize_variable(variable):
    result = unicodedata.normalize("NFKD", variable)
    result = result.lower().strip()
    result = re.sub(r"[^\w]+", "_", result)
    result = re.sub(r"_+", "_", result)

    return result.strip("_")

def infer_dataset_title(filename):
    maybe_title = filename.split("_")[0]
    if (maybe_title.startswith("week")):
        inferred_title = f"Week {maybe_title[4:]}"
    else:
        inferred_title = None
    return inferred_title

def infer_section_title(filename):
    relevant_parts = filename.removesuffix(".csv").removesuffix(".xls").removesuffix(".xlsx").split("_")[1:]
    inferred_title = " ".join(relevant_parts).title()
    return inferred_title

def infer_dataset_index(directory):
    active_columns = get_active_columns(directory)
    dataset_table = [
        {
            "dataset_id": d["dataset"],
            "dataset_title": infer_dataset_title(d["file"]),
            "readme_section": infer_section_title(d["file"]),
            "source": d["file"],
            "source_match": "exact",
            "variable": col,
            "variable_norm": normalize_variable(col),
            "variable_class": None,
            "variable_description": None,
            "variable_in_source": True,
        }
        for d in active_columns
        for col in d["columns"]
    ]

    return dataset_table

def loop_dataset_year(directory):
    directory_path = Path(directory) 
    subdirectories = sorted(
        [p for p in directory_path.iterdir() if p.is_dir()]
    )
    
    year_data = []
    for dir in subdirectories:
        readme_path = Path(str(dir) + "/readme.md")
        if readme_path.is_file():
            this_week = build_dataset_index(str(dir))
            if this_week is None:
                this_week = infer_dataset_index(str(dir))
        else:
            # print(f"The file {readme_path} does not exist")
            this_week = infer_dataset_index(str(dir))

        year_data.append(this_week)
    return year_data

def get_years(start=2018, end=2016, datadir=TT_DATA):
    all_data = [
        loop_dataset_year(f"{datadir}/{year}/")
        for year in range(start, end)
    ]
    
    all_flat = [
        row
        for year in all_data
        for week in year
        for row in week
    ]
    
    all_flat_sorted = sorted(all_flat, key=lambda x: x["dataset_id"])

    return all_flat_sorted

def get_years_columnar(start=2018, end=2016, datadir=TT_DATA):
    all_data = get_years(start, end, datadir)
    
    df_columns = pd.DataFrame(all_data)

    return df_columns
