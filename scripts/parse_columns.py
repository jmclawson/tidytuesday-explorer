import argparse

from parsing_utils import get_years_columnar
from duckdb_files import db_save_parquet

# Add dataset name with these rules
# start with the first header in the file
# if this first header is a level-1 header, it may be good
# if not, be prepared to replace with first level 1 header
# Do not keep "Data info" (or similars) as readme_section

# DATA_DIR = "tt_data"
# OUTPUT_FILE = "tt_columns.parquet"

parser = argparse.ArgumentParser()
parser.add_argument("--data-dir", required=True)
parser.add_argument("--output", required=True)
args = parser.parse_args()

DATA_DIR = args.data_dir
OUTPUT_FILE = args.output

tt_columns = get_years_columnar(start=2018, end=2026, datadir=DATA_DIR)

db_save_parquet(tt_columns, OUTPUT_FILE)
