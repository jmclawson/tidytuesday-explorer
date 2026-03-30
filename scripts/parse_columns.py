from parsing_utils import get_years_columnar
from duckdb_files import db_save_parquet

# Add dataset name with these rules
# start with the first header in the file
# if this first header is a level-1 header, it may be good
# if not, be prepared to replace with first level 1 header
# Do not keep "Data info"

tt_data = get_years_columnar(2018, 2026)

db_save_parquet(tt_data, "tt_columns.parquet")
