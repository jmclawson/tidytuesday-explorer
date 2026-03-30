import duckdb

def db_save_parquet(tbl, filename):
    with duckdb.connect() as con:
        con.register("tmp", tbl)
        con.execute(f"COPY tmp TO '{filename}' (FORMAT PARQUET)")

# usage: db_save_parquet(df_columns, "tt_columns.parquet")
