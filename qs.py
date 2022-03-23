def cache(func):
    def wrapped_func(*args, **kwargs):
        from datetime import date

        table_name = func.__name__.split("_")[-1]
        parquet_path = f"./data/{table_name}.parquet"
        try:
            table = pd.read_parquet(parquet_path)
        except FileNotFoundError:
            table = func(*args, **kwargs)
            table.to_parquet(parquet_path)
        return table

    return wrapped_func

def query(sql_stmt, params, wrds_username):
    import wrds

    with wrds.Connection(wrds_username=wrds_username) as db:
        data = db.raw_sql(sql_stmt, date_cols=["date"], params=params)
    return data

@cache
def get_crsp(permnos, wrds_username):
    sql_crsp = """
    SELECT DISTINCT
      date,
      permno,
      ret,
      prc,
      prc * shrout AS cap
    FROM crsp.dsf
    WHERE permno in %(permnos)s
    AND date >= '1997-01-01'
    AND date <= '2019-12-31';
    """
    params = {"permnos": permnos}
    crsp = query(sql_crsp, params, wrds_username).astype({"permno":"int"})
    return crsp
