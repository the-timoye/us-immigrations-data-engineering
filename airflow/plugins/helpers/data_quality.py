from airflow.hooks.postgres_hook import PostgresHook
import logging

def data_count_check(*args, **kwargs):
    table = kwargs["params"]["table"]
    expected_row_count = kwargs["params"]["expected_row_count"]
    redshift_hook = PostgresHook("redshift_conn_id")
    records = redshift_hook.get_records(f"""
        SELECT COUNT(*)
        FROM {table}
    """)

    if records[0][0] < expected_row_count:
        raise ValueError(f"Expected row count for {table} to be {expected_row_count}, found {records[0][0]}");

    logging.info(f"Data count check passed with number of records = ", records[0][0])

def null_value_check(*args, **kwargs):
    table = kwargs["params"]["table"]
    column_name = kwargs["params"]["column_name"]
    redshift_hook = PostgresHook("redshift_conn_id")
    records = redshift_hook.get_records(f"""
        SELECT COUNT(*)
        FROM {table}
        WHERE {column_name} IS NULL
    """)

    if records[0][0] > 0:
        raise ValueError(f"Expected null value count for {table} to be 0, found {records[0][0]}");

    logging.info(f"Data count check passed no null values!")

def column_type_check(*args, **kwargs):
    table = kwargs["params"]["table"]
    column_name = kwargs["params"]["column_name"]
    data_type = kwargs["params"]["data_type"]
    redshift_hook = PostgresHook("redshift_conn_id")
    records = redshift_hook.get_records(f"""
        select "column", type
        from pg_table_def
        where tablename = '{table}'
    """)
    for index in range(len(records)):
        if records[index] == column_name:
            if records[index][0] != data_type:
                raise TypeError(f"Invalid column data type. Expected {data_type}, found {records[index][0]}")


    logging.info(f"Records", records)