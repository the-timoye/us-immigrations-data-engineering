from airflow.hooks.postgres_hook import PostgresHook
import logging

def data_count_check(*args, **kwargs):
    """
        @description:
            This function runs a data count quality check on the specified table in redshift. 
            It compares the resulting number of rows with the expected value and throws an error if these values do not match.
        @params:
            table (STR): The Redshift table needed to be tested.
            expected_row_count (INT): The number of rows expected to be in the specified table
        @returns:
            ValueError (Error): if the number of rows do not match.
    """
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
    """
        @description:
            This function runs a null value quality check on the specified table, and column in redshift. 
            It checks if the specified column has a null value in it.
        @params:
            table (STR): The Redshift table needed to be tested.
            column_name (STR): The name of the column to check
        @returns:
            ValueError (Error): If at least one null value is found in the column
    """
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
    """
        @description:
            This function runs a column type quality check on the specified table in redshift. 
            This ensures columns are of the expected data types.
        @params:
            table (STR): The Redshift table needed to be tested.
            column_name (STR): The name of the column to check
            data_type (STR): Expected data type of the column. E.g. DATE, VARCHAR, INTEGER
        @returns:
            TypeError (Error): if the columns datatype do not match
    """
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