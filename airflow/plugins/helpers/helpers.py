def remane_columns(df, columns_dictionary):
    """
        @description:
            This function changes the name of columns specified in a dictionary.
            The key in the dictionary shoud be the old column name, and the value, the new column name.
        @params:
            df (DataFrame): The Spark dataframe to be manipulated.
            columns_dictionary (DICT): A dictionary containing old column names (Key) and new column names (Value)
        @returns:
            A new DataFrame with the columns changed
    """
    new_df = df
    for key, value in columns_dictionary.items():
        new_df = new_df.withColumnRenamed(key, value);
    return new_df