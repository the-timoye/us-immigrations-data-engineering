def remane_columns(df, columns_dictionary):
    new_df = df
    for key, value in columns_dictionary.items():
        new_df = new_df.withColumnRenamed(key, value);
    return new_df