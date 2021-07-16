copy_statement = """
    COPY {table_name}
    FROM "s3://{config['S3']['BUCKET']}"
    ACCESS_KEY_ID '{config[AWS]['KEY]}'
    SECRET_ACCESS_KEY '{config['AWS']['SECRET']}'
    FORMAT AS {format}
    REGION {config['REDSHIFT']['REGION']}
"""