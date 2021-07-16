from model.models import model_data
from model.models import tables as table_names
# from airflow.hooks.S3_hook import S3_hook


class Tables:
    """
        @description: 
            creates an object of tables
            index of each array should correspond with the inputs.
            For example: table_names[0] should equal datasets[0]
        @params:
            table(STR): name of the table
            dataset(DataFrame): dataset, corresponding with the table name
        @method:
            iterates through the table names to assign tables to their respective datasets
    """
    def __init__(self, name, dataset):
        self.name = name;
        self.dataset = dataset;

    def create_tables(datasets):
        tables = []
        for i in range(0, len(table_names)):
            tables.insert(i, Tables(table_names[i], datasets[i]))
        return tables


def load_to_s3():
    datasets = model_data()
    tables = Tables.create_tables(datasets);
    
    for table in tables:
        print(f'========================================= WRITING {table.name.upper()} TABLE TO S3 =========================================')
        # table.dataset.write.mode('append').parquet(f"s3a://{config['S3']['BUCKET']}/{table.name}.parquet")
        table.dataset.write.mode('overwrite').csv(f"datasets/output/{table.name}.csv")
        # table.dataset.show(5)
        # print(table.dataset.count())
    print('done');