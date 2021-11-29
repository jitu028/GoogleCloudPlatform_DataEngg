from google.cloud import bigquery
from google.cloud.exceptions import NotFound

#https://blog.morizyun.com/python/library-bigquery-google-cloud.html

#Function to create a dataset in Bigquery
def bq_create_dataset(client, dataset):
    dataset_ref = bigquery_client.dataset(dataset)

    try:
        dataset = bigquery_client.get_dataset(dataset_ref)
        print('Dataset {} already exists.'.format(dataset))
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset = bigquery_client.create_dataset(dataset)
        print('Dataset {} created.'.format(dataset.dataset_id))
    return dataset

def bq_create_table(client, dataset, table_name):
    dataset_ref = bigquery_client.dataset(dataset)

    # Prepares a reference to the table
    table_ref = dataset_ref.table(table_name)

    try:
        table =  bigquery_client.get_table(table_ref)
        print('table {} already exists.'.format(table))
    except NotFound:
        schema = [
            bigquery.SchemaField('Day', 'INTEGER', mode='REQUIRED'),
            bigquery.SchemaField('Conversion_A', 'INTEGER', mode='REQUIRED'),
            bigquery.SchemaField('Conversion_B', 'INTEGER', mode='REQUIRED'),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table = bigquery_client.create_table(table)
        print('table {} created.'.format(table.table_id))
    return table

def export_items_to_bigquery(client, dataset, table):
    

    # Prepares a reference to the dataset
    dataset_ref = bigquery_client.dataset(dataset)

    table_ref = dataset_ref.table(table)
    table = bigquery_client.get_table(table_ref)  # API call

    rows_to_insert = [
        (1, 32, 32),
        (2, 64, 29),
        (3, 108, 108)
    ]
    errors = bigquery_client.insert_rows(table, rows_to_insert)  # API request
    assert errors == []

if __name__ == "__main__":
    bigquery_client = bigquery.Client()
    data = bq_create_dataset(bigquery_client, "00_bigquery_00")
    table = bq_create_table(bigquery_client, "00_bigquery_00", "AB_table")
    value_table = export_items_to_bigquery(bigquery_client, "00_bigquery_00", "AB_table")

