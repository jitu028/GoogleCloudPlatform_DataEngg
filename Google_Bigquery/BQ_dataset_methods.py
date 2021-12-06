from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from google.cloud.exceptions import NotFound

#https://blog.morizyun.com/python/library-bigquery-google-cloud.html

#create a new dataset by providing project and dataset_id
def create_new_dataset(client,project, dataset_id):
       
        dataset_ref = client.dataset(dataset_id)

        try:
            # Check if the dataset with specified ID already exists
            data = client.get_dataset(dataset_ref)
            return "dataset already exists {}".format(data)
            # log.info('Dataset {} already exists! Skipping create operation.'.format(dataset_id))
            #print(f'Dataset {dataset_id} already exists! Skipping create operation.')
        except NotFound:
            # Construct a full Dataset object to send to the API.
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = 'US'
            dataset = client.create_dataset(dataset)  # API request
            return "Created new dataset {}".format(dataset)
            # log.info('Dataset {} created successfully project: {}.'.format(dataset.dataset_id, self.client.project))


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

#Function to create a dataset in Table
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

#Function to export data into table in Bigquery
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
    project = "dca-sandbox-project-4"
    dataset_id = "001_demodata"
    data = bq_create_dataset(bigquery_client, "00_bigquery_00")
    table = bq_create_table(bigquery_client, "00_bigquery_00", "AB_table")
    export_items_to_bigquery(bigquery_client, "00_bigquery_00", "AB_table")
 

    # data = create_new_dataset(client,project,dataset_id)
    # print(data)