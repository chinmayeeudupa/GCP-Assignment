from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from pyparsing import basestring

def fetching_credentials_explicitly(key_file):
    # Explicitly use service account credentials by specifying the private key
    # file.
    storage_client = storage.Client.from_service_account_json(
        key_file)
    return storage_client

def download_blob(storage_client, bucket_name, source_blob_names, destination_file_names):
    """Downloads a blob from the bucket."""

    bucket = storage_client.bucket(bucket_name)

    # Construct a client side representation of a blob.
    # Note `Bucket.blob` differs from `Bucket.get_blob` as it doesn't retrieve
    # any content from Google Cloud Storage. As we don't need additional data,
    # using `Bucket.blob` is preferred here.
    for i in range(len(source_blob_names)):
        blob = bucket.blob(source_blob_names[i])
        blob.download_to_filename(destination_file_names[i])

    print(
        "Downloaded storage object {} from bucket {} to local file {}.".format(
            source_blob_names, bucket_name, destination_file_names
        )
    )

def bigquery_operations(key_file, destination_file_names):
    """Left join the Customer and Order files, create dataset and table, 
       populate the table with merged result"""

    # Read the csv files
    customers_file = pd.read_csv(destination_file_names[1]).astype(basestring)
    orders_file = pd.read_csv(destination_file_names[0]).astype(basestring)

    # Left join the files on Customer ID
    left_join_result = pd.merge(customers_file, orders_file, on='CustomerID', how='left')

    # Construct a BigQuery client object.
    credentials = service_account.Credentials.from_service_account_file(
        key_file, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    client = bigquery.Client(credentials=credentials, project=credentials.project_id,)

    # Set dataset_id to the ID of the dataset to create.
    dataset_id = "{}.customerOrderDataset".format(client.project)

    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)

    # Specify the geographic location where the dataset should reside.
    dataset.location = "US"

    # Send the dataset to the API for creation, with an explicit timeout.
    # Raises google.api_core.exceptions.Conflict if the Dataset already
    # exists within the project.
    dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
    print("Created dataset {}.{}".format(client.project, dataset.dataset_id))

    table_id = "{}.merged_table".format(dataset_id)

    # Create schema
    schema = [
        bigquery.SchemaField("CustomerID", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("CustomerName", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("ContactName", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("Address", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("City", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("PostalCode", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("Country", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("OrderID", "STRING"),
        bigquery.SchemaField("EmployeeID", "STRING"),
        bigquery.SchemaField("OrderDate", "STRING"),
        bigquery.SchemaField("ShipperID", "STRING")
    ]

    # Create table
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )

    # Load data from merged file to the table
    job_config = bigquery.LoadJobConfig(schema=schema, 
        # Optionally, set the write disposition. BigQuery appends loaded rows
        # to an existing table by default, but with WRITE_TRUNCATE write
        # disposition it replaces the table with the loaded data.
        write_disposition='WRITE_TRUNCATE',)

    job = client.load_table_from_dataframe(
        left_join_result, table_id, job_config=job_config
    ) # Make an API request.
    job.result()

def main():
    key_file = 'sublime-calling-325912-5180a3c41cc6.json'
    storage_client = fetching_credentials_explicitly(key_file)
    bucket_name = 'assignment_files_upload_bucket'
    source_blob_names = ['Customers.csv','Orders.csv']
    destination_file_names = ['/Users/chinmayeeudupagn/Downloads/gcp assignment/DownloadedOrders.csv', '/Users/chinmayeeudupagn/Downloads/gcp assignment/DownloadedCustomers.csv']
    download_blob(storage_client, bucket_name, source_blob_names, destination_file_names)
    bigquery_operations(key_file, destination_file_names)

if __name__=="__main__":
    main()
