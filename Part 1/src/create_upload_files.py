from google.cloud import storage

def fetching_credentials_explicitly():
    # Explicitly use service account credentials by specifying the private key
    # file.
    storage_client = storage.Client.from_service_account_json(
        'sublime-calling-325912-5180a3c41cc6.json')
    return storage_client

def create_bucket_class_location(storage_client, bucket_name):
    """Create a new bucket in specific location with storage class"""

    bucket = storage_client.bucket(bucket_name)
    bucket.storage_class = "COLDLINE"
    new_bucket = storage_client.create_bucket(bucket, location="us")

    print(
        "Created bucket {} in {} with storage class {}".format(
            new_bucket.name, new_bucket.location, new_bucket.storage_class
        )
    )

def upload_blob(storage_client, bucket_name, source_file_names, destination_blob_names):
    """Upload the files to the cloud bucket"""
    bucket = storage_client.bucket(bucket_name)

    for i in range(len(source_file_names)):
        blob = bucket.blob(destination_blob_names[i])
        blob.upload_from_filename(source_file_names[i])

    print(
        "File {} uploaded to {}.".format(
            source_file_names, destination_blob_names
        )
    )
    

def main():
    storage_client = fetching_credentials_explicitly()
    bucket_name = 'assignment_files_upload_bucket'
    source_file_names = ['/Users/chinmayeeudupagn/Downloads/gcp assignment/Orders.csv', '/Users/chinmayeeudupagn/Downloads/gcp assignment/Customers.csv']
    destination_blob_names = ['Customers.csv','Orders.csv']
    create_bucket_class_location(storage_client, bucket_name)
    upload_blob(storage_client, bucket_name, source_file_names, destination_blob_names)

if __name__=="__main__":
    main()

