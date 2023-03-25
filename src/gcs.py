import os
import logging
from pathlib import Path
from google.cloud import storage

# TODO: cloud loggingにも飛ばす設定をする
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(
    logging.Formatter(
        "[%(asctime)s] [%(name)s] [L%(lineno)d] [%(levelname)s][%(funcName)s] %(message)s "
    )
)
logger.addHandler(handler)
logger.propagate = False


class GCSClient(object):
    def __init__(self, project=None):
        self.client = storage.Client(project)

    def upload_blob(self, bucket_name, source_file_name, destination_blob_name):
        """Uploads a file to the bucket."""
        bucket = self.client.get_bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name, timeout=None)
        logger.info(
            "File {} uploaded to {}.".format(source_file_name, destination_blob_name)
        )

    def upload_directory(self, bucket_name, source_dir, destination_dir):
        bucket = self.client.get_bucket(bucket_name)
        assert os.path.isdir(source_dir)
        for local_file in Path(source_dir).glob("*"):
            local_file = str(local_file)
            if not os.path.isfile(local_file):
                self.upload_directory(
                    bucket_name,
                    local_file,
                    destination_dir + "/" + os.path.basename(local_file),
                )
            else:
                remote_path = os.path.join(
                    destination_dir, os.path.basename(local_file)
                )
                blob = bucket.blob(remote_path)
                blob.upload_from_filename(local_file)

    def download_directory(self, bucket_name, source_dir, destination_dir):
        fetch_list_blobs = self.fetch_list_blobs(bucket_name, source_dir)
        for blob in fetch_list_blobs:
            dest_path = Path(destination_dir) / Path(blob).relative_to(source_dir)
            if not (dest_path.parent).exists():
                dest_path.parent.mkdir(parents=True)
            self.download_blob(bucket_name, blob, dest_path)

    def copy_blob(
        self, bucket_name, blob_name, destination_bucket_name, destination_blob_name
    ):
        """Copies a blob from one bucket to another with a new name."""
        src_bucket = self.client.get_bucket(bucket_name)
        src_blob = src_bucket.blob(blob_name)
        dst_bucket = self.client.get_bucket(destination_bucket_name)
        blob_copy = src_bucket.copy_blob(src_blob, dst_bucket, destination_blob_name)
        logger.info("Blob {} copied to blob {}.".format(src_blob.name, blob_copy.name))

    def download_blob(self, bucket_name, source_blob_name, destination_file_name):
        """Downloads a blob from the bucket."""
        bucket = self.client.get_bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        blob.download_to_filename(destination_file_name)
        logger.info(
            "Blob {} downloaded to {}.".format(source_blob_name, destination_file_name)
        )

    def delete_blob(self, bucket_name, blob_name):
        """Delete a blob in the bucket."""
        bucket = self.client.get_bucket(bucket_name)
        bucket.delete_blob(blob_name)

    def fetch_list_blobs(self, bucket_name, prefix=None, delimiter=None):
        """Fetch the name of blobs in the bucket."""
        bucket = self.client.get_bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=prefix, delimiter=delimiter)
        return [blob.name for blob in blobs]

    def exists(self, bucket_name, prefix=None, delimeter=None):
        blobs_list = self.fetch_list_blobs(bucket_name, prefix, delimeter)
        return len(blobs_list) > 0
