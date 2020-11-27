#!/usr/bin/python3

import boto3
import logging
import os
from botocore.exceptions import ClientError
from File import File


class S3:

    def __init__(self, bucket='mediabackups'):
        ACCESS_KEY = os.getenv('ACCESS_KEY')
        SECRET_KEY = os.getenv('SECRET_KEY')
        ENDPOINT_URL = os.getenv('ENDPOINT_URL')
        REGION = os.getenv('REGION')

        session = boto3.session.Session()
        self.bucket = bucket

        self.client = session.client('s3',
                                     region_name=REGION,
                                     endpoint_url=ENDPOINT_URL,
                                     aws_access_key_id=ACCESS_KEY,
                                     aws_secret_access_key=SECRET_KEY,
                                     verify=False)  # temporary workaround for self-signed certs

    def upload_file(self, file_path, upload_name):
        """
        Uploads given local file file_path into the s3 location with the
        upload_name virtual path/identifier.
        """
        try:
            self.client.upload_file(file_path, self.bucket, upload_name)
        except ClientError as e:
            logging.error(e)
            return -1
        return 0

    def upload_dir(self, parent_dir, wiki):
        """
        Uploads of files withing the directory named as the wiki, located in the
        parent_dir directory, into the S3-backed-api service, with
        the wiki/filename/sha1 virtual file structure.
        Returns different from 0 if at least one upload failed.
        """
        file_dir = os.path.join(parent_dir, wiki)
        for filename in os.listdir(file_dir):
            print(filename)
            path = os.path.join(file_dir, filename)
            sha1 = File.sha1sum(path)
            result = self.upload_file(path, os.path.join(wiki, filename, sha1))
            if result != 0:
                exit_code = -1
        return exit_code
