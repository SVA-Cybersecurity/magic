#!/usr/bin/env python
# -*- coding: utf-8 -*-

# -------------------------------------------------- #
# METADATA                                           #
# -------------------------------------------------- #
__author__ = "Alexander Goedeke"
__version__ = "0.5.0"


# -------------------------------------------------- #
# IMPORTS                                            #
# -------------------------------------------------- #
import os
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from ..interfaces.enricher import BaseEnricher
from ..helpers.utils import TaskWrapper
from ..helpers.registry import register_enricher


@register_enricher(name="s3_upload")
class S3Upload(BaseEnricher):
    """
    Enricher for uploading files to an AWS S3 bucket.

    Configuration options in settings.s3:
    - endpoint_url: Custom S3 endpoint (optional, for S3-compatible services)
    - aws_access_key_id: AWS Access Key
    - aws_secret_access_key: AWS Secret Access Key
    - verify_ssl: SSL certificate verification (default: True)

    Configuration options in enrich.s3_upload:
    - enabled: Enable/disable the enricher
    - bucket_path: Target path in bucket (e.g. folder/)
    - input_filename: Source file to upload (default: timesketch.jsonl)
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs, logger=__name__)

    def get_tasks(self):
        if self._is_enabled():
            self.logger.debug(f"Create s3_upload task with params: {self.config.model_dump()}")
            return [TaskWrapper(name="s3_upload", coroutine=self.upload_to_s3())]
        return []

    async def upload_to_s3(self):
        """Uploads the configured file to the S3 bucket."""

        # Check if S3 settings are configured
        if self.settings.s3 is None:
            self.logger.error("S3 settings are not configured. Please add an 's3' section to your config file.")
            return

        # Determine source file
        source_file = os.path.join(self.output_dir, self.config.input_filename)

        if not os.path.exists(source_file):
            self.logger.error(f"Source file {source_file} not found. Cannot upload to S3.")
            return

        try:
            # Configure S3 client
            s3_config = {
                'aws_access_key_id': self.settings.s3.aws_access_key_id,
                'aws_secret_access_key': self.settings.s3.aws_secret_access_key,
            }

            # Add optional parameters
            if self.settings.s3.endpoint_url:
                s3_config['endpoint_url'] = self.settings.s3.endpoint_url

            # Configure SSL verification
            if not self.settings.s3.verify_ssl:
                s3_config['verify'] = False
                self.logger.warning("SSL verification is disabled!")

            self.logger.debug(f"Initializing S3 client")
            s3_client = boto3.client('s3', **s3_config)

            # Extract bucket name and path
            bucket_path = self.config.bucket_path.strip('/')

            # Bucket name is the first part of the path
            # e.g. "my-bucket/path/to/file" -> bucket="my-bucket", key="path/to/file/filename"
            if '/' in bucket_path:
                bucket_name = bucket_path.split('/')[0]
                object_path = '/'.join(bucket_path.split('/')[1:])
            else:
                bucket_name = bucket_path
                object_path = ''

            # Add filename
            filename = os.path.basename(source_file)
            if object_path:
                object_key = f"{object_path}/{filename}"
            else:
                object_key = filename

            self.logger.info(f"Uploading {source_file} to s3://{bucket_name}/{object_key}")

            # Test bucket access
            s3_client.head_bucket(Bucket=bucket_name)

            self.logger.info(
                f"Using S3 endpoint {s3_client.meta.endpoint_url} and access key ID {'*' * 16}{s3_client._get_credentials().access_key[16:]}"
            )

            # Perform upload
            s3_client.upload_file(Filename=source_file, Bucket=bucket_name, Key=object_key)

            self.logger.info(f"Successfully uploaded {filename} to S3 bucket {bucket_name}")

        except NoCredentialsError:
            self.logger.error("AWS credentials not found or invalid")
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            error_message = e.response.get('Error', {}).get('Message', str(e))
            self.logger.error(f"S3 upload failed with error {error_code}: {error_message}")
        except Exception as e:
            self.logger.error(f"Unexpected error during S3 upload: {str(e)}")
