# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

import structlog

from pydiverse.pipedag.optional_dependency.boto import ClientError, boto3


def create_bucket_if_not_exists(bucket_name: str, s3_client, region: str | None = None) -> bool:
    logger = structlog.get_logger(__name__ + ":create_bucket_if_not_exists")
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        logger.info(f"Bucket '{bucket_name}' already exists.")
        return False
    except ClientError as e:
        error_code = int(e.response["Error"]["Code"])
        if error_code == 404:
            # Bucket does not exist, safe to create
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region} if region is not None else {},
            )
            logger.info(f"Bucket '{bucket_name}' created.")
            return True
        elif error_code == 403:
            logger.info(f"Bucket '{bucket_name}' exists but is owned by someone else.")
            return False
        else:
            raise


def initialize_test_s3_bucket(
    host: str = "127.0.0.1", port: str | int = 9000, test_bucket: str = "pipedag-test-bucket"
) -> bool:
    if boto3 is not None:
        minio_client = boto3.client(
            "s3",
            endpoint_url=f"http://{host}:{port}",
            aws_access_key_id="minioadmin",  # default MinIO credentials
            aws_secret_access_key="minioadmin",
            region_name="us-east-1",
            config=boto3.session.Config(s3={"addressing_style": "path"}),
        )
        return create_bucket_if_not_exists(test_bucket, minio_client)
    return False
