"""
Iceberg Handler Module
Manages Iceberg table initialization and statistics with AWS Glue catalog
"""

import logging
import os
from typing import Dict, Any
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema, NestedField
from pyiceberg.types import StringType, IntegerType, TimestampType, ListType, LongType
from pyiceberg.partitioning import PartitionSpec, PartitionField

# Setup logging
logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# AWS Configuration - hardcoded credentials
AWS_ACCESS_KEY_ID = "ASIAZYHN7XI6SJHG2IYS"
AWS_SECRET_ACCESS_KEY = "J1gUJkFCD56VKhyjkC8Ema+RfuwwAxphvS8GC3Jq"
AWS_SESSION_TOKEN = "FwoGZXIvYXdzEOj//////////wEaDGRdqp1tmWssuWSvziLWAX68UXEWe+GYyRaQpdTvG2CYABGE1z2YuUAham+71MnXE+o/dM/qERvUrbkFRg6lfFOILRytUbr/PwiWCdPYad9s5uK+uTzRucOFxpo8lNbD8LUnwIoLiKkA5DdHxK/qsrLPaQX0de4LUvNhBzW7qarP5rLm0G67CmW4lWmfvhp2xcF0CXZWRgk0UkJ+5DaNdvMnOz6IuQQUaAtQlpOZ9i8KuydmOYlk/5b5ybyvdme1vf0oD7iIMQaDdDlN6vCzc7p7VYQPT1vBQwEkF8BBrQcfUa4grGso2LXfxQYyM0qC+4aDBNUmrXGXr5s8ngKDmYfrENGAQAWd50UU3gvU8et5rkUhtXOjY8Q8JweFHHAzcA=="
AWS_REGION = "us-east-1"
S3_WAREHOUSE_PATH = "s3://batch-transpiler/testing-batch-processing/"
ICEBERG_CATALOG_NAME = os.getenv("ICEBERG_CATALOG_NAME", "glue_catalog")

# Global variables
iceberg_catalog = None
batch_statistics_table = None


def initialize_iceberg_catalog():
    """Initialize AWS Glue Iceberg catalog and create tables if they don't exist"""
    global iceberg_catalog, batch_statistics_table

    try:
        logger.info(f"Initializing AWS Glue catalog with S3 warehouse: {S3_WAREHOUSE_PATH}")

        # Initialize AWS Glue catalog using pyiceberg load_catalog function
        iceberg_catalog = load_catalog(
            name=ICEBERG_CATALOG_NAME,
            **{
                "type": "glue",
                "s3.access-key-id": AWS_ACCESS_KEY_ID,
                "s3.secret-access-key": AWS_SECRET_ACCESS_KEY,
                "s3.session-token": AWS_SESSION_TOKEN,
                "s3.region": AWS_REGION,
                "warehouse": S3_WAREHOUSE_PATH,
                "glue.region": AWS_REGION,
                "glue.access-key-id": AWS_ACCESS_KEY_ID,
                "glue.secret-access-key": AWS_SECRET_ACCESS_KEY,
                "glue.session-token": AWS_SESSION_TOKEN,
            },
        )

        logger.info(f"AWS Glue catalog initialized successfully")

        # Define batch statistics table schema with partitioning fields
        batch_stats_schema = Schema(
            NestedField(1, "query_id", LongType(), required=False),
            NestedField(2, "batch_id", StringType(), required=False),
            NestedField(3, "company_name", StringType(), required=False),  # Partition field
            NestedField(
                4, "event_date", StringType(), required=False
            ),  # Partition field (format: YYYY-MM-DD)
            NestedField(
                5, "batch_number", IntegerType(), required=False
            ),  # Batch number for file naming
            NestedField(6, "timestamp", TimestampType(), required=False),
            NestedField(7, "status", StringType(), required=False),
            NestedField(8, "executable", StringType(), required=False),
            NestedField(9, "from_dialect", StringType(), required=False),
            NestedField(10, "to_dialect", StringType(), required=False),
            NestedField(11, "original_query", StringType(), required=False),
            NestedField(12, "converted_query", StringType(), required=False),
            NestedField(
                13,
                "supported_functions",
                ListType(element_id=20, element_type=StringType(), element_required=False),
                required=False,
            ),
            NestedField(
                14,
                "unsupported_functions",
                ListType(element_id=21, element_type=StringType(), element_required=False),
                required=False,
            ),
            NestedField(
                15,
                "udf_list",
                ListType(element_id=22, element_type=StringType(), element_required=False),
                required=False,
            ),
            NestedField(
                16,
                "tables_list",
                ListType(element_id=23, element_type=StringType(), element_required=False),
                required=False,
            ),
            NestedField(17, "processing_time_ms", LongType(), required=False),
            NestedField(18, "error_message", StringType(), required=False),
            NestedField(
                19,
                "unsupported_functions_after_transpilation",
                ListType(element_id=24, element_type=StringType(), element_required=False),
                required=False,
            ),
            NestedField(
                20,
                "joins_list",
                ListType(element_id=25, element_type=StringType(), element_required=False),
                required=False,
            ),
        )

        # Define partition specification for company_name and event_date
        partition_spec = PartitionSpec(
            PartitionField(source_id=3, field_id=1000, transform="identity", name="company_name"),
            PartitionField(source_id=4, field_id=1001, transform="identity", name="event_date"),
        )

        # Create namespace if it doesn't exist
        namespace = "default"
        try:
            iceberg_catalog.create_namespace(namespace)
            logger.info(f"Created namespace: {namespace}")
        except Exception as ns_error:
            logger.info(f"Namespace {namespace} already exists or creation failed: {ns_error}")

        # Load existing batch statistics table
        table_identifier = f"{namespace}.batch_statistics"
        try:
            # Load existing table from S3
            batch_statistics_table = iceberg_catalog.load_table(table_identifier)
            logger.info(f"Successfully loaded existing Iceberg table: {table_identifier}")

            # Log table schema for verification
            existing_columns = [field.name for field in batch_statistics_table.schema().fields]
            logger.info(f"Table schema columns: {existing_columns}")

        except Exception as e:
            logger.error(f"Failed to load existing Iceberg table {table_identifier}: {str(e)}")
            logger.info(
                "The table should exist at s3://batch-transpiler/testing-batch-processing/default/batch_statistics/"
            )
            batch_statistics_table = None

        logger.info("AWS Glue Iceberg catalog and tables initialized successfully")
        return True

    except Exception as e:
        logger.error(f"Failed to initialize AWS Glue Iceberg catalog: {str(e)}")
        logger.error(f"Error details: {type(e).__name__}")
        return False


# Don't initialize on module import - causes segfault with forking
# Initialize only in worker processes when needed
# initialize_iceberg_catalog()
