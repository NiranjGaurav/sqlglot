"""
Iceberg Handler Module
Manages Iceberg table initialization and statistics with AWS Glue catalog
"""

import logging
import os
from pyiceberg.catalog import load_catalog

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# AWS Configuration - hardcoded credentials  
AWS_ACCESS_KEY_ID = ""
AWS_SECRET_ACCESS_KEY = ""
AWS_SESSION_TOKEN = ""
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
        
        # Set AWS credentials via environment variables (PyIceberg 0.7.0 works better this way)
        os.environ['AWS_ACCESS_KEY_ID'] = AWS_ACCESS_KEY_ID
        os.environ['AWS_SECRET_ACCESS_KEY'] = AWS_SECRET_ACCESS_KEY
        os.environ['AWS_SESSION_TOKEN'] = AWS_SESSION_TOKEN
        os.environ['AWS_DEFAULT_REGION'] = AWS_REGION
        
        # Initialize AWS Glue catalog using pyiceberg load_catalog function
        # Use minimal configuration with environment variables
        iceberg_catalog = load_catalog(
            name=ICEBERG_CATALOG_NAME,
            type="glue",
            warehouse=S3_WAREHOUSE_PATH,
            region=AWS_REGION
        )
        
        logger.info(f"AWS Glue catalog initialized successfully")

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
            logger.info("The table should exist at s3://batch-transpiler/testing-batch-processing/default/batch_statistics/")
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