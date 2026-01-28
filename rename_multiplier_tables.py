import boto3
import logging
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# AWS Glue client setup
glue_client = boto3.client('glue')

def copy_tables_with_new_names(database_name, table_mappings):
    """
    Copy tables with new names within the same database while preserving original tables.
    
    :param database_name: Target database name
    :param table_mappings: Dictionary of {old_table_name: new_table_name}
    """
    try:
        total_tables_copied = 0
        total_tables_failed = 0
        
        for old_table_name, new_table_name in table_mappings.items():
            try:
                # Retrieve the existing table metadata
                response = glue_client.get_table(
                    DatabaseName=database_name, 
                    Name=old_table_name
                )
                table = response['Table']
                
                # Prepare new table input
                new_table_input = {
                    key: value for key, value in table.items() 
                    if key in ['StorageDescriptor', 'PartitionKeys', 'TableType', 'Parameters', 'Description']
                }
                new_table_input['Name'] = new_table_name
                
                # Check if the new table already exists
                try:
                    glue_client.get_table(DatabaseName=database_name, Name=new_table_name)
                    logger.warning(f"Table {new_table_name} already exists in {database_name}. Skipping.")
                    continue
                except glue_client.exceptions.EntityNotFoundException:
                    # Create the new table
                    glue_client.create_table(
                        DatabaseName=database_name, 
                        TableInput=new_table_input
                    )
                    logger.info(f"Successfully created new table {new_table_name} as a copy of {old_table_name}")
                    total_tables_copied += 1
            
            except ClientError as e:
                logger.error(f"Failed to copy table {old_table_name}: {e}")
                total_tables_failed += 1
        
        # Summary logging
        logger.info(f"Table Copy Complete - Total Copied: {total_tables_copied}, Total Failed: {total_tables_failed}")
        
        return total_tables_copied, total_tables_failed
    
    except Exception as e:
        logger.error(f"Unexpected error during table copy: {e}")
        raise

def main():
    # Specify the table mappings
    table_mappings = {
        # these are table names from crawling oedi data lake, created empty tables
        # "com_annual_multipliers_amy_parquet": "com_annual_disaggregation_multipliers_amy",
        # "res_annual_multipliers_amy_parquet": "res_annual_disaggregation_multipliers_amy",
        # these are table names from crawling com_annual_multipliers_amy folder in my bucket
        # "com_annual_multipliers_amy": "com_annual_disaggregation_multipliers_amy",
        # "res_annual_multipliers_amy": "res_annual_disaggregation_multipliers_amy",
        # "com_hourly_multipliers_amy": "com_hourly_disaggregation_multipliers_amy",
        # "res_hourly_multipliers_amy": "res_hourly_disaggregation_multipliers_amy"
        # these are generated multiplier tables from the original code, don't need this when the version suffix are consistent
        "com_annual_disaggregation_multipliers": "com_annual_disaggregation_multipliers_amy",
        "res_annual_disaggregation_multipliers": "res_annual_disaggregation_multipliers_amy",
        "com_hourly_disaggregation_multipliers": "com_hourly_disaggregation_multipliers_amy",
        "res_hourly_disaggregation_multipliers": "res_hourly_disaggregation_multipliers_amy"
    }
    
    # database_name = "default2"
    # database_name = "baseline"
    database_name = "rerun"
    
    try:
        copied, failed = copy_tables_with_new_names(database_name, table_mappings)
        print(f"Copied {copied} tables. {failed} tables failed to copy.")
    except Exception as e:
        print(f"Failed to copy tables: {e}")

if __name__ == "__main__":
    main()
