import bauplan
import datetime
import re
from data_quality_tests import are_there_nulls, expect_column_values_to_be_unique


def get_import_branch_name(branch_name: str):
    """Generate a unique branch name with timestamp."""
    timestamp = datetime.datetime.now().isoformat(timespec='seconds').replace(":", "_")
    return f"{branch_name}_{timestamp}"


def extract_table_name(filename):
    """Extract the table name from the filename."""
    pattern = r'zilliant-demo-data-\d{4}-\d{2}-\d{2}-(.*?)\.csv'
    match = re.search(pattern, filename)
    return match.group(1) if match else None


def import_data_in_iceberg(
        client: bauplan.Client,
        table_name: str,
        ref_branch: str,
        source_s3: str,
        namespace: str,
):
    """
    Imports data into an Iceberg table in Bauplan, creating or replacing the table as needed.

    This function checks if the specified table already exists in the given branch.
    If it does, the table is deleted and recreated. The function then imports data
    from the specified S3 location into the newly created table.

    Parameters:
    :param client bauplan.Client
    :param table_name str - The name of the Iceberg table to create or replace.
    :param ref_branch str - The branch in which the table will be created or replaced.
    :param source_s3_location str - The S3 location containing the source data for import.
    :param namespace str - The namespace under which the table will be created.
    :return None

    """
    try:
        client.create_table(table=table_name, search_uri=source_s3, branch=ref_branch, namespace=namespace, replace=True)
        print(f"✅ Table '{table_name}' created successfully.")
        client.import_data(table=table_name, search_uri=source_s3, branch=ref_branch, namespace=namespace)
        print(f"✅ Data imported in '{table_name}'.")

    except bauplan.exceptions.BauplanError as e:
        print(f"Error: {e}")
        raise Exception('🔴 The import did not work correctly')


def from_raw_to_staging(
        bpln_client: bauplan.Client,
        s3_source_folder: str,
        list_of_tables_to_import: list,
        namespace: str

):
    """
    Imports raw data files from S3 into a staging area using a dedicated import branch and namespace.
    Steps:
      1. Creates a raw import branch (with a timestamp).
      2. Creates the specified namespace if it does not exist.
      3. Imports each file (from list_of_tables_to_import) as an Iceberg table from s3_source_folder.
      4. Runs data quality tests on the 'transaction_line_item' table:
         - Ensures 'line_total' contains no nulls.
         - Checks that 'transaction_line_item_id' values are unique.
      5. Merges the import branch into 'main' if all tests pass.

    Parameters:
    :param  bpln_client: bauplan.Client - The Bauplan client instance.
    :param  s3_source_folder: str - S3 folder containing raw data files.
    :param  list_of_tables_to_import: list - Filenames to import.
    :param  namespace: str - Namespace for the imported tables.

    Raises:
    AssertionError: if any data quality test fails.

    """
    # Create a raw import branch with a timestamp in the name

    raw_import_branch = get_import_branch_name(branch_name='ciro.zilliant_upload')
    bpln_client.create_branch(branch=raw_import_branch, from_ref='main')
    print(f"✅ Branch '{raw_import_branch}' created.")

    # create a namespace
    if not bpln_client.has_namespace(namespace=namespace, ref=raw_import_branch):
        namespace = bpln_client.create_namespace(namespace=namespace, branch=raw_import_branch)
        print(f"✅: Namespace 'zilliant' created successfully.")

    # import the files as Icberg tables into the import branch
    for filename in list_of_tables_to_import:
        table_name = extract_table_name(filename)
        s3_source = f'{s3_source_folder}{filename}'
        import_data_in_iceberg(
            client=bpln_client,
            table_name=table_name,
            ref_branch=raw_import_branch,
            source_s3=s3_source,
            namespace=namespace
        )

    # Run data quality tests on the newly created tables before merging them into the main branch
    print(f"👀: Running data quality tests...")

    # Check that there are no null values in the column transaction_line_item in the table transaction_line_item
    # stop the pipeline from running if the test fails by asserting the test
    _are_there_null_line_total = are_there_nulls(
        client=bpln_client,
        table_name='transaction_line_item',
        column_to_check='line_total',
        ingestion_branch=raw_import_branch,
        namespace=namespace,
    )
    print(f'Are there nulls "line_total" in table "transaction_line_item"? {_are_there_null_line_total}')
    assert not _are_there_null_line_total

    # Check that the values of the colum transaction_line_item_id in the table transaction_line_item are unique
    # stop the pipeline from running if the test fails by asserting the test
    _are_transaction_ids_unique = expect_column_values_to_be_unique(
        client=bpln_client,
        table_name='transaction_line_item',
        column_to_check='transaction_line_item_id',
        ingestion_branch=raw_import_branch,
        namespace=namespace
    )
    print(f'Are transaction Ids in table "transaction_line_item" all unique? {_are_transaction_ids_unique}')
    assert _are_transaction_ids_unique

    # merge the import branch into the main branch
    bpln_client.merge_branch(source_ref=raw_import_branch, into_branch='main')
    print(f"✅ Branch '{raw_import_branch}' merged into main.")


def from_staging_to_applications(
        bpln_client: bauplan.Client,
        pipeline_folder: str,
        namespace: str,
):
    """
    Runs the transformation pipeline from staging to the insight layer using Bauplan.
    Executes the pipeline located in pipeline_folder under the specified namespace,
    prints the resulting job state, and logs any errors encountered during execution.

    Parameters:
    :param bpln_client: bauplan.Client - The Bauplan client instance.
    :param pipeline_folder: str- Directory containing the pipeline to run.
    :param namespace: str - Namespace in which the pipeline is executed.

    Logs:
    Prints the job ID and state if successful; otherwise, prints an error message.

    """
    try:
        insight_branch = get_import_branch_name(branch_name='ciro.zilliant_insight_layer')
        bpln_client.create_branch(branch=insight_branch, from_ref='main')
        print(f"✅ Branch '{insight_branch}' created.")
    except bauplan.errors.BauplanError as e:
        print(f'Something went wrong while creting the transformation branch: {e}')
    # run the transformation pipeline for the insight layer
    run_state = bpln_client.run(
        project_dir=pipeline_folder,
        ref=insight_branch,
        namespace=namespace,
    )
    print(f'This is the result for {run_state.job_id}: {run_state}')
    if run_state.job_status.lower() == 'failed':
        raise Exception(f"Pipeline {run_state.job_id} run failed: {run_state.job_status}")

    # merge the branch of the insight layer into the main branch
    try:
        bpln_client.merge_branch(source_ref=insight_branch, into_branch='main')
        print(f"✅ Branch '{insight_branch}' merged into main.")
    except bauplan.errors.BauplanError as e:
        print(f'🔴Error in branch {insight_branch} into main')


def main():
    # Instantiate a bauplan client
    bpln_client = bauplan.Client()
    # Definte the source s3 location for the Raw data
    s3_source_folder = 's3://alpha-hello-bauplan/zilliant-synthetic-data/'
    # define namespace in the data catalog
    namespace = 'zilliant'
    # Define the list of files that need to be imported as Iceberg tables
    list_of_files = [
        f"zilliant-demo-data-2025-02-12-account.csv",
        f"zilliant-demo-data-2025-02-12-transaction_line_item.csv",
        f"zilliant-demo-data-2025-02-12-product_data.csv",
        f"zilliant-demo-data-2025-02-12-product_category.csv",
        f"zilliant-demo-data-2025-02-12-supplier_sku_lookup.csv",
    ]
    # import the raw data into the staging zone
    from_raw_to_staging(
        bpln_client=bpln_client,
        s3_source_folder=s3_source_folder,
        list_of_tables_to_import=list_of_files,
        namespace=namespace
    )
    # run the transformation pipeline from staging to marts and applications
    from_staging_to_applications(
        bpln_client=bpln_client,
        pipeline_folder='transformation_pipeline',
        namespace=namespace
    )
    print("🐬 So long and thanks for all the fish!")


if __name__ == "__main__":
    main()