import bauplan
import datetime
import re
from data_quality_tests import are_there_nulls


def get_import_branch_name():
    """Generate a unique branch name with timestamp."""
    timestamp = datetime.datetime.now().isoformat(timespec='seconds').replace(":", "_")
    return f"ciro.zilliant_upload_{timestamp}"


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
       ----------
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


def main():
    # Initialize Bauplan client
    bpln_client = bauplan.Client()

    # Define file prefix and construct file names
    prefix = 'zilliant-demo-data-2025-02-12'
    filenames = [
        f"{prefix}-account.csv",
        f"{prefix}-transaction_line_item.csv",
        f"{prefix}-product_data.csv",
        f"{prefix}-product_category.csv",
        f"{prefix}-supplier_sku_lookup.csv",
    ]

    # Create a raw import branch
    raw_import_branch = get_import_branch_name()
    bpln_client.create_branch(branch=raw_import_branch, from_ref='main')
    print(f"✅ Branch '{raw_import_branch}' created.")

    # create namespace
    namespace = 'zilliant'
    if not bpln_client.has_namespace(namespace=namespace, ref=raw_import_branch):
        namespace = bpln_client.create_namespace(namespace=namespace, branch=raw_import_branch)
        print(f"✅: Namespace 'zilliant' created successfully.")

    # import each file as an Iceberg table in the raw import branch
    for filename in filenames:
        table_name = extract_table_name(filename)
        s3_source = f's3://alpha-hello-bauplan/zilliant-synthetic-data/{filename}'
        import_data_in_iceberg(
            client=bpln_client,
            table_name=table_name,
            ref_branch=raw_import_branch,
            source_s3=s3_source,
            namespace=namespace
        )
    # Run data quality tests to check that we have no nulls in the column transaction_id in the table
    _are_there_null_transaction_ids = are_there_nulls(
        client=bpln_client,
        table_name='transaction_line_item',
        column_to_check='transaction_id',
        ingestion_branch=raw_import_branch,
        namespace=namespace,
    )
    assert not _are_there_null_transaction_ids

    bpln_client.merge_branch(source_ref=raw_import_branch, into_branch='main')
    print(f"✅ Branch '{raw_import_branch}' merged into main.")

    print("🐬 So long and thanks for all the fish!")


if __name__ == "__main__":
    main()







