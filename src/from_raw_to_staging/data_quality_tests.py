import bauplan


def are_there_nulls(
        client: bauplan.Client,
        table_name: str,
        column_to_check: str,
        ingestion_branch: str,
        namespace: str,
):
    """
    We check the data quality by running a data quality test in-process: we use
    Bauplan SDK to query the data as an Arrow table, and check if the
    target column is not null through vectorized PyArrow operations.

    :param client: Bauplan
    :param table_name str
    :param ingestion_branch str
    :param namespace str
    :param column_to_check str
    :return: bool
    """
    arrow_table = client.scan(
        table=table_name,
        ref=ingestion_branch,
        namespace=namespace,
        columns=[column_to_check]
    )
    return arrow_table[column_to_check].null_count > 0


### This is the same test but it uses an SQL query so it is a bit more flexible when it comes to writing your own custom logic.
# def are_there_nulls(
#         client: bauplan.Client,
#         table_name: str,
#         column_to_check: str,
#         ingestion_branch: str,
#         namespace: str,
# ):
#     """
#    We check the data quality by running a data quality test in-process: we use
#    Bauplan SDK to query the data as an Arrow table, and check if the
#    target column is not null through vectorized PyArrow operations.
#
#    :param client: Bauplan
#    :param table_name str
#    :param ingestion_branch str
#    :param namespace str
#    :param column_to_check str
#    :return: bool
#    """
#     sql_query = f"""
#         SELECT COUNT(*)
#         FROM {table_name}
#         WHERE {column_to_check} IS NULL
#         """
#     table = client.query(query=sql_query, ref=ingestion_branch, namespace=namespace).to_pylist()
#     # get the content of the only row and make sure it's a zero
#     return table[0]['C'] == 0
