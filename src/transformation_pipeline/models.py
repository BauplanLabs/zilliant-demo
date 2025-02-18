import bauplan


# @bauplan.model(internet_access=True, materialization_strategy='REPLACE')
# @bauplan.python('3.11', pip={'openai': '1.57.2'})
# def product_data_enriched(
#         data=bauplan.Model('zilliant.product_data'),
#         openai_api_key=bauplan.Parameter('openai_api_key'),
# ):
#
#     """
#     Enriches product data by generating category tags via ChatGPT.
#
#     This function reads the 'description' column from the input table (zilliant.product_data),
#     sends each product description to ChatGPT via the process_row function to generate a set of
#     category tags, and appends the responses as a new column ('llm_tags').
#
#     Output Table:
#     | customer_product_id | product_name | category_name | sku | description | supplier_id | unit_cost | llm_tags |
#     |--------------------|---------------|--------------|------|------------|-------------|-----------|-----------|
#     | Ncxe-9847775191   | Wireless Mouse | Retail       | 123  | <text>     | XAHZ-455199 |454.6183   | "<text>   |
#
#     """
#     from openai import OpenAI
#     from concurrent.futures import ThreadPoolExecutor, as_completed
#     from gpt_utils import process_row
#
#     # Get product descriptions as a list
#
#     descriptions = data['description'].to_pylist()
#     print("\n\n=====> Starting LLM processing...\n")
#     llm_tags = []
#     max_workers = 4
#     with ThreadPoolExecutor(max_workers=max_workers) as executor:
#         # Correctly iterate over the list of descriptions
#         future_to_index = {
#             executor.submit(process_row, openai_api_key, desc): idx
#             for idx, desc in enumerate(descriptions)
#         }
#         for future in as_completed(future_to_index):
#             idx = future_to_index[future]
#             try:
#                 response_text = future.result()
#                 print(f"Processed row {idx}: {response_text}")
#             except Exception as e:
#                 response_text = f"Error: {str(e)}"
#                 print(f"Error processing row {idx}: {response_text}")
#             llm_tags.append(response_text)
#         print(len(llm_tags))
#     return data.append_column('llm_tags', [llm_tags])


@bauplan.model(materialization_strategy='REPLACE')
@bauplan.python('3.11', pip={'duckdb': '1.2.0'})
def top_selling_products(
        transaction_line_item=bauplan.Model('zilliant.transaction_line_item'),
        # product_data=bauplan.Model('product_data_enriched')
        product_data=bauplan.Model('zilliant.product_data')
):
    """
    Joins transaction line items with enriched product data and aggregates sales metrics.

    Output Table:
    | customer_product_id | product_name   | sku         | category_name   | total_units_sold | total_revenue |
    |---------------------|----------------|-------------|-----------------|------------------|---------------|
    | <id>                | <product name> | <sku>       | <category>      | <number>         | <number>      |

    """
    import duckdb

    results = duckdb.sql("""
    SELECT 
        t.customer_product_id,
        p.product_name,
        p.sku,
        p.category_name,
        SUM(t.order_quantity) AS total_units_sold,
        SUM(t.line_total) AS total_revenue
    FROM transaction_line_item t
    JOIN product_data p 
      ON t.customer_product_id = p.customer_product_id
    GROUP BY 
        t.customer_product_id, 
        p.product_name, 
        p.sku, 
        p.category_name
    ORDER BY total_revenue DESC
    """
    ).arrow()

    return results


@bauplan.model(materialization_strategy='REPLACE')
@bauplan.python('3.11', pip={'duckdb': '1.2.0'})
def top_selling_suppliers(
        top_selling_products=bauplan.Model('top_selling_products'),
        supplier_sku_lookup=bauplan.Model('zilliant.supplier_sku_lookup')
):
    """
    Aggregates revenue by supplier to identify the top-selling suppliers.
    Joins the 'product_sales' table with the 'supplier_sku_lookup' table on the 'sku'
    field and sums the total revenue per supplier. and order results in descending order by revenue.

    Final Output Table:
    | supplier_name   | total_supplier_revenue |
    |-----------------|------------------------|
    | <supplier name>| <number>               |

    """
    import duckdb

    results = duckdb.sql("""
     SELECT
        s.supplier_name,
        SUM(ps.total_revenue) AS total_supplier_revenue
    FROM top_selling_products ps
    JOIN supplier_sku_lookup s ON ps.sku = s.sku
    GROUP BY s.supplier_name
    ORDER BY total_supplier_revenue DESC
    LIMIT 10;
    """
    ).arrow()

    return results
