import bauplan


@bauplan.model(internet_access=True, materialization_strategy='REPLACE')
@bauplan.python('3.11', pip={'openai': '1.57.2'})
def data_enrichment_openai(
        data=bauplan.Model('zilliant.product_data'),
        openai_api_key=bauplan.Parameter('openai_api_key'),
):
    """
    Enriches product data by generating category tags via ChatGPT.

    The function reads the 'description' column from the input table, sends each description to ChatGPT
    using the process_row function, and appends the results as a new column ('llm_tags') to the table.

    :param data: The input dataset containing at least a 'description' column.
    :param openai_api_key: OpenAI API key for accessing the ChatGPT model.
    :return: The input data with an appended 'llm_tags' column containing the generated tags.
    """
    from openai import OpenAI
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from gpt_utils import process_row

    # Get product descriptions as a list
    descriptions = data['description'].to_pylist()

    print("\n\n=====> Starting LLM processing...\n")
    llm_tags = []
    max_workers = 4
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Correctly iterate over the list of descriptions
        future_to_index = {
            executor.submit(process_row, openai_api_key, desc): idx
            for idx, desc in enumerate(descriptions)
        }
        for future in as_completed(future_to_index):
            idx = future_to_index[future]
            try:
                response_text = future.result()
                print(f"Processed row {idx}: {response_text}")
            except Exception as e:
                response_text = f"Error: {str(e)}"
                print(f"Error processing row {idx}: {response_text}")
            llm_tags.append(response_text)
        print(len(llm_tags))

    return data.append_column('llm_tags', [llm_tags])


@bauplan.model()
@bauplan.python('3.11', pip={'duckdb': '1.2.0'})
def product_sales(
        transaction_line_item=bauplan.Model('zilliant.transaction_line_item'),
        product_data=bauplan.Model('data_enrichment_openai')
):
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
    """
    ).arrow()

    return results


@bauplan.model(materialization_strategy='REPLACE')
@bauplan.python('3.11', pip={'duckdb': '1.2.0'})
def top_selling_products(
        product_sales=bauplan.Model('zilliant.product_sales')
):
    import duckdb

    results = duckdb.sql("""
     SELECT 
        product_name,
        sku,
        category_name,
        total_units_sold,
        total_revenue
    FROM product_sales
    ORDER BY total_revenue DESC
    """
    ).arrow()

    return results


@bauplan.model(materialization_strategy='REPLACE')
@bauplan.python('3.11', pip={'duckdb': '1.2.0'})
def top_selling_suppliers(
        product_sales=bauplan.Model('zilliant.product_sales'),
        supplier_sku_lookup=bauplan.Model('zilliant.supplier_sku_lookup')
):
    import duckdb

    results = duckdb.sql("""
     SELECT
        s.supplier_name,
        SUM(ps.total_revenue) AS total_supplier_revenue
    FROM product_sales ps
    JOIN supplier_sku_lookup s ON ps.sku = s.sku
    GROUP BY s.supplier_name
    ORDER BY total_supplier_revenue DESC
    LIMIT 10;
    """
    ).arrow()

    return results
