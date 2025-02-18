# Zilliant demo
## Overview 
This project is a bauplan implementation of a high level architecture provided by Stephen Zeek at Zilliant. 

### Desired architecture 

- Landing Zone (S3)
  - File Validations + AV
  - Files not persisted here
  - CSVs

- Raw Zone (S3)
  - Archive and debugging store 
  - Parquet

- Staging Zone
  - Serving layer – APIs query here. 
  - Loading parquet to Aurora PG now
  - Would like to get this to s3 + iceberg

- Insights layer
  - dbt layer which executes specific business logic to identify opportunities

### Bauplan implementation  
The proposed architecture can be significantly simplified by using simple serverless functions over branches with Iceberg tables on S3. 

**- Landing zone (S3)** - Dump all the files in S3 - this zone seems at firs glance outside the scope of Bauplan.

**- Raw Zone(S3) + Staging Zone (S3).** Mapping these two zones onto the concept of data branches simplifies significantly the design, and make debug easier. 
- The Raw Zone becomes an import branch
- The Staging Zone is created by a merging the import branch into main
- the ref import branch remains available for debug and observability
- rolling back is always possible because the platform provides automatic versioning upon commits  


- **The Serving layer** is the same as the Staging Zone, since all the Staging tables can be queried directly with any query engine that speaks Iceberg (which include Bauplan itself).

- **The Insight layer**
- We prepared a transformation pipeline (see below for the details) - the business logic of this pipeline is arbitrary, nothing hinges on it. Stephen will provide the specifics of the pipeline he wants to build. 
- Note - If the team at Zilliant is married to dbt we can compile against our runner and keep the code in dbt. 

# Run the project 
## Setup
### Python environment
To run the project, you need to have `Python >=3.10` installed. We recommend using a virtual environment to manage dependencies:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Bauplan
* [Join](https://www.bauplanlabs.com/#join) the bauplan sandbox, sign in, create your username and API key.
* Complete do the 3-min [tutorial](https://docs.bauplanlabs.com/en/latest/tutorial/index.html) to get familiar with the platform.
* When you gain access, public datasets (including the one used in this project) will be available for you to start building pipelines.

## OpenAI
* Sign up on OpenAI to get your API key — you’re free to experiment with different LLMs by replacing the LLM utility code in the pipeline.
* Once you have an OpenAI api key, you can add it as a secret to your bauplan project. Open your terminal and run the following command replacing the value with your openAI token - This will allow bauplan to connect OpenAI securely:
```bash
cd src/transformation_pipeline
bauplan parameter set --name openai_api_key --value aaa --type secret
```
You can inspect the file `bauplan_project.yml` in the folder `src/transformation_pipeline` and you will see that a new parameter can be found:
```yaml
parameters:
    openai_api_key:
        type: secret
        default: kUg6q4141413...
        key: awskms:///arn:aws:kms:us-...
```
### I don't have an OpenAI API token...can I skip the OpenAI step?
Don't worry we got you covered. Go in the file `transformation_pipeline/models.py` and comment the entire function called `product_data_enriched` -  from line 4 to line 50.
Then, go to the second function `top_selling_products`, comment line 57 and de-comment line 58.

## Run it
```bash
cd src 
python end_to_end_flow.py
```
The script executes the entire data pipeline (~150 lines of Python) from start to finish. Let’s break it down step by step.

### 🚀 High-Level Overview
The process can be divided into **two main stages**:
1. **Data Ingestion & Validation** → From the raw zone to the staging zone
2. **Data Transformation** → from the staging zone to the insight/application layer

### 🔍 Step-by-Step Breakdown
### **1️⃣ Data Ingestion & Validation**
- **The ingestion logic** is defined in the function `from_raw_to_staging` in the file `end_to_end_flow.py`
    - **Imports raw files** into the system (S3).
    - **Convert files to Iceberg tables** in a temporary import branch.
    - **Run automated data quality tests** using `data_quality_tests.py`.
    - **Validation check:**
        - ✅ If tests **pass**, the data is merged into the **staging zone**.
        - ❌ If tests **fail**, the branch remains open for debugging.
      
<img src="src/img/Zilliant flow.jpg" alt="Zilliant flow.jpg" width="1000">

### **2️⃣ Data Transformation**
- Once data is staged, it runs through the **transformation pipeline** using Bauplan.
- The transformation logic is defined in the file `transformation_pipeline/models.py`.
- The pipeline enriches the data and creates mart tables that can be visualized as dashboards.
- Showcase: the pipeline calls **OpenAI's API** for data enrichment - We ask `gpt-4` to create category tags from the products from the product descriptions and append them as a new column to the table `product_data`.
- The output is stored as **new tables** in the insights layer: `product_data_enriched` , `top_selling_products` and `top_selling_suppliers`.


<img src="src/img/Zilliant flow (1).jpg" alt="Zilliant flow (1).jpg" width="1000">


### 📊 What Do We Get at the End?
By the end of the process, we have **eight new tables** in the data lake:
- **Imported tables:**
    - `zilliant.account`
    - `zilliant.product_category`
    - `zilliant.product_data`
    - `zilliant.supplier_sku_lookup`
    - `zilliant.transaction_line_item`
- **Transformed tables:**
    - `zilliant.product_data_enriched`
    - `zilliant.top_selling_products`
    - `zilliant.top_selling_suppliers`
You can verify the tables by running:
```bash
bauplan table --namespace zilliant
```
To explore the schema of the new tables run these commands in your terminal:
```bash
bauplan table get zilliant.account
bauplan table get zilliant.product_category
bauplan table get zilliant.product_data
bauplan table get zilliant.product_data_enriched
bauplan table get zilliant.supplier_sku_lookup
bauplan table get zilliant.top_selling_products
bauplan table get zilliant.top_selling_suppliers
bauplan table get zilliant.transaction_line_item
```

### 🔎 Exploring and Querying Data
If you want to explore the final datasets, simply run:
```bash
bauplan query "SELECT * FROM zilliant.product_data_enriched WHERE category_name == 'Retail'"
bauplan query "SELECT * FROM zilliant.top_selling_suppliers"

```
This pipeline sets the foundation for **web-based dashboards**, **integration with Snowflake**, and **other analytics tools**.

### Running the transformation pipeline with bauplan interactively
To run the pipeline - i.e. the DAG going from the table imported to the final marts -- you just need to create a [data branch](https://docs.bauplanlabs.com/en/latest/concepts/branches.html).
```bash
cd src/transformation_pipeline
bauplan branch create <YOUR_USERNAME>.zilliant_dag
bauplan branch checkout <YOUR_USERNAME>.zilliant_dag
```
You can now run the DAG:
```bash
bauplan run
```
You will notice that Bauplan stream back data in real-time, so every print statement you will be visualized in your terminal.

