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
  - DBT layer which executes specific business logic to identify opportunities

  
### Bauplan implementation 
Bauplan can significantly simplify the proposed architecture by using branches and Iceberg tables on S3. 

**- Landing zone (S3)** - Dump all the files in S3 - this zone seems at firs glance outside the scope of Bauplan.

**- Raw Zone(S3) + Staging Zone (S3).** These two zones can mapped onto the concept of branches which simplifies significantly the design, maintenance and debug process. 
The current implementation looks like this:
  - Open an import branch in the Catalog on S3.
  - Create tables and import data as Iceberg Tables into the import branch.
  - Run data quality test
  - If they go well:
      - Merge the import branch into main
  - else:
      - Raise an exception and keep the import branch open for debug.
- The serving layer may the same as the Staging as all the tables in the staging layer can be queried with any query engine that speaks Iceberg (which include Bauplan itself).

**- Insight layer**
- We prepared a transformation pipeline (see below for the details) - the business logic of this pipeline is arbitrary, nothing hinges on it. Stephen will provide the specifics of the pipeline he wants to built. 


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

## Run the project

```bash
cd src 
python end_to_end_flow.py
```

## What the hell just happened?
The script you just ran takes care of the entire flow which looks like this:
![Zilliant flow.jpg](src%2Fimg%2FZilliant%20flow.jpg)
![Zilliant flow (1).jpg](src%2Fimg%2FZilliant%20flow%20%281%29.jpg)

Whereas this looks very complex, we can do this with just ~150 lines of no-nonsense Python. 
The first step from landing to staging is done in the function `from_raw_to_staging` in the file `end_to_end_flow.py`. 
The branches allow to implement the passage from Raw to Staging as a Write-Audit-Publish workflow which ensures that data quality is easy to enforce while constructing the artifacts that downstream processes will rely upon. 

The second step is done in the function `from_staging_to_applications` and runs a transformation pipeline with bauplan serverless runtime. The code of the pipeline can be found in the file `transformation_pipeline/models.py`.
This specific semantics of this pipeline has been implemented in the spirit of providing something that might look reasonable as a transformation pipeline. The specific business logic will depend on the exact use case that the team at Zillant plans on supporting.  
In this pipeline, we showcase how easy it is to call an external API - in this case OpenAI - and save the results back into the datalake in an Iceberg Table.  

The final outcome of running the end-to-end pipeline is that we will have eight new tables in our data lake: 5 from the import phase and three from the transformation pipiline. 
To check the new tables simply run this command in your terminal:

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
If you want to query one of the final tables produced by the pipeline, you can use the `query` command from bauplan:

```bash
bauplan query "SELECT * FROM zilliant.top_selling_suppliers"
```

We will be able to implement web-based interfaces to query the data, as well as interoperate with other query engines (e.g. Snowflake).   

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

