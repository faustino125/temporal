# Usage

To execute the data processing scripts, use the following command from the root of the `data_engineering` project:

```bash
python -m <ROOT_MODULE>.<MAIN_FILE_FOLDER>.<MAIN_FILE_LAYER> --env <ENVIRONMENT> --workflow <WORKFLOWS> --flow <FLOWS> --start_dt <START_DATE> --end_dt <END_DATE> --nodes <NODES> --overwrite_strategy <STRATEGY>
```
- __ROOT_MODULE__: This placeholder represents the root module or package of your project. For this project, replace <ROOT_MODULE> with data_engineering. It serves as the main entry point for executing scripts and orchestrating operations within the project's structure.
- __MAIN_FILE_FOLDER__:  Replace <MAIN_FILE_FOLDER> with core
- __MAIN_FILE__: Replace <MAIN_FILE> with main_flow
- __ENVIRONMENT__: It can either be: dev, preprod or prod, nevertheless, we must only use dev environment if we a are doing tests.
- __WORKFLOWS__: There are 2 types of workflows: data_cleaning or data_transformation
- __FLOWS__: Depending on the type of workflow, different flows are used:
  - **For `data_cleaning` workflows:**
    - __transactional_flow__: This flow is designed for data that change daily, in other terms, this kind of data is partitioned by days.
    - __snapshot_flow__: This flow is designed for data that change monthly, in other terms, it is partitioned by months, so an update date of a row will always reference the state at the end of the month.
    - __catalog_flow__: This flow is designed for data that is not partitioned by time and only reflects dictionary or categorical data that is not partitioned by a date, therefore, we do not need to use the parameters _START_DATE_ or _END_DATE_ when we use this type of flow.
    - __qa_flow__: This flow is used to validate the integrity of the cleaned data, identifying inconsistencies and missing values to ensure data quality.

  - **For `data_transformation` workflows:**
    - __data_transformation_flow__: This flow is designed to apply feature transformations on a monthly basis, adjusting data to a structured format suitable for downstream processing.
    - __qa_flow__: This flow is used to validate the integrity of the transformed data, identifying inconsistencies and missing values to ensure data quality.
- __START_DATE__: The inclusive start date from where we want our data partitioned, in **`<YYYY-MM-DD>`** format. Exclusively for transactional and snapshot flows. If not provided, it defaults to the first day of the previous month, following the rules of incremental loading based on the prior month.
- __END_DATE__: The inclusive end date from where we want our data partitioned, in **`<YYYY-MM-DD>`** format. Exclusively for transactional and snapshot flows. If not provided, it defaults to the last day of the previous month, in line with the incremental loading strategy based on the prior month.
- __NODES__ (optional): The specific nodes that we want to execute from the specified _FLOW_, they need to be in the following format: **`<"node1, node2, ... , nodeN">`** . If not provided it will execute all the nodes inside the _FLOW_
- __OVERWRITE_STRATEGY__ (optional): The strategy that we want to use when saving the output for each _FLOW_. The valid values for this param are: **`replaceWhere or overwriteSchema`**. If not provided it will take the default value which is replaceWhere

# IMPORTANT
Ensure that the nodes provided via _--nodes_ correspond to valid entries inside a flow in the _flow.yml_ , these must correspond to module names and flow node files in the following format:

```bash
<Base Module Name>.<Flow node file>
```

In order for the node to execute correctly, the __Flow Node File__ must have a flow function with the same name as its .py file. Also for accessing the correct __data_catalog__ node, the __Flow function__ must be named as: **data_catalog name + _flow**

There is an integration with Databricks Hive_metastore tables. For all the Hive schemas it's important to keep in mind the following:
- If it's the first time executing a node from a certain domain, a new schema will be created following the next structure:
```sql
ENVIRONMENT_WORKFLOW_DOMAIN.FLOW_KEY
```
- When creating a table, all the physical files will be stored in the ADL_Gen2.
- The Hive tables will be synchronized with the ADL_Gen2, therefore, we will be able to modify the data using SQL queries and the changes will be reflected in both Hive and the ADL delta file.
- The only feature that will not be synchronized will be the DROP TABLE statement due to security reasons.
- The physical storage container will be defined by the __ENVIRONMENT__ we send as param.

# Data Engineering

This project is a modular set of ETL pipelines built with Prefect and PySpark, designed to clean, transform, and process data. It supports operations such as loading raw data from various sources, applying data validation, and saving the processed data into Delta format. The project is configurable through YAML files, can handle different environments (_development_, _production_, and _sandbox_), data overwrite strategies, and different types of flows (_snapshot_, _transactional_, or _catalog_). The project is packaged as a Python wheel with an entrypoint for a smooth and reproducible execution through a Spark Cluster or another Big Data computing resource.

# Dependencies

- **Prefect**: Orchestrates tasks and flows.
- **PySpark**: Handles data processing and transformations.
- **YAML**: Configuration management.
- **argparse**: Command-line argument parsing.
- **pdoc3**: Generates automated documentation for the source code in Markdown format.

# Unified Structure

The project is packaged into a `.whl` file named `data_engineering`, facilitating its distribution and deployment. The version of the `.whl` package is dynamically set using the current date and time.

- ## Directory Structure

    A clear and organized directory structure has been established for data processing tasks, including the core, which contains reusable processes for each data layer:
```
     data_engineering/
     ├── core/
     │   ├── utils.py
     │   ├── load_data_flow.py
     │   ├── save_data_flow.py
     |   ├── check_date_integrity.py
     |   ├── documentation_hive.py
     |   ├── prefixes.yml
     │   └── main_flow.py
     ├── data_cleaning/
     │   ├── conf/
     │   └── src/
     │       ├── customer/
     │       └── credit_card/
     ├── data_transformation/
     │   ├── conf/
     │   └── src/
     │       ├── customer/
     │       └── credit_card/
     ├── data_integration/
     │   ├── conf/
     │   └── src/
     │       ├── customer/
     │       └── credit_card/
     ├── env/
     │   ├── base/
     |       └── global_settings.yml
     │   └── dev/
     |       └── global_settings.yml
     |   └── prod/
     |       └── global_settings.yml

     setup.py
     MANIFEST.in

```



- ### `core` Folder

  The `core` folder within `data_engineering` contains the main files for managing Prefect flows and tasks, as well as standardized functions for reuse in data cleaning and transformation processes:

  - `utils.py`: General utility functions.
  - `load_data_flow.py`: Functions for loading YAML files, loading data, and data filtering, used for the data_cleaning and data_transformation layer.
  - `save_data_flow.py`: Functions for saving data tables in Delta format after their respective cleaning and transformation.
  - `main_flow.py`: This script serves as the main execution flow for running specific data processes in a data engineering environment. Depending on the type of flow specified, it will execute different processes such as cataloging, data cleaning, or data transformation.

- ### `data_cleaning` Folder

  The `data_cleaning` folder is where data cleaning processes are performed for each domain. Here, the necessary processes are configured and executed to ensure that the data is in an optimal state for subsequent transformation and analysis.

  When creating a `.py` file for each domain within the `data_cleaning` folder, it is crucial to include the `p_flow` parameter in the `@flow` decorator. This parameter should specify the type of flow being implemented, allowing for validation between `data_cleaning` and `data_transformation`. Ensuring this parameter is correctly defined guarantees that the data flow is processed appropriately according to its specific purpose.

  Additionally, the `p_flow` parameter must also be passed to the `load_raw_data_flow` and `save_data_flow` tasks. This ensures that each task can handle the data flow consistently with the specified flow type.

- ### `data_transformation` Folder

  The `data_transformation` folder is where data transformations are performed for each domain. This is where business logic is implemented, generating final tables with features ready to be consumed by analytical models.

  When creating a `.py` file for each domain within the `data_transformation` folder, it is crucial to include the `p_flow` parameter in the `@flow` decorator. This parameter should specify the type of flow being implemented, allowing for validation between `data_cleaning` and `data_transformation`. Ensuring this parameter is correctly defined guarantees that the data flow is processed appropriately according to its specific purpose.

  Additionally, the `p_flow` parameter must also be passed to the `load_raw_data_flow` and `save_data_flow` tasks. This ensures that each task can handle the data flow consistently with the specified flow type.

- ### `data_integration` Folder

  The `data_integration` folder is where data aggregations are performed for each domain. This is where business logic is implemented, generating final tables with features ready to be consumed by analytical models.

  When creating a `.py` file for each domain within the `data_integration` folder, it is crucial to include the `p_flow` parameter in the `@flow` decorator. This parameter should specify the type of flow being implemented, allowing for validation between `data_cleaning` and `data_transformation`. Ensuring this parameter is correctly defined guarantees that the data flow is processed appropriately according to its specific purpose.

  Additionally, the `p_flow` parameter must also be passed to the `load_raw_data_flow` and `save_data_flow` tasks. This ensures that each task can handle the data flow consistently with the specified flow type.

# Installation:
Use the following command after you have your git token active:
```bash
git clone https://Bi-McKensey@dev.azure.com/Bi-McKensey/Data%20Factory/_git/DataEngineerHub
```
# Documentation
This project uses _pdoc3_ and a bash script to generate automated documentation in markdown files from docstrings embedded in the python code. To generate the documentation and save it into a markdown, after you have already completed your changes, run the following command in the root project folder:

```bash
./de_process_documentation <LAYER> <MODULE>
```

- **`LAYER`**: The layer of data_engineering which contains the modules we want to generate documentation for, it can either be `data_cleaning`, `data_transformation`, or `core`. If it's not provided, it will generate the documentation for all the available layers.

- **`MODULE`**: The module inside `src` package of the specified `LAYER` we want to generate the documentation for. Exclusively for `data_cleaning` and `data_transformation` layer. If it's not provided, it will generate the documentation for all the available modules.


# Disclaimer

This project and its source code are the exclusive property of **Ingeniería de Datos Analítica Avanzada, BI**. Unauthorized copying, distribution, or use of any part of this project is strictly prohibited. This project is intended solely for internal use by the authorized members of **Ingeniería de Datos Analítica Avanzada, BI**.

All rights are reserved. Any attempt to misuse, modify, or distribute this code without prior permission will result in legal consequences. For further inquiries or to request access, please contact the project maintainers.

**Ingeniería de Datos Analítica Avanzada, BI © 2024**
