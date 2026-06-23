# Programming Conventions

This document outlines the programming conventions to be followed in this project to ensure consistency, readability, and maintainability of the codebase.

## 1. Docstring Format for Functions

All functions must include docstrings that describe their behavior, arguments, and return values. The format for function docstrings follows the standard Python convention:

```python
def example_function(p_param1: str, p_param2: int) -> str:
    """
    Brief description of what the function does.

    Args:
        p_param1 (str): Description of the first parameter.
        p_param2 (int): Description of the second parameter.

    Returns:
        str: Description of the return value.
    """
    pass
```

### Guidelines:
- The first line should be a short summary of the function's purpose.
- List each argument with its type and description under the `Args:` section.
- Include the return type and description under the `Returns:` section.

## 2. Docstring Format for Flows and Tasks

In addition to the standard docstring structure, flows and tasks use a specific format for their docstrings. All `flows` and `tasks` must define the `name` attribute. Tasks must also include the `tags` attribute, which helps to categorize tasks for easier debugging and monitoring. The structure is as follows:

### Flows:
```python
@flow(name="example_flow")
def example_flow(p_param1: str, p_param2: str) -> None:
    """
    Description

    The flow performs the following operations:
    1. Step 1
    2. Step 2
    3. Step 3

    Args:
        param1 (str): Description of the first parameter.
        param2 (str): Description of the second parameter.

    Returns:
        None: (Include this only if the flow does not return anything)
    """
    pass
```

### Tasks:
Tasks should include both the name and tags parameters. The name parameter is used to specify a unique identifier for the task, and tags are used for categorization.
```python
@task(
    name="example_task",
    tags=["category1", "category2"]
)
def example_task(p_param1: str, p_param2: int) -> int:
    """
    Brief description of what the task does.

    Args:
        p_param1 (str): Description of the first parameter.
        p_param2 (int): Description of the second parameter.

    Returns:
        int: Description of the return value.
    """
    pass
```

### Guidelines:
- Flows should list the steps involved in the flow's execution.
- Both flows and tasks should use the `Args` and `Returns` sections to describe parameters and return values.

## 3. Task Naming Convention

All tasks should end with `_task` in their name. This makes it easy to identify tasks in the codebase. For example:

```python
@task
def clean_data_task(df: DataFrame) -> DataFrame:
    # Task content
    pass
```

## 4. Flow Naming Convention

All flows should end with `_flow` in their name to maintain consistency and clarity. For example:

```python
@flow
def customer_data_flow() -> None:
    # Flow content
    pass
```

## 5. File Naming Convention

Each Python file containing a flow must be named according to the flow it contains. The file name should exactly match the name of the flow, excluding the `_flow` suffix. For example, a flow named `customer_data_flow` should be placed in a file called `customer_data_flow.py`.

```plaintext
Correct:
  customer_data_flow.py

Incorrect:
  data_flow_customer.py
```

## 6. DataFrame Naming Convention

DataFrames should always include the prefix `df` to clearly indicate that they hold tabular data. For example:

```python
df_customers = load_customer_data()
```

## 7. Naming Convention for Tasks, Methods, Variables, Parameters, and DataFrames

The naming convention for tasks, methods, variables, parameters, and DataFrames should follow **lower_snake_case**. This means that names should be all lowercase, with words separated by underscores.

### Examples:
- Tasks: `process_data_task`
- Variables: `customer_name`, `total_sales`
- Methods: `load_data`, `save_results`
- DataFrames: `df_orders`, `df_customers`

## 8. Parameter Naming Convention in Functions, Tasks, and Flows

All parameters in regular functions, tasks, and flows must have the prefix `p_` and should use **lower_snake_case**. This convention ensures clarity by differentiating parameters from other variables in the code. For example:

```python
def example_function(p_input_data: Dataframe, p_start_date: str, p_end_date: str) -> None:
        """
    Brief description of what the function does.

    Args:
        p_input_data: Description of the first parameter.
        p_start_date: Description of the second parameter.
        p_end_date: Description of the third parameter.

    Returns:
        None: (Include this only if the flow does not return anything)
    """
    # Function content
    pass
```


## 9. Operation Formatting

When performing arithmetic operations (addition, subtraction, etc.), each element in the operation must be placed on a separate line, and the operator should be on the same line as the operand it modifies.

### Example:
```python
result = (
    value1
    + value2
    - value3
    * value4
)
```

This formatting ensures clarity and makes it easier to modify individual parts of the expression.

## 10. Date Convention in DataFrames Returned by Flows

When working with DataFrames in flows, it's important that the date column from which the data is partitioned is always the last field in the select clause. This ensures that the date field is consistently positioned at the end of the DataFrame. Example:

```python
df_result = df.select(
    "some_column",
    "another_column",
    "yet_another_column",
    "date_column" # Ensure this is the last in the selection
)
```

This conventions helps maintain a predictable structure, making it easier to analyze and work with the data later in the pipeline.


## 11. Descriptive Naming

All variable names, functions, parameters, tasks, flows, and DataFrames must have descriptive and meaningful names. The names should clearly indicate their purpose or function without requiring additional comments.

### Examples:
- Use `total_revenue` instead of `tr`.
- Use `load_customer_data` instead of `load_data`.

Good descriptive names help make the code self explanatory and easier to understand for anyone working on or reviewing the project.


## 12. Naming Conventions for Features

To maintain a consistent standard across columns returned as features, the following naming conventions must be followed:

### Suffixes for Column Names
1. **_cnt**: This suffix must be used for columns that result from a **count operation**, where the column represents a count of data points.
   - Example: `numero_transacciones_cnt`
2. **_val**: This suffix must be used for columns that represent **numeric values denoting amounts** or monetary sums.
   - Example: `monto_creditos_val`

### Singular vs. Plural Naming Based on Data Granularity
To differentiate between features derived from **snapshot** (monthly) data and **transactional** (daily) data, the naming should reflect the granularity:

1. **Snapshot Data (Monthly)**:
   - Column names must be **singular** in Spanish.
   - Example: `saldo_promedio_val` (average balance from snapshot data).

2. **Transactional Data (Daily)**:
   - Column names must be **plural** in Spanish.
   - Example: `monto_transacciones_val` (transaction amounts from daily data).

### Example Columns
- **`monto_creditos_val`**: A monetary value column derived from snapshot data (singular).
- **`monto_credito_val`**: Incorrect naming for snapshot data (must be singular for snapshot data).
- **`numero_transacciones_cnt`**: A count column derived from transactional data (plural).

### Summary of Rules
| Type                 | Suffix   | Granularity | Example                |
|----------------------|----------|-------------|------------------------|
| Count (Operation)    | `_cnt`   | Singular/Plural based on data granularity | `numero_transacciones_cnt` |
| Monetary Value       | `_val`   | Singular/Plural based on data granularity | `monto_creditos_val`       |
| Snapshot (Monthly)   | N/A      | Singular    | `saldo_promedio_val`    |
| Transactional (Daily)| N/A      | Plural      | `monto_transacciones_val`|

By adhering to these conventions, we ensure clarity and consistency in column naming across features.

## 13.  Including the `p_flow` Parameter in Domain Files

When creating a `.py` file for each domain, it is crucial to include the `p_flow` parameter in the `@flow` decorator. This parameter should specify the type of flow being implemented, thereby allowing validation between `data_cleaning` and `data_transformation`. Ensuring this parameter is correctly defined will guarantee that the data flow is processed appropriately according to its specific purpose.

Additionally, the `p_flow` parameter must also be passed to the tasks `load_raw_data_flow` and `save_data_flow`. This ensures that each task can handle the data flow consistently with the specified flow type.

### Implementation Example:
Replace `domain` with the corresponding data domain you are working on, such as `customer`, `credit_card`, among other domains.
```python
@flow(name="domain_flow")
def domain_flow(
    p_flow_key: str,
    p_start_dt: str,
    p_end_dt: str,
    p_env: str = "",
    p_output_domain: str = "",
    p_overwrite_strategy: str = "",
    p_flow: str = "",
):
    raw_data = load_raw_data_flow(
        p_key=p_flow_key, p_start_dt=p_start_dt, p_end_dt=p_end_dt, p_flow=p_flow
    )
    df_domain = raw_data["raw_domain"]

    df_domain_final = clean_domain_task(df_domain)

    save_data_flow(
        p_output_df=df_domain_final,
        p_key=p_flow_key,
        p_environment=p_env,
        p_output_domain=p_output_domain,
        p_overwrite_strategy=p_overwrite_strategy,
        p_flow=p_flow,
    )
```
