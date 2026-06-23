from databricks.connect import DatabricksSession  # type: ignore
from prefect import flow, task  # type: ignore
from pyspark.sql import DataFrame  # type: ignore

from data_engineering.core.check_date_integrity import (
    all_dates_task,
    collect_tables_for_column,
    count_records,
    date_sequence_task,
    dynamic_query,
    evaluate_missing_information,
    get_date_range_check,
    schema_tables,
    table_columns,
)
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns

spark = DatabricksSession.builder.getOrCreate()

date_column_frequencies = {
    "fecha_informacion": "monthly",
    "fecha_transaccion": "daily",
    "fecha_catalogo": "monthly",
    "_observ_end_dt": "monthly",
}


@arrange_columns(p_start_cols=["fecha_informacion", "tabla", "tipo_tabla"])
@task(name="check_information_by_date_task", tags=["dates"])
def check_information_by_date_task(
    start_date: str,
    end_date: str,
) -> DataFrame:
    """Generate the number of records for each table

    Args:
        start_date (str): start date.
        end_date (str): end date.

    Returns:
        DataFrame: Dataframe with table name and number of records.
    """
    df_daily_dates = date_sequence_task(start_date, end_date, "1,0")
    df_eom_dates = date_sequence_task(start_date, end_date, "1")
    list_table = [
        item for item in schema_tables() if not item.startswith(("qa", "dbd"))
    ]

    df_table_type = table_columns(list_table, date_column_frequencies)

    df_result = spark.createDataFrame([], schema="fecha_informacion date, tabla string")
    df_date_table = df_result

    for column_name, frequency in date_column_frequencies.items():
        tables_to_process = collect_tables_for_column(df_table_type, column_name)
        if not tables_to_process:
            continue

        date_df = df_daily_dates if frequency == "daily" else df_eom_dates
        sentence = f"select {column_name} fecha_informacion from "
        df_by_data_type = dynamic_query(tables_to_process, sentence)
        if df_by_data_type is not None:
            df_result = df_result.union(df_by_data_type)

        df_dates = all_dates_task(date_df, tables_to_process)
        if df_dates is not None:
            df_date_table = df_date_table.union(df_dates)

    df_records = count_records(df_result, start_date, end_date)

    df_final = (
        df_date_table.join(df_records, ["fecha_informacion", "tabla"], "left")
        .join(df_table_type, ["tabla"], "left")
        .fillna(0)
    )

    return df_final


@flow(name="check_date_flow")
def check_date_flow():
    """
    The flow performs the following operations:
    1. Validates the number of records for each table.
    2. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value
    """
    custom_validations = {"eqmas": 11}
    error_message = ("Data Transformation without month information",)
    start_date, end_date = get_date_range_check()
    df_check_date = check_information_by_date_task(start_date, end_date)
    save_data_flow(df_check_date)
    evaluate_missing_information(df_check_date, error_message, custom_validations)
