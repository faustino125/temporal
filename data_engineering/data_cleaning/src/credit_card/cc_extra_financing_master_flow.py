from prefect import flow, task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_cleaning.src.utils.utils_data_clean import (
    clean_currency_task,
    convert_to_hex_task,
    replace_null_or_empty_values,
)


@replace_null_or_empty_values()
@arrange_columns(p_start_cols=["fecha_informacion", "id_cliente", "cuenta_corporativa"])
@task(
    name="clean_cc_extra_financing_master_task",
    tags=["data cleaning", "preprocessing"],
)
def clean_cc_extra_financing_master_task(
    p_raw_cc_extra_financing_summary: DataFrame,
) -> DataFrame:
    """
    Cleans and processes extra financing summary data by
    applying transformations and cleaning steps.
    It standardizes certain fields, creates new calculated columns,
    and fills empty values with default values.

    Args:
        p_raw_cc_extra_financing_summary (DataFrame): Raw cc_extra_financing data.

    Returns:
        DataFrame: Processed extra financing DataFrame.
    """
    df_cc_extra_financing = p_raw_cc_extra_financing_summary.select(
        convert_to_hex_task(f.col("codigo_cliente"), "cliente"),
        convert_to_hex_task(f.col("Cuenta_corporativa"), "cuenta"),
        f.col("codigo_moneda").cast("int").alias("id_moneda"),
        clean_currency_task(f.col("codigo_moneda").cast("int")).alias(
            "descripcion_moneda"
        ),
        f.col("limite_cuotas").cast("double").alias("limite_cuotas"),
        f.col("valor_cuotas_pendiente").cast("double").alias("saldo_pendiente"),
        f.col("disponible_cuotas").cast("double").alias("disponible_cuotas"),
        f.col("cuota_mensual").cast("double").alias("pago_mensual_cuotas"),
        f.col("limite_extrafinanciamiento")
        .cast("double")
        .alias("limite_extrafinanciamiento"),
        f.col("disponible_extrafinanciamiento")
        .cast("double")
        .alias("disponible_extrafinanciamiento"),
        f.col("fecha_informacion"),
    )

    return df_cc_extra_financing


@flow(name="cc_extra_financing_detail_flow")
def cc_extra_financing_master_flow():
    """
    Load, process, and save credit card extra financing data in the data lake.

    This flow performs the following operations:
    1. Loads raw credit card extra financing summary data using the specified date range
    2. Cleans and processes the credit card extra financing summary data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed data.
    """
    raw_data = load_raw_data_flow()
    df_cc_extra_financing_data = raw_data["raw_cc_extra_financing_summary"]

    df_cc_extra_financing_data = clean_cc_extra_financing_master_task(
        df_cc_extra_financing_data
    )

    save_data_flow(df_cc_extra_financing_data)
