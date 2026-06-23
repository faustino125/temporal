from prefect import flow, task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_transformation.src.utils.utils import (
    add_exchange_rate_task,
    convert_currency_to_gtq_task,
    create_currency_col_task,
    join_dataframes_task,
)


@task(
    name="process_credit_card_account_task",
    tags=["data transformation", "processing"],
)
def process_credit_card_account_task(
    p_cleaned_cc_account: DataFrame,
    p_cleaned_cc_updates: DataFrame,
    p_cleaned_daily_rate: DataFrame,
) -> DataFrame:
    """
    Transforms and processes credit cards account data.

    Args:
        p_cleaned_cc_account (DataFrame): Cleaned main credit card account data.
        p_cleaned_cc_updates (DataFrame): Cleaned credit card updates data.
        p_cleaned_daily_rate (DataFrame): Cleaned daily exchange data.

    Returns:
        DataFrame: Transformed main credit card account DataFrame.
    """
    daily_rate = p_cleaned_daily_rate.filter(
        f.col("fecha_transaccion") == f.col("_observ_end_dt")
    )

    account_df = (
        add_exchange_rate_task(p_cleaned_cc_account, daily_rate, True)
        .select(
            f.col("id_cliente"),
            f.col("cuenta_corporativa"),
            f.col("_observ_end_dt"),
            f.col("bi_online_flag"),
            f.col("segmento"),
            f.col("emisor"),
            f.col("id_categoria"),
            f.col("categoria"),
            f.col("categoria_estandard"),
            f.col("id_situacion_cuenta"),
            f.col("descripcion_situacion_cuenta"),
            f.col("situacion_cuenta_homologado"),
            f.col("limite_gtq"),
            f.col("fecha_corte"),
            f.col("saldo_al_corte_gtq"),
            f.col("saldo_al_corte_usd"),
            (
                f.col("saldo_al_corte_gtq")
                + (f.col("saldo_al_corte_usd") * f.col("tasa_cambio"))
            )
            .cast("float")
            .alias("saldo_al_corte_quetzalizado"),
            f.col("empresarial_flag"),
            f.col("institucional_flag"),
            f.col("flag_no_renovacion"),
            f.when(
                (
                    f.col("descripcion_producto").contains("especial")
                    & (f.col("categoria") == "classic")
                    & (f.col("grupo_situacion") == "activa")
                ),
                1,
            )
            .otherwise(0)
            .alias("activa_especial_flag"),
            f.col("dias_mora_gtq"),
            f.col("dias_mora_usd"),
            f.when(f.col("dias_mora") < 90, 0).otherwise(1).alias("mora90D_flag"),
            f.when(f.col("dias_mora") < 60, 0).otherwise(1).alias("mora60D_flag"),
            f.when(f.col("dias_mora") == 0, 0).otherwise(1).alias("mora30D_flag"),
            f.col("dias_mora").alias("max_dias_mora"),
            f.col("grupo_situacion"),
            f.col("fecha_apertura"),
            f.col("fecha_vencimiento_pago"),
            f.col("pago_gtq"),
            f.col("pago_usd"),
            ((f.col("pago_usd") * f.col("tasa_cambio")) + f.col("pago_gtq")).alias(
                "pago_quetzalizado"
            ),
            f.col("producto_mala_situacion_flag"),
            f.col("ciclo_mora_gtq"),
            f.col("ciclo_mora_usd"),
            f.col("ciclo_mora").alias("max_ciclo_mora"),
            f.when(f.col("ciclo_mora_gtq") > 0, f.col("saldo_al_dia_gtq"))
            .otherwise(0)
            .alias("saldo_mora_gtq"),
            f.when(f.col("ciclo_mora_usd") > 0, f.col("saldo_al_dia_usd"))
            .otherwise(0)
            .alias("saldo_mora_usd"),
            f.when(
                (f.col("ciclo_mora") > 0) & (f.col("saldo_al_dia_gtq") > 0),
                f.col("saldo_al_dia_gtq"),
            )
            .otherwise(0)
            .alias("mayor_saldo_mora_gtq"),
            f.when(
                (f.col("ciclo_mora") > 0) & (f.col("saldo_al_dia_usd") > 0),
                f.col("saldo_al_dia_usd"),
            )
            .otherwise(0)
            .alias("mayor_saldo_mora_usd"),
            f.col("tasa_cambio"),
        )
        .select(
            f.col("*"),
            f.when(
                f.col("max_ciclo_mora") > 0,
                f.col("mayor_saldo_mora_gtq")
                + (f.col("mayor_saldo_mora_usd") * f.col("tasa_cambio")),
            )
            .otherwise(0)
            .alias("mayor_saldo_mora_quetzalizado"),
            f.when(
                (f.col("ciclo_mora_gtq") > 0) & (f.col("ciclo_mora_usd") > 0),
                f.col("saldo_mora_gtq")
                + f.col("saldo_mora_usd") * f.col("tasa_cambio"),
            )
            .when(f.col("ciclo_mora_gtq") > 0, f.col("saldo_mora_gtq"))
            .when(
                f.col("ciclo_mora_usd") > 0,
                f.col("saldo_mora_usd") * f.col("tasa_cambio"),
            )
            .otherwise(0)
            .alias("saldo_mora_quetzalizado"),
        )
        .drop(f.col("tasa_cambio"))
    )

    cc_limit_changes = (
        p_cleaned_cc_updates.filter(
            (f.col("id_proceso") == 5)
            & (f.col("estatus_anterior") != f.col("estatus_actual"))
        )
        .groupBy(
            f.col("id_cliente"), f.col("cuenta_corporativa"), f.col("_observ_end_dt")
        )
        .agg(
            f.lit(1).alias("cambio_limite_flag"),
        )
    )

    account_df = account_df.join(
        cc_limit_changes,
        on=["id_cliente", "cuenta_corporativa", "_observ_end_dt"],
        how="left",
    )

    return account_df


@task(
    name="process_credit_card_balance_task",
    tags=["data transformation", "processing"],
)
def process_credit_card_balance_task(
    p_cleaned_cc_balance: DataFrame,
    p_cleaned_daily_rate: DataFrame,
) -> DataFrame:
    """
    Transforms and processes credit cards balance data.

    Args:
        p_cleaned_cc_balance (DataFrame): Cleaned credit card balance data.
        p_cleaned_daily_rate (DataFrame): Cleaned daily exchange data.

    Returns:
        DataFrame: Transformed credit card balance DataFrame.
    """
    daily_rate = p_cleaned_daily_rate.filter(
        f.col("fecha_transaccion") == f.col("_observ_end_dt")
    )

    df_balance = add_exchange_rate_task(p_cleaned_cc_balance, daily_rate, True).select(
        f.col("cuenta_corporativa"),
        f.col("_observ_end_dt"),
        f.col("saldo_total_usd"),
        f.col("saldo_total_gtq"),
        (
            convert_currency_to_gtq_task("saldo_total_usd", "tasa_cambio")
            + f.col("saldo_total_gtq")
        ).alias("saldo_total_quetzalizado"),
        f.col("saldo_capital_usd"),
        f.col("saldo_capital_gtq"),
        (
            convert_currency_to_gtq_task("saldo_capital_usd", "tasa_cambio")
            + f.col("saldo_capital_gtq")
        ).alias("saldo_capital_quetzalizado"),
        f.col("saldo_intereses_usd"),
        f.col("saldo_intereses_gtq"),
        (
            convert_currency_to_gtq_task("saldo_intereses_usd", "tasa_cambio")
            + f.col("saldo_intereses_gtq")
        ).alias("saldo_intereses_quetzalizado"),
        f.col("cuota_minima_pago_gtq"),
        f.col("cuota_minima_pago_usd"),
        (
            convert_currency_to_gtq_task("cuota_minima_pago_usd", "tasa_cambio")
            + f.col("cuota_minima_pago_gtq")
        ).alias("cuota_minima_pago_quetzalizado"),
    )

    return df_balance


@task(
    name="process_credit_card_monthly_payments_task",
    tags=["data transformation", "processing"],
)
def process_credit_card_monthly_payments_task(
    p_cleaned_cc_transaction_universe: DataFrame,
    p_cleaned_daily_rate: DataFrame,
) -> DataFrame:
    """
    Transforms and processes credit cards monthly payments data.

    Args:
        p_cleaned_cc_transaction_universe (DataFrame): Cleaned credit card payments.
        p_cleaned_daily_rate (DataFrame): Cleaned daily exchange data.

    Returns:
        DataFrame: Transformed credit card payments DataFrame.
    """
    cc_transaction_universe = (
        add_exchange_rate_task(p_cleaned_cc_transaction_universe, p_cleaned_daily_rate)
        .filter(f.col("id_tipo_transaccion").isin(2, 7))
        .select(
            f.col("*"),
            f.when(f.col("id_tipo_transaccion") == 2, f.lit("pagos"))
            .otherwise(f.lit("otros_creditos"))
            .alias("descripcion_transaccion"),
            *create_currency_col_task("id_moneda", "monto_transaccion2", "tasa_cambio")
        )
        .groupBy(f.col("cuenta_corporativa"), f.col("_observ_end_dt"))
        .pivot("descripcion_transaccion")
        .agg(
            f.sum(f.col("monto_transaccion2_gtq")).alias("monto_pago_gtq"),
            f.sum(f.col("monto_transaccion2_usd")).alias("monto_pago_usd"),
            f.sum(f.col("monto_transaccion2_quetzalizado")).alias(
                "monto_pago_quetzalizado"
            ),
        )
    )

    return cc_transaction_universe


@task(
    name="process_credit_card_tenure_tsae83_task",
    tags=["data transformation", "processing"],
)
def process_credit_card_tenure_tsae83_task(
    p_cleaned_cc_tenure_tsae83: DataFrame,
) -> DataFrame:
    """
    Transforms and processes credit cards tenure data from tsae83.

    Args:
        p_cleaned_cc_tenure_tsae83 (DataFrame): Cleaned tenure from tsae83 data.

    Returns:
        DataFrame: Transformed credit card tsae83 tenure DataFrame.
    """
    df_tsae83 = (
        p_cleaned_cc_tenure_tsae83.filter(~f.col("id_situacion_mora").isin(5, 11))
        .groupBy(f.col("cuenta_corporativa"), f.col("_observ_end_dt"))
        .agg(
            f.max(
                f.when(
                    f.col("dias_mora") > f.col("dias_mora_intereses"),
                    f.col("dias_mora"),
                ).otherwise(f.col("dias_mora_intereses"))
            ).alias("max_dias_mora_e83"),
            f.max(
                f.when(
                    f.col("ciclo_mora") > f.col("ciclo_mora_intereses"),
                    f.col("ciclo_mora"),
                ).otherwise(f.col("ciclo_mora_intereses"))
            ).alias("max_ciclo_mora_e83"),
            f.max(f.col("dias_mora_capital")).alias("max_dias_mora_capital_e83"),
            f.max(f.col("dias_mora_intereses")).alias("max_dias_mora_intereses_e83"),
        )
    )

    return df_tsae83


@task(
    name="process_credit_card_financing_installments_task",
    tags=["data transformation", "processing"],
)
def process_credit_card_financing_installments_task(
    p_cleaned_cc_extra_financing_detail: DataFrame,
    p_cleaned_cc_extra_financing_master: DataFrame,
) -> DataFrame:
    """
    Transforms and processes credit card financing data from both the datail and
    summary data.

    Args:
        p_cleaned_cc_extra_financing_detail (DataFrame): Cleaned cc financing
        detail data.
        p_cleaned_cc_extra_financing_master (DataFrame): Cleaned cc financing
        summary data.

    Returns:
        DataFrame: Transformed credit card financing installments DataFrame.
    """

    summary_df = p_cleaned_cc_extra_financing_master.select(
        f.col("cuenta_corporativa"),
        f.col("_observ_end_dt"),
        f.col("limite_cuotas"),
        f.col("disponible_cuotas"),
        f.col("limite_extrafinanciamiento"),
        f.col("disponible_extrafinanciamiento"),
    )

    financing_detail_df = p_cleaned_cc_extra_financing_detail.filter(
        f.col("estado_extrafinanciamiento") == "activo"
    ).groupBy(f.col("cuenta_corporativa"), f.col("_observ_end_dt"))

    detail_by_group_df = financing_detail_df.pivot("agrupacion_promocion").agg(
        f.sum(f.col("cuota_mensual")).alias("cuota_mes"),
        f.count(f.col("cuenta_corporativa")).cast("int").alias("cnt"),
    )

    financing_detail_df = financing_detail_df.agg(
        f.sum(f.col("monto_total")).alias("monto_total_extrafinanciamiento"),
        f.sum(f.col("saldo_pendiente")).alias("saldo_pendiente_extrafinanciamiento"),
        (f.sum(f.col("saldo_pendiente")) / f.sum(f.col("monto_total"))).alias(
            "porcentaje_saldo_pendiente_extrafinanciamiento"
        ),
    )

    financing_detail_df = financing_detail_df.join(
        detail_by_group_df, on=["cuenta_corporativa", "_observ_end_dt"], how="inner"
    )

    installments_df = summary_df.join(
        financing_detail_df,
        on=["cuenta_corporativa", "_observ_end_dt"],
        how="fullouter",
    ).fillna(0)

    return installments_df


@arrange_columns(p_start_cols=["_observ_end_dt", "id_cliente", "cuenta_corporativa"])
@task(name="transform_credit_card_task", tags=["data transformation", "processing"])
def transform_credit_card_task(
    p_cleaned_cc_account: DataFrame,
    p_cleaned_cc_categories: DataFrame,
    p_cleaned_cc_updates: DataFrame,
    p_cleaned_cc_balance: DataFrame,
    p_cleaned_cc_tenure_tsae83: DataFrame,
    p_cleaned_cc_transaction_universe: DataFrame,
    p_cleaned_daily_rate: DataFrame,
    p_cleaned_cc_extra_financing_detail: DataFrame,
    p_cleaned_cc_extra_financing_master: DataFrame,
) -> DataFrame:
    """
    Transforms and processes credit card information to create features.

    Args:
        p_cleaned_cc_account (DataFrame): Cleaned main credit card account data.
        p_cleaned_cc_categories (DataFrame): Cleaned credit card categories data.
        p_cleaned_cc_updates (DataFrame): Cleaned credit card updates data.
        p_cleaned_cc_balance (DataFrame): Cleaned credit card balance data.
        p_cleaned_cc_tenure_tsae83 (DataFrame): Cleaned tenure from tsae83 data.
        p_cleaned_cc_transaction_universe (DataFrame): Cleaned credit card payments.
        p_cleaned_daily_rate (DataFrame): Cleaned daily exchange data.
        p_cleaned_cc_extra_financing_detail (DataFrame): Cleaned cc financing
        detail data.
        p_cleaned_cc_extra_financing_master (DataFrame): Cleaned cc financing
        summary data.

    Returns:
        DataFrame: Main credit card features DataFrame.
    """
    p_cleaned_cc_account = (
        p_cleaned_cc_account.alias("acc")
        .join(
            p_cleaned_cc_categories.alias("cat"),
            on=["id_categoria_tc"],
            how="left",
        )
        .select(
            p_cleaned_cc_account["*"],
            f.when(f.col("cat.emisor").isNotNull(), f.col("cat.emisor"))
            .otherwise(f.col("acc.emisor"))
            .alias("emisor"),
            f.col("cat.segmento"),
            f.col("cat.id_categoria"),
            f.col("cat.descripcion_categoria").alias("categoria"),
            f.col("cat.categoria_estandard"),
            f.col("cat.descripcion_producto"),
            f.col("cat.empresarial_flag"),
            f.col("cat.institucional_flag"),
            f.col("cat.bi_online_flag"),
        )
        .drop(p_cleaned_cc_account.emisor)
    )

    cc_account_df = process_credit_card_account_task(
        p_cleaned_cc_account,
        p_cleaned_cc_updates,
        p_cleaned_daily_rate,
    )

    cc_balance_df = process_credit_card_balance_task(
        p_cleaned_cc_balance, p_cleaned_daily_rate
    )

    cc_tenure_tsae83_df = process_credit_card_tenure_tsae83_task(
        p_cleaned_cc_tenure_tsae83
    )

    cc_account_df = cc_account_df.join(
        cc_tenure_tsae83_df, on=["_observ_end_dt", "cuenta_corporativa"], how="left"
    ).select(
        cc_account_df["*"],
        f.col("max_dias_mora_e83"),
        f.col("max_ciclo_mora_e83"),
        f.when(f.col("max_ciclo_mora") == 0, 0)
        .otherwise(f.col("max_dias_mora_capital_e83"))
        .alias("max_dias_mora_capital_e83"),
        f.when(f.col("max_ciclo_mora") == 0, 0)
        .otherwise(f.col("max_dias_mora_intereses_e83"))
        .alias("max_dias_mora_intereses_e83"),
    )

    cc_monthly_payments = process_credit_card_monthly_payments_task(
        p_cleaned_cc_transaction_universe, p_cleaned_daily_rate
    )

    cc_installments_df = process_credit_card_financing_installments_task(
        p_cleaned_cc_extra_financing_detail, p_cleaned_cc_extra_financing_master
    )

    df_cc_features = join_dataframes_task(
        ["cuenta_corporativa", "_observ_end_dt"],
        [
            cc_account_df,
            cc_balance_df,
            cc_monthly_payments,
            cc_installments_df,
        ],
    )

    return df_cc_features


@flow(name="credit_card_flow")
def credit_card_flow():
    """
    Loads, transforms and saves main credit card features in the data lake.

    The flow performs the following operations:
    1. Loads cleaned credit card data using the specified date range.
    2. Transforms and processes the credit card features data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
        credit card features data.
    """
    raw_data = load_raw_data_flow()

    df_credit_card_features = transform_credit_card_task(
        raw_data["cleaned_cc_account"],
        raw_data["cleaned_cc_categories"],
        raw_data["cleaned_cc_updates"],
        raw_data["cleaned_cc_balance"],
        raw_data["cleaned_cc_tenure_tsae83"],
        raw_data["cleaned_cc_transaction_universe"],
        raw_data["cleaned_exchange_rate"],
        raw_data["cleaned_cc_extra_financing_detail"],
        raw_data["cleaned_cc_extra_financing_master"],
    )

    save_data_flow(df_credit_card_features)
