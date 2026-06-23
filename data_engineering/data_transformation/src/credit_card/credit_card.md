---
description: |
    API documentation for modules: data_engineering.data_transformation.src.credit_card, data_engineering.data_transformation.src.credit_card.additional_credit_card_flow, data_engineering.data_transformation.src.credit_card.credit_card_flow.

lang: en

classoption: oneside
geometry: margin=1in
papersize: a4

linkcolor: blue
links-as-notes: true
...



# Module `data_engineering.data_transformation.src.credit_card` {#data_engineering.data_transformation.src.credit_card}





## Sub-modules

* [data_engineering.data_transformation.src.credit_card.additional_credit_card_flow](#data_engineering.data_transformation.src.credit_card.additional_credit_card_flow)
* [data_engineering.data_transformation.src.credit_card.credit_card_flow](#data_engineering.data_transformation.src.credit_card.credit_card_flow)







# Module `data_engineering.data_transformation.src.credit_card.additional_credit_card_flow` {#data_engineering.data_transformation.src.credit_card.additional_credit_card_flow}







## Functions



### Function `additional_credit_card_flow` {#data_engineering.data_transformation.src.credit_card.additional_credit_card_flow.additional_credit_card_flow}




>     def additional_credit_card_flow()


Loads, transforms and saves additional credit card features in the data lake.

The flow performs the following operations:
1. Loads cleaned additional credit card data using the specified date range.
2. Transforms and processes the additional credit card features data.
3. Saves the processed data to the appropriate environment using the
   specified overwrite strategy.


Returns
-----=
<code>None</code>
:   This flow does not return a value, but saves the processed


additional credit card features data.





# Module `data_engineering.data_transformation.src.credit_card.credit_card_flow` {#data_engineering.data_transformation.src.credit_card.credit_card_flow}







## Functions



### Function `credit_card_flow` {#data_engineering.data_transformation.src.credit_card.credit_card_flow.credit_card_flow}




>     def credit_card_flow()


Loads, transforms and saves main credit card features in the data lake.

The flow performs the following operations:
1. Loads cleaned credit card data using the specified date range.
2. Transforms and processes the credit card features data.
3. Saves the processed data to the appropriate environment using the
   specified overwrite strategy.


Returns
-----=
<code>None</code>
:   This flow does not return a value, but saves the processed


credit card features data.


### Function `process_credit_card_account_task` {#data_engineering.data_transformation.src.credit_card.credit_card_flow.process_credit_card_account_task}




>     def process_credit_card_account_task(
>         p_cleaned_cc_account: pyspark.sql.dataframe.DataFrame,
>         p_cleaned_cc_updates: pyspark.sql.dataframe.DataFrame,
>         p_cleaned_daily_rate: pyspark.sql.dataframe.DataFrame
>     ) ‑> pyspark.sql.dataframe.DataFrame


Transforms and processes credit cards account data.


Args
-----=
**```p_cleaned_cc_account```** :&ensp;<code>DataFrame</code>
:   Cleaned main credit card account data.


**```p_cleaned_cc_updates```** :&ensp;<code>DataFrame</code>
:   Cleaned credit card updates data.


**```p_cleaned_daily_rate```** :&ensp;<code>DataFrame</code>
:   Cleaned daily exchange data.



Returns
-----=
<code>DataFrame</code>
:   Transformed main credit card account DataFrame.




### Function `process_credit_card_balance_task` {#data_engineering.data_transformation.src.credit_card.credit_card_flow.process_credit_card_balance_task}




>     def process_credit_card_balance_task(
>         p_cleaned_cc_balance: pyspark.sql.dataframe.DataFrame,
>         p_cleaned_daily_rate: pyspark.sql.dataframe.DataFrame
>     ) ‑> pyspark.sql.dataframe.DataFrame


Transforms and processes credit cards balance data.


Args
-----=
**```p_cleaned_cc_balance```** :&ensp;<code>DataFrame</code>
:   Cleaned credit card balance data.


**```p_cleaned_daily_rate```** :&ensp;<code>DataFrame</code>
:   Cleaned daily exchange data.



Returns
-----=
<code>DataFrame</code>
:   Transformed credit card balance DataFrame.




### Function `process_credit_card_financing_installments_task` {#data_engineering.data_transformation.src.credit_card.credit_card_flow.process_credit_card_financing_installments_task}




>     def process_credit_card_financing_installments_task(
>         p_cleaned_cc_extra_financing_detail: pyspark.sql.dataframe.DataFrame,
>         p_cleaned_cc_extra_financing_master: pyspark.sql.dataframe.DataFrame
>     ) ‑> pyspark.sql.dataframe.DataFrame


Transforms and processes credit card financing data from both the datail and
summary data.


Args
-----=
**```p_cleaned_cc_extra_financing_detail```** :&ensp;<code>DataFrame</code>
:   Cleaned cc financing


detail data.
**```p_cleaned_cc_extra_financing_master```** :&ensp;<code>DataFrame</code>
:   Cleaned cc financing


summary data.

Returns
-----=
<code>DataFrame</code>
:   Transformed credit card financing installments DataFrame.




### Function `process_credit_card_monthly_payments_task` {#data_engineering.data_transformation.src.credit_card.credit_card_flow.process_credit_card_monthly_payments_task}




>     def process_credit_card_monthly_payments_task(
>         p_cleaned_cc_transaction_universe: pyspark.sql.dataframe.DataFrame,
>         p_cleaned_daily_rate: pyspark.sql.dataframe.DataFrame
>     ) ‑> pyspark.sql.dataframe.DataFrame


Transforms and processes credit cards monthly payments data.


Args
-----=
**```p_cleaned_cc_transaction_universe```** :&ensp;<code>DataFrame</code>
:   Cleaned credit card payments.


**```p_cleaned_daily_rate```** :&ensp;<code>DataFrame</code>
:   Cleaned daily exchange data.



Returns
-----=
<code>DataFrame</code>
:   Transformed credit card payments DataFrame.




### Function `process_credit_card_tenure_tsae83_task` {#data_engineering.data_transformation.src.credit_card.credit_card_flow.process_credit_card_tenure_tsae83_task}




>     def process_credit_card_tenure_tsae83_task(
>         p_cleaned_cc_tenure_tsae83: pyspark.sql.dataframe.DataFrame
>     ) ‑> pyspark.sql.dataframe.DataFrame


Transforms and processes credit cards tenure data from tsae83.


Args
-----=
**```p_cleaned_cc_tenure_tsae83```** :&ensp;<code>DataFrame</code>
:   Cleaned tenure from tsae83 data.



Returns
-----=
<code>DataFrame</code>
:   Transformed credit card tsae83 tenure DataFrame.





-----
Generated by *pdoc* 0.11.6 (<https://pdoc3.github.io>).
