{%- set yaml_metadata -%}
source_model: 'raw_transactions'
derived_columns:
  SOURCE: '!RAW_TRANSACTIONS'
  LOAD_DATE: DATEADD(DAY, 1, TRANSACTION_DATE)
  EFFECTIVE_FROM: 'TRANSACTION_DATE'
hashed_columns:
  TRANSACTION_PK:
    - 'CUSTOMER_ID'
    - 'TRANSACTION_NUMBER'
  CUSTOMER_FK: 'CUSTOMER_ID'
  CUSTOMER_PK: 'CUSTOMER_ID'
  ORDER_FK: 'ORDER_ID'
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{% set source_model = metadata_dict['source_model'] %}

{% set derived_columns = metadata_dict['derived_columns'] %}

{% set hashed_columns = metadata_dict['hashed_columns'] %}

{{ dbtvault.stage(include_source_columns=true,
                  source_model=source_model,
                  derived_columns=derived_columns,
                  hashed_columns=hashed_columns,
                  ranked_columns=none) }}