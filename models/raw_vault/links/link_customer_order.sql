{%- set source_model = "v_stg_orders" -%}
{%- set src_pk = "ORDER_CUSTOMER_PK" -%}
{%- set src_fk = ["CUSTOMER_PK", "ORDER_PK"] -%}
{%- set src_ldts = "LOAD_DATE" -%}
{%- set src_source = "SOURCE" -%}

{{ dbtvault.link(src_pk=src_pk, src_fk=src_fk, src_ldts=src_ldts,
                 src_source=src_source, source_model=source_model) }}
